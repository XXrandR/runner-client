package com.maximus.runner.application.lifecycle;

import com.google.protobuf.ByteString;
import com.maximus.runner.AuthenticateRequest;
import com.maximus.runner.AuthenticationResponse;
import com.maximus.runner.HandshakeChallenge;
import com.maximus.runner.HandshakeProof;
import com.maximus.runner.HandshakeRequest;
import com.maximus.runner.HandshakeResponse;
import com.maximus.runner.RunnerRequest;
import com.maximus.runner.ServerResponse;
import com.maximus.runner.application.port.RunnerConnection;
import com.maximus.runner.configuration.RunnerConfig;
import com.maximus.runner.domain.RunnerState;
import com.maximus.runner.domain.RunnerStateMachine;
import com.maximus.runner.domain.SessionContext;
import com.maximus.runner.infrastructure.logging.RunnerLog;
import com.maximus.runner.security.RunnerHmacSigner;
import io.grpc.StatusRuntimeException;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** Drives connect, authentication, challenge/response and reconnection. */
public final class RunnerLifecycle implements LifecycleContext {

    private static final int HANDSHAKE_NONCE_LENGTH_BYTES = 32;
    private static final long AUTHENTICATION_TIMEOUT_MS = 15_000;
    private static final long HANDSHAKE_TIMEOUT_MS = 35_000;

    private final RunnerConfig config;
    private final RunnerStateMachine stateMachine = RunnerStateMachine.createWithLogging();
    private final ReconnectPolicy reconnectPolicy;
    private final ActiveSessionHandler activeSessionHandler;
    private final Supplier<? extends RunnerConnection> connectionFactory;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final Object lifecycleLock = new Object();

    private RunnerConnection connection;
    private long phaseStartedAtNanos;

    public RunnerLifecycle(RunnerConfig config, ActiveSessionHandler activeSessionHandler,
                           Supplier<? extends RunnerConnection> connectionFactory) {
        this.config = Objects.requireNonNull(config, "config");
        this.activeSessionHandler = Objects.requireNonNull(activeSessionHandler, "activeSessionHandler");
        this.connectionFactory = Objects.requireNonNull(connectionFactory, "connectionFactory");
        this.reconnectPolicy = new ReconnectPolicy(
                config.initialReconnectDelayMs(), config.maxReconnectDelayMs());
    }

    public void run() {
        synchronized (lifecycleLock) {
            if (stateMachine.getState() == RunnerState.PROVISIONED) {
                stateMachine.transitionTo(RunnerState.DISCONNECTED, "initialized");
            }
        }
        try {
            while (!shutdown.get()) {
                if (stateMachine.getState() == RunnerState.DISCONNECTED) {
                    if (!sleep(reconnectPolicy.currentDelayMs())) {
                        break;
                    }
                    if (!shutdown.get()) {
                        attemptConnection();
                    }
                } else {
                    checkPhaseTimeout();
                    if (!sleep(100)) {
                        break;
                    }
                }
            }
        } finally {
            shutdown();
        }
    }

    public void shutdown() {
        shutdown.set(true);
        synchronized (lifecycleLock) {
            disconnectInternal("shutdown");
        }
    }

    @Override
    public RunnerState getState() {
        return stateMachine.getState();
    }

    @Override
    public boolean isShutdown() {
        return shutdown.get();
    }

    @Override
    public void disconnect(RunnerConnection expectedConnection, String reason) {
        synchronized (lifecycleLock) {
            if (expectedConnection == connection && expectedConnection != null) {
                disconnectInternal(reason);
            }
        }
    }

    private void attemptConnection() {
        RunnerConnection candidate;
        synchronized (lifecycleLock) {
            if (shutdown.get() || stateMachine.getState() != RunnerState.DISCONNECTED) {
                return;
            }
            try {
                candidate = Objects.requireNonNull(connectionFactory.get(), "connectionFactory result");
            } catch (RuntimeException exception) {
                RunnerLog.error("Could not create connection", exception);
                reconnectPolicy.increase();
                return;
            }
            connection = candidate;
        }

        // Connecting can take up to ten seconds. Shutdown and callbacks must not wait on that IO.
        try {
            candidate.open(new AttemptListener(candidate));
        } catch (RuntimeException exception) {
            synchronized (lifecycleLock) {
                if (connection == candidate) {
                    RunnerLog.error("Connection attempt failed", exception);
                    disconnectInternal("connection attempt failed");
                }
            }
            return;
        }

        synchronized (lifecycleLock) {
            if (shutdown.get() || connection != candidate) {
                // A callback or shutdown already closed this attempt during open().
                return;
            }
            try {
                stateMachine.transitionTo(RunnerState.AUTHENTICATING, "connect");
                phaseStartedAtNanos = System.nanoTime();
                sendAuthentication(candidate);
            } catch (RuntimeException exception) {
                if (connection == candidate) {
                    RunnerLog.error("Connection attempt failed", exception);
                    disconnectInternal("connection attempt failed");
                }
            }
        }
    }

    private void sendAuthentication(RunnerConnection candidate) {
        AuthenticateRequest request = AuthenticateRequest.newBuilder()
                .setCredential(config.credential()).build();
        candidate.send(RunnerRequest.newBuilder().setAuthenticate(request).build());
        RunnerLog.info("AUTHENTICATE sent");
    }

    private void sendHandshake(RunnerConnection candidate) {
        HandshakeRequest request = HandshakeRequest.newBuilder()
                .setRunnerId(config.runnerId())
                .setRunnerVersion(config.runnerVersion())
                .setProtocolVersion(config.protocolVersion())
                .build();
        stateMachine.transitionTo(RunnerState.HANDSHAKING, "handshake");
        phaseStartedAtNanos = System.nanoTime();
        candidate.send(RunnerRequest.newBuilder().setHandshake(request).build());
        RunnerLog.info("HANDSHAKE sent");
    }

    private void checkPhaseTimeout() {
        synchronized (lifecycleLock) {
            RunnerState state = stateMachine.getState();
            long limitMs = switch (state) {
                case AUTHENTICATING -> AUTHENTICATION_TIMEOUT_MS;
                case HANDSHAKING -> HANDSHAKE_TIMEOUT_MS;
                default -> 0;
            };
            if (limitMs > 0 && System.nanoTime() - phaseStartedAtNanos
                    >= java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(limitMs)) {
                RunnerLog.warning(state + " timed out");
                disconnectInternal(state + " timed out");
            }
        }
    }

    private void handleResponse(RunnerConnection candidate, ServerResponse response) {
        synchronized (lifecycleLock) {
            if (candidate != connection || shutdown.get()) {
                return; // Response from a previous, already closed stream.
            }
            try {
                switch (stateMachine.getState()) {
                    case AUTHENTICATING -> handleAuthenticationResponse(candidate, response);
                    case HANDSHAKING -> handleHandshakeResponse(candidate, response);
                    case ACTIVE -> activeSessionHandler.onActiveResponse(response);
                    default -> RunnerLog.warning("Unexpected response in state " + stateMachine.getState());
                }
            } catch (RuntimeException exception) {
                RunnerLog.error("Could not process server response", exception);
                disconnectInternal("response processing failed");
            }
        }
    }

    private void handleAuthenticationResponse(RunnerConnection candidate, ServerResponse response) {
        if (!response.hasAuthentication()) {
            RunnerLog.warning("Unexpected response while AUTHENTICATING");
            return;
        }
        AuthenticationResponse authentication = response.getAuthentication();
        if (authentication.getAccepted()) {
            stateMachine.transitionTo(RunnerState.AUTHENTICATED, "authentication accepted");
            sendHandshake(candidate);
        } else {
            disconnectInternal("authentication rejected: " + authentication.getFailureReason());
        }
    }

    private void handleHandshakeResponse(RunnerConnection candidate, ServerResponse response) {
        if (response.hasHandshakeChallenge()) {
            handleHandshakeChallenge(candidate, response.getHandshakeChallenge());
            return;
        }
        if (!response.hasHandshake()) {
            RunnerLog.warning("Unexpected response while HANDSHAKING");
            return;
        }
        HandshakeResponse handshake = response.getHandshake();
        if (!handshake.getAccepted()) {
            disconnectInternal("handshake rejected");
            return;
        }
        SessionContext context = new SessionContext(
                handshake.getSessionId(),
                handshake.getHeartbeatIntervalSeconds(),
                handshake.getProtocolVersion());
        reconnectPolicy.reset();
        stateMachine.transitionTo(RunnerState.ACTIVE, "handshake accepted");
        activeSessionHandler.onSessionStarted(context, candidate);
    }

    private void handleHandshakeChallenge(RunnerConnection candidate, HandshakeChallenge challenge) {
        byte[] nonce = challenge.getNonce().toByteArray();
        byte[] hmac = null;
        RunnerLog.info("HANDSHAKE_CHALLENGE received");
        try {
            if (nonce.length != HANDSHAKE_NONCE_LENGTH_BYTES) {
                disconnectInternal("invalid handshake challenge: expected 32-byte nonce");
                return;
            }
            if (challenge.getExpiresAtEpochMillis() <= System.currentTimeMillis()) {
                disconnectInternal("handshake challenge expired");
                return;
            }
            hmac = RunnerHmacSigner.signNonce(config.key(), nonce);
            HandshakeProof proof = HandshakeProof.newBuilder()
                    .setHmac(ByteString.copyFrom(hmac))
                    .build();
            candidate.send(RunnerRequest.newBuilder().setHandshakeProof(proof).build());
            RunnerLog.info("HANDSHAKE_PROOF sent");
        } catch (IllegalArgumentException | IllegalStateException exception) {
            RunnerLog.error("Could not calculate handshake proof", exception);
            disconnectInternal("handshake proof calculation failed");
        } finally {
            Arrays.fill(nonce, (byte) 0);
            if (hmac != null) {
                Arrays.fill(hmac, (byte) 0);
            }
        }
    }

    /** Called with lifecycleLock held; closing a stream cannot stop a newer attempt. */
    private void disconnectInternal(String reason) {
        RunnerConnection old = connection;
        connection = null;
        activeSessionHandler.onSessionStopped();
        if (old != null) {
            try {
                old.close();
            } catch (RuntimeException exception) {
                RunnerLog.error("Could not close connection", exception);
            }
        }
        if (stateMachine.getState() != RunnerState.DISCONNECTED) {
            stateMachine.transitionTo(RunnerState.DISCONNECTED, reason);
            if (!shutdown.get()) {
                reconnectPolicy.increase();
            }
        } else if (old != null && !shutdown.get()) {
            reconnectPolicy.increase();
        }
    }

    private boolean sleep(long millis) {
        long remaining = millis;
        while (remaining > 0 && !shutdown.get()) {
            long chunk = Math.min(remaining, 100);
            try {
                Thread.sleep(chunk);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                return false;
            }
            remaining -= chunk;
        }
        return !shutdown.get();
    }

    private final class AttemptListener implements RunnerConnection.ConnectionListener {
        private final RunnerConnection source;

        private AttemptListener(RunnerConnection source) {
            this.source = source;
        }

        @Override
        public void onResponse(ServerResponse response) {
            handleResponse(source, response);
        }

        @Override
        public void onError(Throwable error) {
            synchronized (lifecycleLock) {
                if (source == connection) {
                    String status = error instanceof StatusRuntimeException grpcError
                            ? " | gRPC status=" + grpcError.getStatus().getCode() : "";
                    RunnerLog.error("gRPC stream error" + status, error);
                    disconnectInternal("connection lost");
                }
            }
        }

        @Override
        public void onCompleted() {
            synchronized (lifecycleLock) {
                if (source == connection) {
                    RunnerLog.warning("Server closed the stream");
                    disconnectInternal("connection lost");
                }
            }
        }
    }
}
