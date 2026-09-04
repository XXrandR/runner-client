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
import com.maximus.runner.infrastructure.grpc.GrpcSession;
import com.maximus.runner.infrastructure.grpc.GrpcSessionOpenException;
import com.maximus.runner.security.RunnerHmacSigner;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public final class RunnerLifecycle implements RunnerConnection.ConnectionListener, LifecycleContext {

    private static final int HANDSHAKE_NONCE_LENGTH_BYTES = 32;

    private final RunnerConfig config;
    private final RunnerStateMachine stateMachine;
    private final ReconnectPolicy reconnectPolicy;
    private final ActiveSessionHandler activeSessionHandler;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final Object lifecycleLock = new Object();

    private RunnerConnection connection;

    public RunnerLifecycle(RunnerConfig config, ActiveSessionHandler activeSessionHandler) {
        this.config = config;
        this.stateMachine = RunnerStateMachine.createWithLogging();
        this.reconnectPolicy = new ReconnectPolicy(
                config.initialReconnectDelayMs(),
                config.maxReconnectDelayMs()
        );
        this.activeSessionHandler = activeSessionHandler;
    }

    public void run() {
        stateMachine.transitionTo(RunnerState.DISCONNECTED, "initialized");

        while (!shutdown.get()) {

            if (stateMachine.getState() == RunnerState.DISCONNECTED) {

                if (!sleep(reconnectPolicy.currentDelayMs())) {
                    break;
                }

                if (!shutdown.get()) {
                    attemptConnection();
                }

            } else {

                if (!sleep(100)) {
                    break;
                }
            }
        }

        activeSessionHandler.onSessionStopped();
    }

    public void shutdown() {
        shutdown.set(true);
        disconnect("shutdown");
    }

    public RunnerState getState() {
        return stateMachine.getState();
    }

    public boolean isShutdown() {
        return shutdown.get();
    }

    public Object lifecycleLock() {
        return lifecycleLock;
    }

    private void attemptConnection() {
        synchronized (lifecycleLock) {

            if (stateMachine.getState() != RunnerState.DISCONNECTED) {
                return;
            }

            connection = new GrpcSession(config);

            try {
                connection.open(this);
            } catch (GrpcSessionOpenException exception) {

                System.out.println();
                System.out.println("[RUNNER] ✗ Failed to create Connect() stream");
                System.out.println(
                        "[RUNNER] Error: "
                                + (exception.getCause() != null
                                ? exception.getCause().getMessage()
                                : exception.getMessage())
                );

                connection = null;
                reconnectPolicy.increase();

                return;
            }

            stateMachine.transitionTo(RunnerState.AUTHENTICATING, "connect");
            sendAuthentication();
            reconnectPolicy.reset();
        }
    }

    private void sendAuthentication() {
        long timestampEpochMs = System.currentTimeMillis();
        String hmac = RunnerHmacSigner.sign(
                config.key(),
                config.credential(),
                timestampEpochMs
        );

        AuthenticateRequest authenticateRequest = AuthenticateRequest.newBuilder()
                .setCredential(config.credential())
                .setHmac(hmac)
                .setTimestampEpochMs(timestampEpochMs)
                .build();

        connection.send(
                RunnerRequest.newBuilder()
                        .setAuthenticate(authenticateRequest)
                        .build()
        );

        System.out.println("[RUNNER] → AUTHENTICATE sent");
    }

    private void sendHandshake() {
        HandshakeRequest handshakeRequest = HandshakeRequest.newBuilder()
                .setRunnerId(config.runnerId())
                .setRunnerVersion(config.runnerVersion())
                .setProtocolVersion(config.protocolVersion())
                .build();
        stateMachine.transitionTo(RunnerState.HANDSHAKING, "handshake");

        connection.send(
                RunnerRequest.newBuilder()
                        .setHandshake(handshakeRequest)
                        .build()
        );
        System.out.println("[RUNNER] → HANDSHAKE sent");
    }

    @Override
    public void onResponse(ServerResponse response) {
        synchronized (lifecycleLock) {

            System.out.println("[RUNNER] ← Response received");

            RunnerState currentState = stateMachine.getState();

            switch (currentState) {
                case AUTHENTICATING -> handleAuthenticationResponse(response);
                case HANDSHAKING -> handleHandshakeResponse(response);
                case ACTIVE -> activeSessionHandler.onActiveResponse(response);
                default -> System.out.println(
                        "[RUNNER] ← Unexpected response in state "
                                + currentState
                );
            }
        }
    }

    private void handleAuthenticationResponse(ServerResponse response) {

        if (!response.hasAuthentication()) {
            System.out.println(
                    "[RUNNER] ← Unexpected response while AUTHENTICATING"
            );
            return;
        }

        AuthenticationResponse authenticationResponse =
                response.getAuthentication();

        if (authenticationResponse.getAccepted()) {

            stateMachine.transitionTo(
                    RunnerState.AUTHENTICATED,
                    "authentication accepted"
            );
            sendHandshake();

        } else {

            disconnect(
                    "authentication rejected: "
                            + authenticationResponse.getFailureReason()
            );
        }
    }

    private void handleHandshakeResponse(ServerResponse response) {

        if (response.hasHandshakeChallenge()) {
            handleHandshakeChallenge(response.getHandshakeChallenge());
            return;
        }

        if (!response.hasHandshake()) {
            System.out.println(
                    "[RUNNER] ← Unexpected response while HANDSHAKING"
            );
            return;
        }

        HandshakeResponse handshakeResponse = response.getHandshake();

        if (handshakeResponse.getAccepted()) {

            SessionContext sessionContext = new SessionContext(
                    handshakeResponse.getSessionId(),
                    handshakeResponse.getHeartbeatIntervalSeconds(),
                    handshakeResponse.getProtocolVersion()
            );

            stateMachine.transitionTo(RunnerState.ACTIVE, "handshake accepted");
            activeSessionHandler.onSessionStarted(sessionContext, connection);

        } else {

            disconnect("handshake rejected");
        }
    }

    private void handleHandshakeChallenge(HandshakeChallenge challenge) {
        byte[] nonce = challenge.getNonce().toByteArray();

        System.out.println("[RUNNER] ← HANDSHAKE_CHALLENGE received");

        if (nonce.length != HANDSHAKE_NONCE_LENGTH_BYTES) {
            disconnect("invalid handshake challenge: nonce must contain 32 bytes");
            return;
        }

        if (challenge.getExpiresAtEpochMillis() <= System.currentTimeMillis()) {
            Arrays.fill(nonce, (byte) 0);
            disconnect("handshake challenge expired");
            return;
        }

        byte[] hmac = null;

        try {
            hmac = RunnerHmacSigner.signNonce(config.key(), nonce);

            HandshakeProof proof = HandshakeProof.newBuilder()
                    .setHmac(ByteString.copyFrom(hmac))
                    .build();

            connection.send(
                    RunnerRequest.newBuilder()
                            .setHandshakeProof(proof)
                            .build()
            );

            System.out.println("[RUNNER] → HANDSHAKE_PROOF sent");
        } catch (IllegalArgumentException | IllegalStateException exception) {
            System.out.println(
                    "[RUNNER] ✗ Could not calculate handshake proof: "
                            + exception.getMessage()
            );
            disconnect("handshake proof calculation failed");
        } finally {
            Arrays.fill(nonce, (byte) 0);
            if (hmac != null) {
                Arrays.fill(hmac, (byte) 0);
            }
        }
    }


    @Override
    public void onError(Throwable throwable) {

        synchronized (lifecycleLock) {

            logStreamError(throwable);
            disconnect("connection lost");
        }
    }

    @Override
    public void onCompleted() {

        synchronized (lifecycleLock) {

            System.out.println();
            System.out.println("[RUNNER] ✗ Server closed the stream");

            disconnect("connection lost");
        }
    }

    public void disconnect(String reason) {

        RunnerState currentState = stateMachine.getState();

        activeSessionHandler.onSessionStopped();
        closeConnectionIfOpen();

        if (currentState == RunnerState.DISCONNECTED) {
            return;
        }

        stateMachine.transitionTo(RunnerState.DISCONNECTED, reason);
        reconnectPolicy.increase();
    }

    private void closeConnectionIfOpen() {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    private void logStreamError(Throwable throwable) {

        System.out.println();
        System.out.println("[RUNNER] ✗ gRPC stream ERROR");
        System.out.println(
                "[RUNNER] Error type: "
                        + throwable.getClass().getName()
        );
        System.out.println(
                "[RUNNER] Error message: "
                        + throwable.getMessage()
        );

        if (throwable instanceof StatusRuntimeException exception) {

            Status status = exception.getStatus();

            System.out.println(
                    "[RUNNER] gRPC status: "
                            + status.getCode()
            );
            System.out.println(
                    "[RUNNER] gRPC description: "
                            + status.getDescription()
            );
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
}
