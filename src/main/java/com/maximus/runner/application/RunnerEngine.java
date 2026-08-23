package com.maximus.runner.application;

import com.maximus.runner.AuthenticateRequest;
import com.maximus.runner.AuthenticationResponse;
import com.maximus.runner.Command;
import com.maximus.runner.CommandResult;
import com.maximus.runner.HandshakeRequest;
import com.maximus.runner.HandshakeResponse;
import com.maximus.runner.Heartbeat;
import com.maximus.runner.HealthUpdate;
import com.maximus.runner.RunnerRequest;
import com.maximus.runner.RunnerStatus;
import com.maximus.runner.ServerResponse;
import com.maximus.runner.StatusUpdate;
import com.maximus.runner.config.RunnerConfig;
import com.maximus.runner.domain.RunnerState;
import com.maximus.runner.domain.RunnerStateMachine;
import com.maximus.runner.domain.SessionContext;
import com.maximus.runner.infrastructure.grpc.GrpcSession;
import com.maximus.runner.infrastructure.grpc.GrpcSessionListener;
import com.maximus.runner.infrastructure.grpc.GrpcSessionOpenException;
import com.maximus.runner.infrastructure.health.SystemHealthCollector;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.atomic.AtomicBoolean;

public class RunnerEngine implements GrpcSessionListener {

    private final RunnerConfig config;
    private final RunnerStateMachine stateMachine;
    private final SystemHealthCollector healthCollector;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final AtomicBoolean activeSessionRunning = new AtomicBoolean(false);
    private final Object lifecycleLock = new Object();

    private GrpcSession grpcSession;
    private SessionContext sessionContext;
    private Thread activeSessionThread;
    private long reconnectBackoffMs;

    public RunnerEngine(RunnerConfig config) {
        this.config = config;
        this.stateMachine = RunnerStateMachine.createWithLogging();
        this.healthCollector = new SystemHealthCollector();
        this.reconnectBackoffMs = config.initialReconnectDelayMs();
    }

    public void run() {
        stateMachine.transitionTo(RunnerState.DISCONNECTED, "initialized");

        while (!shutdown.get()) {

            if (stateMachine.getState() == RunnerState.DISCONNECTED) {

                if (!sleep(reconnectBackoffMs)) {
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

        stopActiveSession();
    }

    public void shutdown() {
        shutdown.set(true);
        disconnect("shutdown");
    }

    private void attemptConnection() {
        synchronized (lifecycleLock) {

            if (stateMachine.getState() != RunnerState.DISCONNECTED) {
                return;
            }

            grpcSession = new GrpcSession(config);

            try {
                grpcSession.open(this);
            } catch (GrpcSessionOpenException exception) {

                System.out.println();
                System.out.println("[RUNNER] ✗ Failed to create Connect() stream");
                System.out.println(
                        "[RUNNER] Error: "
                                + (exception.getCause() != null
                                ? exception.getCause().getMessage()
                                : exception.getMessage())
                );

                grpcSession = null;
                increaseReconnectBackoff();

                return;
            }

            stateMachine.transitionTo(RunnerState.AUTHENTICATING, "connect");
            sendAuthentication();
            reconnectBackoffMs = config.initialReconnectDelayMs();
        }
    }

    private void sendAuthentication() {
        AuthenticateRequest authenticateRequest = AuthenticateRequest.newBuilder()
                .setCredential(config.credential())
                .build();

        grpcSession.send(
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

        grpcSession.send(
                RunnerRequest.newBuilder()
                        .setHandshake(handshakeRequest)
                        .build()
        );

        stateMachine.transitionTo(RunnerState.HANDSHAKING, "handshake");

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
                case ACTIVE -> handleActiveResponse(response);
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

        if (!response.hasHandshake()) {
            System.out.println(
                    "[RUNNER] ← Unexpected response while HANDSHAKING"
            );
            return;
        }

        HandshakeResponse handshakeResponse = response.getHandshake();

        if (handshakeResponse.getAccepted()) {

            sessionContext = new SessionContext(
                    handshakeResponse.getSessionId(),
                    handshakeResponse.getHeartbeatIntervalSeconds(),
                    handshakeResponse.getProtocolVersion()
            );

            stateMachine.transitionTo(RunnerState.ACTIVE, "handshake accepted");
            startActiveSession();

        } else {

            disconnect("handshake rejected");
        }
    }

    private void handleActiveResponse(ServerResponse response) {

        if (response.hasHeartbeat()) {

            System.out.println(
                    "[RUNNER] ← HEARTBEAT"
                            + " | timestamp="
                            + response.getHeartbeat().getTimestamp()
            );

        } else if (response.hasCommand()) {

            handleCommand(response.getCommand());

        } else {

            System.out.println("[RUNNER] ← Unknown response payload");
        }
    }

    private void handleCommand(Command command) {

        System.out.println(
                "[RUNNER] ← COMMAND"
                        + " | id="
                        + command.getCommandId()
                        + " | type="
                        + command.getType()
        );

        sendCommandResult(
                command.getCommandId(),
                false,
                "",
                "not implemented"
        );
    }

    private void startActiveSession() {
        stopActiveSession();

        sendStatusUpdate(RunnerStatus.READY);

        activeSessionRunning.set(true);
        activeSessionThread = new Thread(
                this::runActiveSessionLoop,
                "runner-active-session"
        );
        activeSessionThread.start();

        System.out.println("[RUNNER] Active session started");
    }

    private void runActiveSessionLoop() {
        long intervalMs = resolveHeartbeatIntervalMs();

        while (
                activeSessionRunning.get()
                        && !shutdown.get()
                        && stateMachine.getState() == RunnerState.ACTIVE
        ) {
            try {
                synchronized (lifecycleLock) {

                    if (
                            !activeSessionRunning.get()
                                    || shutdown.get()
                                    || stateMachine.getState() != RunnerState.ACTIVE
                                    || grpcSession == null
                    ) {
                        break;
                    }

                    sendHeartbeat();
                    sendStatusUpdate(RunnerStatus.READY);
                    sendHealthUpdate();
                }

                if (!sleep(intervalMs)) {
                    break;
                }

            } catch (Exception exception) {

                System.out.println(
                        "[RUNNER] ✗ Active session error: "
                                + exception.getMessage()
                );

                synchronized (lifecycleLock) {
                    disconnect("active session error");
                }

                break;
            }
        }
    }

    private void stopActiveSession() {
        activeSessionRunning.set(false);

        if (activeSessionThread != null) {
            activeSessionThread.interrupt();

            try {
                activeSessionThread.join(5_000);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }

            activeSessionThread = null;
        }
    }

    private long resolveHeartbeatIntervalMs() {
        if (sessionContext != null && sessionContext.heartbeatIntervalSeconds() > 0) {
            return sessionContext.heartbeatIntervalSeconds() * 1_000L;
        }

        return config.fallbackHeartbeatIntervalMs();
    }

    private void sendHeartbeat() {
        long timestamp = System.currentTimeMillis();

        Heartbeat heartbeat = Heartbeat.newBuilder()
                .setTimestamp(timestamp)
                .build();

        grpcSession.send(
                RunnerRequest.newBuilder()
                        .setHeartbeat(heartbeat)
                        .build()
        );

        System.out.println(
                "[RUNNER] → HEARTBEAT sent | timestamp=" + timestamp
        );
    }

    private void sendStatusUpdate(RunnerStatus runnerStatus) {
        StatusUpdate statusUpdate = StatusUpdate.newBuilder()
                .setStatus(runnerStatus)
                .build();

        grpcSession.send(
                RunnerRequest.newBuilder()
                        .setStatus(statusUpdate)
                        .build()
        );

        System.out.println("[RUNNER] → STATUS sent | status=" + runnerStatus);
    }

    private void sendHealthUpdate() {
        HealthUpdate healthUpdate = HealthUpdate.newBuilder()
                .setRunnerId(config.runnerId())
                .setStatus(healthCollector.collect())
                .build();

        grpcSession.send(
                RunnerRequest.newBuilder()
                        .setHealth(healthUpdate)
                        .build()
        );

        System.out.println("[RUNNER] → HEALTH sent");
    }

    private void sendCommandResult(
            String commandId,
            boolean success,
            String payload,
            String error
    ) {
        CommandResult.Builder commandResult = CommandResult.newBuilder()
                .setCommandId(commandId)
                .setSuccess(success);

        if (!payload.isEmpty()) {
            commandResult.setPayload(payload);
        }

        if (!error.isEmpty()) {
            commandResult.setError(error);
        }

        grpcSession.send(
                RunnerRequest.newBuilder()
                        .setCommandResult(commandResult.build())
                        .build()
        );

        System.out.println(
                "[RUNNER] → COMMAND_RESULT sent | id=" + commandId
        );
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

    private void disconnect(String reason) {

        RunnerState currentState = stateMachine.getState();

        stopActiveSession();
        sessionContext = null;
        closeSessionIfOpen();

        if (currentState == RunnerState.DISCONNECTED) {
            return;
        }

        stateMachine.transitionTo(RunnerState.DISCONNECTED, reason);
        increaseReconnectBackoff();
    }

    private void closeSessionIfOpen() {
        if (grpcSession != null) {
            grpcSession.close();
            grpcSession = null;
        }
    }

    private void increaseReconnectBackoff() {
        reconnectBackoffMs = Math.min(
                reconnectBackoffMs * 2,
                config.maxReconnectDelayMs()
        );
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
