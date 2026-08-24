package com.maximus.runner.application.lifecycle;

import com.maximus.runner.Command;
import com.maximus.runner.CommandResult;
import com.maximus.runner.Heartbeat;
import com.maximus.runner.HealthUpdate;
import com.maximus.runner.RunnerRequest;
import com.maximus.runner.RunnerStatus;
import com.maximus.runner.ServerResponse;
import com.maximus.runner.StatusUpdate;
import com.maximus.runner.application.port.RunnerConnection;
import com.maximus.runner.configuration.RunnerConfig;
import com.maximus.runner.domain.RunnerState;
import com.maximus.runner.domain.SessionContext;
import com.maximus.runner.application.monitoring.ServerHealthMonitor;

import java.util.concurrent.atomic.AtomicBoolean;

public final class SessionManager implements ActiveSessionHandler {

    private final RunnerConfig config;
    private final ServerHealthMonitor serverHealthMonitor;
    private final AtomicBoolean activeSessionRunning = new AtomicBoolean(false);

    private LifecycleContext lifecycleContext;
    private RunnerConnection activeConnection;
    private SessionContext sessionContext;
    private Thread activeSessionThread;

    public SessionManager(RunnerConfig config) {
        this.config = config;
        this.serverHealthMonitor = new ServerHealthMonitor(config);
    }

    public void attach(LifecycleContext lifecycleContext) {
        this.lifecycleContext = lifecycleContext;
    }

    @Override
    public void onSessionStarted(SessionContext sessionContext, RunnerConnection connection) {
        this.sessionContext = sessionContext;
        this.activeConnection = connection;
        startActiveSession();
    }

    @Override
    public void onSessionStopped() {
        stopActiveSession();
        sessionContext = null;
        activeConnection = null;
    }

    @Override
    public void onActiveResponse(ServerResponse response) {
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
                        && !lifecycleContext.isShutdown()
                        && lifecycleContext.getState() == RunnerState.ACTIVE
        ) {
            try {
                synchronized (lifecycleContext.lifecycleLock()) {

                    if (
                            !activeSessionRunning.get()
                                    || lifecycleContext.isShutdown()
                                    || lifecycleContext.getState() != RunnerState.ACTIVE
                                    || activeConnection == null
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

                synchronized (lifecycleContext.lifecycleLock()) {
                    lifecycleContext.disconnect("active session error");
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

        activeConnection.send(
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

        activeConnection.send(
                RunnerRequest.newBuilder()
                        .setStatus(statusUpdate)
                        .build()
        );

        System.out.println("[RUNNER] → STATUS sent | status=" + runnerStatus);
    }

    private void sendHealthUpdate() {
        HealthUpdate healthUpdate = serverHealthMonitor.buildHealthUpdate(config.runnerId());

        activeConnection.send(
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

        activeConnection.send(
                RunnerRequest.newBuilder()
                        .setCommandResult(commandResult.build())
                        .build()
        );

        System.out.println(
                "[RUNNER] → COMMAND_RESULT sent | id=" + commandId
        );
    }

    private boolean sleep(long millis) {
        long remaining = millis;

        while (remaining > 0 && !lifecycleContext.isShutdown()) {
            long chunk = Math.min(remaining, 100);

            try {
                Thread.sleep(chunk);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                return false;
            }

            remaining -= chunk;
        }

        return !lifecycleContext.isShutdown();
    }
}
