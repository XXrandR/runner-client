package com.maximus.runner.application.lifecycle;

import com.maximus.runner.Command;
import com.maximus.runner.CommandResult;
import com.maximus.runner.CommandResultStatus;
import com.maximus.runner.Heartbeat;
import com.maximus.runner.HealthUpdate;
import com.maximus.runner.RunnerRequest;
import com.maximus.runner.RunnerStatus;
import com.maximus.runner.ServerResponse;
import com.maximus.runner.StatusUpdate;
import com.maximus.runner.application.monitoring.ServerHealthMonitor;
import com.maximus.runner.application.port.RunnerConnection;
import com.maximus.runner.configuration.RunnerConfig;
import com.maximus.runner.domain.RunnerState;
import com.maximus.runner.domain.SessionContext;
import com.maximus.runner.infrastructure.logging.RunnerLog;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** Owns periodic messages and server commands while one authenticated session is active. */
public final class SessionManager implements ActiveSessionHandler {

    private final RunnerConfig config;
    private final ServerHealthMonitor serverHealthMonitor;
    private volatile LifecycleContext lifecycleContext;
    private volatile ActiveSession currentSession;

    public SessionManager(RunnerConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        this.serverHealthMonitor = new ServerHealthMonitor(config);
    }

    public void attach(LifecycleContext lifecycleContext) {
        this.lifecycleContext = Objects.requireNonNull(lifecycleContext, "lifecycleContext");
    }

    @Override
    public void onSessionStarted(SessionContext context, RunnerConnection connection) {
        onSessionStopped();
        ActiveSession session = new ActiveSession(context, connection);
        currentSession = session;
        try {
            sendStatusUpdate(session, RunnerStatus.READY);
            session.thread.start();
            RunnerLog.info("Active session started");
        } catch (RuntimeException failure) {
            onSessionStopped();
            throw failure;
        }
    }

    @Override
    public void onSessionStopped() {
        ActiveSession previous = currentSession;
        currentSession = null;
        if (previous != null) {
            previous.running.set(false);
            previous.thread.interrupt();
        }
        // No join here: the lifecycle may be holding its lock while stopping a session.
    }

    @Override
    public void onActiveResponse(ServerResponse response) {
        ActiveSession session = currentSession;
        if (session == null || !session.running.get()) {
            return;
        }
        if (response.hasHeartbeat()) {
            RunnerLog.info("HEARTBEAT received | timestamp=" + response.getHeartbeat().getTimestamp());
        } else if (response.hasCommand()) {
            handleCommand(session, response.getCommand());
        } else {
            RunnerLog.warning("Unknown active-session response payload");
        }
    }

    private void handleCommand(ActiveSession session, Command command) {
        RunnerLog.info("COMMAND received | id=" + command.getCommandId() + " | type=" + command.getType());
        // Execution is a separate feature. Always acknowledge with an explicit failure.
        CommandResult result = CommandResult.newBuilder()
                .setCommandId(command.getCommandId())
                .setSuccess(false)
                .setStatus(CommandResultStatus.FAILED)
                .setFinal(true)
                .setError("not implemented")
                .build();
        session.connection.send(RunnerRequest.newBuilder().setCommandResult(result).build());
        RunnerLog.info("COMMAND_RESULT sent | id=" + command.getCommandId());
    }

    private void runActiveSessionLoop(ActiveSession session) {
        long intervalMs = session.context.heartbeatIntervalSeconds() > 0
                ? session.context.heartbeatIntervalSeconds() * 1_000L
                : config.fallbackHeartbeatIntervalMs();

        while (isCurrent(session)) {
            try {
                sendHeartbeat(session);
                if (!isCurrent(session)) {
                    break;
                }
                sendStatusUpdate(session, RunnerStatus.READY);
                if (!isCurrent(session)) {
                    break;
                }
                sendHealthUpdate(session);
                if (!sleep(session, intervalMs)) {
                    break;
                }
            } catch (RuntimeException failure) {
                if (isCurrent(session)) {
                    RunnerLog.error("Active session failed", failure);
                    lifecycleContext.disconnect(session.connection, "active session error");
                }
                break;
            }
        }
    }

    private boolean isCurrent(ActiveSession session) {
        LifecycleContext lifecycle = lifecycleContext;
        return lifecycle != null
                && currentSession == session
                && session.running.get()
                && !lifecycle.isShutdown()
                && lifecycle.getState() == RunnerState.ACTIVE;
    }

    private boolean sleep(ActiveSession session, long millis) {
        long remaining = millis;
        while (remaining > 0 && isCurrent(session)) {
            long chunk = Math.min(remaining, 100);
            try {
                Thread.sleep(chunk);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                return false;
            }
            remaining -= chunk;
        }
        return isCurrent(session);
    }

    private void sendHeartbeat(ActiveSession session) {
        long timestamp = System.currentTimeMillis();
        Heartbeat heartbeat = Heartbeat.newBuilder().setTimestamp(timestamp).build();
        session.connection.send(RunnerRequest.newBuilder().setHeartbeat(heartbeat).build());
        RunnerLog.info("HEARTBEAT sent | timestamp=" + timestamp);
    }

    private void sendStatusUpdate(ActiveSession session, RunnerStatus status) {
        StatusUpdate update = StatusUpdate.newBuilder().setStatus(status).build();
        session.connection.send(RunnerRequest.newBuilder().setStatus(update).build());
        RunnerLog.info("STATUS sent | status=" + status);
    }

    private void sendHealthUpdate(ActiveSession session) {
        HealthUpdate update = serverHealthMonitor.buildHealthUpdate(config.runnerId());
        session.connection.send(RunnerRequest.newBuilder().setHealth(update).build());
        RunnerLog.info("HEALTH sent");
    }

    private final class ActiveSession {
        private final SessionContext context;
        private final RunnerConnection connection;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final Thread thread;

        private ActiveSession(SessionContext context, RunnerConnection connection) {
            this.context = Objects.requireNonNull(context, "context");
            this.connection = Objects.requireNonNull(connection, "connection");
            this.thread = new Thread(() -> runActiveSessionLoop(this), "runner-active-session");
        }
    }
}
