package com.maximus.runner.application;

import com.maximus.runner.application.lifecycle.RunnerLifecycle;
import com.maximus.runner.application.lifecycle.SessionManager;
import com.maximus.runner.configuration.RunnerConfig;

public final class RunnerService {

    private static volatile RunnerService instance;
    private static final Object INSTANCE_LOCK = new Object();

    private final RunnerLifecycle lifecycle;

    private RunnerService(RunnerConfig config) {
        SessionManager sessionManager = new SessionManager(config);
        this.lifecycle = new RunnerLifecycle(config, sessionManager);
        sessionManager.attach(lifecycle);
    }

    public static void initialize(RunnerConfig config) {
        if (instance != null) {
            throw new IllegalStateException("RunnerService already initialized");
        }

        synchronized (INSTANCE_LOCK) {
            if (instance != null) {
                throw new IllegalStateException("RunnerService already initialized");
            }
            instance = new RunnerService(config);
        }
    }

    public static RunnerService getInstance() {
        RunnerService current = instance;
        if (current == null) {
            throw new IllegalStateException(
                    "RunnerService not initialized. Call initialize(config) first."
            );
        }
        return current;
    }

    public void start() {
        lifecycle.run();
    }

    public void shutdown() {
        lifecycle.shutdown();
    }
}
