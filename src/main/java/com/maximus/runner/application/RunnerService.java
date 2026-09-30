package com.maximus.runner.application;

import com.maximus.runner.application.lifecycle.RunnerLifecycle;
import com.maximus.runner.application.lifecycle.SessionManager;
import com.maximus.runner.configuration.RunnerConfig;
import com.maximus.runner.infrastructure.grpc.GrpcSession;

import java.util.Objects;

/** Builds and owns one runner lifecycle. No global mutable singleton is needed. */
public final class RunnerService {

    private final RunnerLifecycle lifecycle;

    public RunnerService(RunnerConfig config) {
        Objects.requireNonNull(config, "config");
        SessionManager sessionManager = new SessionManager(config);
        this.lifecycle = new RunnerLifecycle(config, sessionManager, () -> new GrpcSession(config));
        sessionManager.attach(lifecycle);
    }

    public void start() {
        lifecycle.run();
    }

    public void shutdown() {
        lifecycle.shutdown();
    }
}
