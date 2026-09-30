package com.maximus.runner.application.lifecycle;

import com.maximus.runner.domain.RunnerState;
import com.maximus.runner.application.port.RunnerConnection;

public interface LifecycleContext {

    RunnerState getState();

    boolean isShutdown();

    void disconnect(RunnerConnection expectedConnection, String reason);
}
