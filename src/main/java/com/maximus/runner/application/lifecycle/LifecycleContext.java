package com.maximus.runner.application.lifecycle;

import com.maximus.runner.domain.RunnerState;

public interface LifecycleContext {

    RunnerState getState();

    boolean isShutdown();

    Object lifecycleLock();

    void disconnect(String reason);
}
