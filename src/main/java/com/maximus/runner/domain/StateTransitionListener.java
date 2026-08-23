package com.maximus.runner.domain;

@FunctionalInterface
public interface StateTransitionListener {

    void onTransition(RunnerState from, RunnerState to, String reason);
}
