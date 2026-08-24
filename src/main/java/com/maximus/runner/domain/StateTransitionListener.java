package com.maximus.runner.domain;

@FunctionalInterface
public interface StateTransitionListener {

    void onTransition(StateTransition transition);
}
