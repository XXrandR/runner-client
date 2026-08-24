package com.maximus.runner.domain;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RunnerStateMachine {

    private static final Map<RunnerState, Set<RunnerState>> ALLOWED_TRANSITIONS = Map.of(
            RunnerState.PROVISIONED, EnumSet.of(RunnerState.DISCONNECTED),
            RunnerState.DISCONNECTED, EnumSet.of(RunnerState.AUTHENTICATING),
            RunnerState.AUTHENTICATING, EnumSet.of(
                    RunnerState.AUTHENTICATED,
                    RunnerState.DISCONNECTED
            ),
            RunnerState.AUTHENTICATED, EnumSet.of(
                    RunnerState.HANDSHAKING,
                    RunnerState.DISCONNECTED
            ),
            RunnerState.HANDSHAKING, EnumSet.of(
                    RunnerState.ACTIVE,
                    RunnerState.DISCONNECTED
            ),
            RunnerState.ACTIVE, EnumSet.of(RunnerState.DISCONNECTED)
    );

    private final List<StateTransitionListener> listeners = new ArrayList<>();

    private RunnerState currentState = RunnerState.PROVISIONED;

    public RunnerState getState() {
        return currentState;
    }

    public void addListener(StateTransitionListener listener) {
        listeners.add(listener);
    }

    public void transitionTo(RunnerState nextState, String reason) {
        if (currentState == nextState) {
            return;
        }

        Set<RunnerState> allowed = ALLOWED_TRANSITIONS.get(currentState);
        if (allowed == null || !allowed.contains(nextState)) {
            throw new IllegalStateException(
                    "Invalid transition: " + currentState + " → " + nextState
                            + " (" + reason + ")"
            );
        }

        RunnerState previousState = currentState;
        currentState = nextState;

        StateTransition transition = new StateTransition(
                previousState,
                nextState,
                reason
        );

        for (StateTransitionListener listener : listeners) {
            listener.onTransition(transition);
        }
    }

    public static RunnerStateMachine createWithLogging() {
        RunnerStateMachine stateMachine = new RunnerStateMachine();
        stateMachine.addListener(LoggingStateTransitionListener.INSTANCE);
        return stateMachine;
    }

    private static final class LoggingStateTransitionListener
            implements StateTransitionListener {

        private static final LoggingStateTransitionListener INSTANCE =
                new LoggingStateTransitionListener();

        @Override
        public void onTransition(StateTransition transition) {
            System.out.println(
                    "[RUNNER][STATE] "
                            + transition.from()
                            + " → "
                            + transition.to()
                            + " ("
                            + transition.reason()
                            + ")"
            );
        }
    }
}
