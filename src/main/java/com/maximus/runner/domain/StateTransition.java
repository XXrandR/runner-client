package com.maximus.runner.domain;

public record StateTransition(
        RunnerState from,
        RunnerState to,
        String reason
) {
}
