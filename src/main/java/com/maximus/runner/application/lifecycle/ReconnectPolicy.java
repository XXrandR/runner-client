package com.maximus.runner.application.lifecycle;

public final class ReconnectPolicy {

    private final long initialDelayMs;
    private final long maxDelayMs;
    private volatile long currentDelayMs;

    public ReconnectPolicy(long initialDelayMs, long maxDelayMs) {
        if (initialDelayMs <= 0 || maxDelayMs < initialDelayMs) {
            throw new IllegalArgumentException("Reconnect delays must be positive and max >= initial");
        }
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.currentDelayMs = initialDelayMs;
    }

    public long currentDelayMs() {
        return currentDelayMs;
    }

    public void increase() {
        // Subtraction also avoids overflow when the delay is very large.
        currentDelayMs = currentDelayMs >= maxDelayMs - currentDelayMs
                ? maxDelayMs
                : currentDelayMs * 2;
    }

    public void reset() {
        currentDelayMs = initialDelayMs;
    }
}
