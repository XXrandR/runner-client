package com.maximus.runner.application.lifecycle;

public final class ReconnectPolicy {

    private final long initialDelayMs;
    private final long maxDelayMs;
    private long currentDelayMs;

    public ReconnectPolicy(long initialDelayMs, long maxDelayMs) {
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.currentDelayMs = initialDelayMs;
    }

    public long currentDelayMs() {
        return currentDelayMs;
    }

    public void increase() {
        currentDelayMs = Math.min(currentDelayMs * 2, maxDelayMs);
    }

    public void reset() {
        currentDelayMs = initialDelayMs;
    }
}
