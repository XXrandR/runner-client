package com.maximus.runner.application.monitoring.collector;

/**
 * Database health check. Stub until JDBC configuration is added to {@code RunnerConfig}.
 */
public final class DatabaseHealthCollector {

    public boolean isAvailable() {
        return false;
    }

    public void check() {
        // No database configured yet.
    }
}
