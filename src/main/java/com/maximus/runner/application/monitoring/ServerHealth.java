package com.maximus.runner.application.monitoring;

import com.maximus.runner.HealthStatus;

public record ServerHealth(
        HealthStatus healthStatus,
        boolean databaseAvailable,
        long networkLatencyMs
) {
}
