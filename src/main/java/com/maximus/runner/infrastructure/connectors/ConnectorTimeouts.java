package com.maximus.runner.infrastructure.connectors;

import com.google.protobuf.Duration;
import com.maximus.runner.application.command.CommandContext;

final class ConnectorTimeouts {
    private ConnectorTimeouts() {}
    static long within(Duration value, CommandContext context) throws Exception {
        if (value.getSeconds() < 0 || value.getSeconds() > 3600 || value.getNanos() < 0 || value.getNanos() >= 1_000_000_000)
            throw new IllegalArgumentException("Invalid connector timeout");
        long millis = value.getSeconds() * 1000L + value.getNanos() / 1_000_000L;
        if (millis <= 0) throw new IllegalArgumentException("Connector timeout must be positive");
        return Math.min(millis, context.remainingMillis());
    }
}
