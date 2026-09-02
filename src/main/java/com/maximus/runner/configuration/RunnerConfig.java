package com.maximus.runner.configuration;

/**
 * Immutable runner configuration.
 * Values are constants for now; migrated from {@code RunnerApplication} where applicable.
 */
public record RunnerConfig(
        String serverHost,
        int serverPort,
        String credential,
        String key,
        String runnerId,
        String runnerVersion,
        int protocolVersion,
        long initialReconnectDelayMs,
        long maxReconnectDelayMs,
        long fallbackHeartbeatIntervalMs
) {

    public static RunnerConfig defaults() {
        return new RunnerConfig(
                "45.55.104.90",
                9090,
                "runner-credential",
                "",
                "runner-1",
                "1.0.0",
                1,
                1_000,
                30_000,
                5_000
        );
    }
}
