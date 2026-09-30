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

    public RunnerConfig {
        if (serverHost == null || serverHost.isBlank() || serverPort < 1 || serverPort > 65_535) {
            throw new IllegalArgumentException("A valid server host and port are required");
        }
        if (credential == null || credential.isBlank() || runnerId == null || runnerId.isBlank()) {
            throw new IllegalArgumentException("Credential and runner ID are required");
        }
        if (protocolVersion <= 0 || fallbackHeartbeatIntervalMs <= 0) {
            throw new IllegalArgumentException("Protocol version and heartbeat interval must be positive");
        }
        if (initialReconnectDelayMs <= 0 || maxReconnectDelayMs < initialReconnectDelayMs) {
            throw new IllegalArgumentException("Reconnect delays must be positive and max >= initial");
        }
    }

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
