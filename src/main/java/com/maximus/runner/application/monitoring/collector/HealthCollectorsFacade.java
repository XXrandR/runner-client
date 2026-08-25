package com.maximus.runner.application.monitoring.collector;

import com.maximus.runner.application.monitoring.ServerHealth;
import com.maximus.runner.configuration.RunnerConfig;

/**
 * Facade for the health metrics collector subsystem.
 * Orchestrates all collectors and returns an aggregated {@link ServerHealth}.
 */
public final class HealthCollectorsFacade {

    private final SystemHealthCollector systemHealthCollector;
    private final DatabaseHealthCollector databaseHealthCollector;
    private final NetworkLatencyCollector networkLatencyCollector;

    public HealthCollectorsFacade(RunnerConfig config) {
        this.systemHealthCollector = new SystemHealthCollector();
        this.databaseHealthCollector = new DatabaseHealthCollector();
        this.networkLatencyCollector = new NetworkLatencyCollector(config);
    }

    public ServerHealth collect() {
        databaseHealthCollector.check();

        return new ServerHealth(
                systemHealthCollector.collect(),
                databaseHealthCollector.isAvailable(),
                networkLatencyCollector.measureLatencyMs()
        );
    }
}
