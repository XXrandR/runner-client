package com.maximus.runner.application.monitoring;

import com.maximus.runner.HealthUpdate;
import com.maximus.runner.application.monitoring.collector.HealthCollectorsFacade;
import com.maximus.runner.configuration.RunnerConfig;

/**
 * Adapter that maps aggregated health metrics to gRPC {@link HealthUpdate} payloads.
 */
public final class ServerHealthMonitor {

    private final HealthCollectorsFacade collectorsFacade;

    public ServerHealthMonitor(RunnerConfig config) {
        this.collectorsFacade = new HealthCollectorsFacade(config);
    }

    public ServerHealth collect() {
        return collectorsFacade.collect();
    }

    public HealthUpdate buildHealthUpdate(String runnerId) {
        ServerHealth serverHealth = collect();

        return HealthUpdate.newBuilder()
                .setRunnerId(runnerId)
                .setStatus(serverHealth.healthStatus())
                .build();
    }
}
