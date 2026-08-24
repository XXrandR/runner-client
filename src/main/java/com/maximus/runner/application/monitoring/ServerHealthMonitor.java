package com.maximus.runner.application.monitoring;

import com.maximus.runner.HealthUpdate;
import com.maximus.runner.application.monitoring.collector.DatabaseHealthCollector;
import com.maximus.runner.application.monitoring.collector.NetworkLatencyCollector;
import com.maximus.runner.application.monitoring.collector.SystemHealthCollector;
import com.maximus.runner.configuration.RunnerConfig;

public final class ServerHealthMonitor {

    private final SystemHealthCollector systemHealthCollector;
    private final DatabaseHealthCollector databaseHealthCollector;
    private final NetworkLatencyCollector networkLatencyCollector;

    public ServerHealthMonitor(RunnerConfig config) {
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

    public HealthUpdate buildHealthUpdate(String runnerId) {
        ServerHealth serverHealth = collect();

        return HealthUpdate.newBuilder()
                .setRunnerId(runnerId)
                .setStatus(serverHealth.healthStatus())
                .build();
    }
}
