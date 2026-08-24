package com.maximus.runner.application.monitoring.collector;

import com.maximus.runner.configuration.RunnerConfig;

import java.net.InetSocketAddress;
import java.net.Socket;

public final class NetworkLatencyCollector {

    private static final int CONNECT_TIMEOUT_MS = 3_000;

    private final RunnerConfig config;

    public NetworkLatencyCollector(RunnerConfig config) {
        this.config = config;
    }

    public long measureLatencyMs() {
        long start = System.currentTimeMillis();

        try (Socket socket = new Socket()) {
            socket.connect(
                    new InetSocketAddress(config.serverHost(), config.serverPort()),
                    CONNECT_TIMEOUT_MS
            );
            return System.currentTimeMillis() - start;
        } catch (Exception exception) {
            return -1;
        }
    }
}
