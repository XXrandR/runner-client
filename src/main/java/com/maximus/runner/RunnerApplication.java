package com.maximus.runner;

import com.maximus.runner.application.RunnerService;
import com.maximus.runner.configuration.RunnerConfig;
import com.maximus.runner.configuration.RunnerConfigLoader;
import com.maximus.runner.infrastructure.logging.RunnerLog;

public final class RunnerApplication {

    private RunnerApplication() {
    }

    public static void main(String[] args) {
        RunnerConfig config = RunnerConfigLoader.load(args);
        RunnerService runner = new RunnerService(config);

        RunnerLog.info("Starting Runner; target=" + config.serverHost() + ":" + config.serverPort());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            RunnerLog.info("Shutting down...");
            runner.shutdown();
        }, "runner-shutdown"));

        runner.start();
        RunnerLog.info("Runner stopped");
    }
}
