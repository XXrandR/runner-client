package com.maximus.runner;

import com.maximus.runner.application.RunnerEngine;
import com.maximus.runner.config.RunnerConfig;

public class App {

    public static void main(String[] args) throws Exception {

        RunnerConfig config = RunnerConfig.defaults();

        System.out.println("==================================================");
        System.out.println("[RUNNER] Starting Runner");
        System.out.println(
                "[RUNNER] Target: "
                        + config.serverHost()
                        + ":"
                        + config.serverPort()
        );
        System.out.println("==================================================");

        RunnerEngine runnerEngine = new RunnerEngine(config);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println();
            System.out.println("[RUNNER] Shutting down...");
            runnerEngine.shutdown();
        }));

        runnerEngine.run();

        System.out.println("[RUNNER] Runner stopped");
    }
}
