package com.maximus.runner;

import com.maximus.runner.application.RunnerService;
import com.maximus.runner.configuration.RunnerConfig;

public class RunnerApplication {

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

        RunnerService.initialize(config);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println();
            System.out.println("[RUNNER] Shutting down...");
            RunnerService.getInstance().shutdown();
        }));

        RunnerService.getInstance().start();

        System.out.println("[RUNNER] Runner stopped");
    }
}
