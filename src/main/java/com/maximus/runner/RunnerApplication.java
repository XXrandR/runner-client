package com.maximus.runner;

import com.maximus.runner.application.RunnerService;
import com.maximus.runner.configuration.RunnerConfig;
import com.maximus.runner.configuration.RunnerConfigLoader;

public class RunnerApplication {

    public static void main(String[] args) throws Exception {

        System.out.println("==================================================");
        System.out.println("[RUNNER] Configuración");
        System.out.println("==================================================");

        RunnerConfig config = RunnerConfigLoader.load(args);

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
