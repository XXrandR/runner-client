package com.maximus.runner.configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Loads {@link RunnerConfig} from CLI arguments and/or interactive console input.
 * {@code serverHost} and {@code serverPort} are required; all other fields are optional.
 */
public final class RunnerConfigLoader {

    private RunnerConfigLoader() {
    }

    public static RunnerConfig load(String[] args) {
        Map<String, String> options = parseArgs(args);
        RunnerConfig defaults = RunnerConfig.defaults();
        boolean interactive = !hasRequiredFromCli(options);

        try (Scanner scanner = new Scanner(System.in)) {
            String host = resolveRequired(options, "host", "Servidor (host)", interactive, scanner);
            int port = resolveRequiredPort(options, interactive, scanner);

            String credential = resolveOptional(options, "credential", "Credencial", defaults.credential(), interactive, scanner);
            String runnerId = resolveOptional(options, "runner-id", "Runner ID", defaults.runnerId(), interactive, scanner);
            String runnerVersion = resolveOptional(options, "runner-version", "Versión del runner", defaults.runnerVersion(), interactive, scanner);
            int protocolVersion = resolveOptionalInt(options, "protocol-version", "Versión de protocolo", defaults.protocolVersion(), interactive, scanner);
            long initialReconnectDelayMs = resolveOptionalLong(
                    options, "reconnect-initial-ms", "Backoff inicial (ms)", defaults.initialReconnectDelayMs(), interactive, scanner);
            long maxReconnectDelayMs = resolveOptionalLong(
                    options, "reconnect-max-ms", "Backoff máximo (ms)", defaults.maxReconnectDelayMs(), interactive, scanner);
            long fallbackHeartbeatIntervalMs = resolveOptionalLong(
                    options, "heartbeat-interval-ms", "Intervalo heartbeat (ms)", defaults.fallbackHeartbeatIntervalMs(), interactive, scanner);

            return new RunnerConfig(
                    host,
                    port,
                    credential,
                    runnerId,
                    runnerVersion,
                    protocolVersion,
                    initialReconnectDelayMs,
                    maxReconnectDelayMs,
                    fallbackHeartbeatIntervalMs
            );
        }
    }

    private static boolean hasRequiredFromCli(Map<String, String> options) {
        String host = options.get("host");
        String port = options.get("port");
        return host != null && !host.isBlank() && port != null && !port.isBlank();
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> options = new HashMap<>();
        if (args == null) {
            return options;
        }

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("--")) {
                String key;
                String value;
                int eq = arg.indexOf('=');
                if (eq > 2) {
                    key = arg.substring(2, eq);
                    value = arg.substring(eq + 1);
                } else {
                    key = arg.substring(2);
                    if (i + 1 >= args.length || args[i + 1].startsWith("--")) {
                        throw new IllegalArgumentException("Missing value for --" + key);
                    }
                    value = args[++i];
                }
                options.put(key, value);
            } else if (!options.containsKey("host")) {
                options.put("host", arg);
            } else if (!options.containsKey("port")) {
                options.put("port", arg);
            } else {
                throw new IllegalArgumentException("Unexpected argument: " + arg);
            }
        }
        return options;
    }

    private static String resolveRequired(
            Map<String, String> options,
            String key,
            String label,
            boolean interactive,
            Scanner scanner
    ) {
        String value = options.get(key);
        if (value != null && !value.isBlank()) {
            return value.trim();
        }
        if (!interactive) {
            throw new IllegalArgumentException("Missing required --" + key);
        }
        return promptRequired(label, scanner);
    }

    private static int resolveRequiredPort(Map<String, String> options, boolean interactive, Scanner scanner) {
        String value = options.get("port");
        if (value != null && !value.isBlank()) {
            return parsePort(value);
        }
        if (!interactive) {
            throw new IllegalArgumentException("Missing required --port");
        }
        while (true) {
            System.out.print("Puerto [requerido]: ");
            String input = scanner.nextLine().trim();
            if (input.isEmpty()) {
                System.out.println("[RUNNER] El puerto es obligatorio.");
                continue;
            }
            try {
                return parsePort(input);
            } catch (IllegalArgumentException e) {
                System.out.println("[RUNNER] " + e.getMessage());
            }
        }
    }

    private static int parsePort(String value) {
        try {
            int port = Integer.parseInt(value.trim());
            if (port < 1 || port > 65_535) {
                throw new IllegalArgumentException("Puerto inválido (1-65535): " + value);
            }
            return port;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Puerto inválido: " + value);
        }
    }

    private static String resolveOptional(
            Map<String, String> options,
            String key,
            String label,
            String defaultValue,
            boolean interactive,
            Scanner scanner
    ) {
        String value = options.get(key);
        if (value != null && !value.isBlank()) {
            return value.trim();
        }
        if (!interactive) {
            return defaultValue;
        }
        return promptOptional(label, defaultValue, scanner);
    }

    private static int resolveOptionalInt(
            Map<String, String> options,
            String key,
            String label,
            int defaultValue,
            boolean interactive,
            Scanner scanner
    ) {
        String value = options.get(key);
        if (value != null && !value.isBlank()) {
            return parseInt(value, label);
        }
        if (!interactive) {
            return defaultValue;
        }
        return promptOptionalInt(label, defaultValue, scanner);
    }

    private static long resolveOptionalLong(
            Map<String, String> options,
            String key,
            String label,
            long defaultValue,
            boolean interactive,
            Scanner scanner
    ) {
        String value = options.get(key);
        if (value != null && !value.isBlank()) {
            return parseLong(value, label);
        }
        if (!interactive) {
            return defaultValue;
        }
        return promptOptionalLong(label, defaultValue, scanner);
    }

    private static String promptRequired(String label, Scanner scanner) {
        while (true) {
            System.out.print(label + " [requerido]: ");
            String input = scanner.nextLine().trim();
            if (!input.isEmpty()) {
                return input;
            }
            System.out.println("[RUNNER] Este campo es obligatorio.");
        }
    }

    private static String promptOptional(String label, String defaultValue, Scanner scanner) {
        System.out.print(label + " [" + defaultValue + "]: ");
        String input = scanner.nextLine().trim();
        return input.isEmpty() ? defaultValue : input;
    }

    private static int promptOptionalInt(String label, int defaultValue, Scanner scanner) {
        while (true) {
            System.out.print(label + " [" + defaultValue + "]: ");
            String input = scanner.nextLine().trim();
            if (input.isEmpty()) {
                return defaultValue;
            }
            try {
                return parseInt(input, label);
            } catch (IllegalArgumentException e) {
                System.out.println("[RUNNER] " + e.getMessage());
            }
        }
    }

    private static long promptOptionalLong(String label, long defaultValue, Scanner scanner) {
        while (true) {
            System.out.print(label + " [" + defaultValue + "]: ");
            String input = scanner.nextLine().trim();
            if (input.isEmpty()) {
                return defaultValue;
            }
            try {
                return parseLong(input, label);
            } catch (IllegalArgumentException e) {
                System.out.println("[RUNNER] " + e.getMessage());
            }
        }
    }

    private static int parseInt(String value, String label) {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(label + " inválido: " + value);
        }
    }

    private static long parseLong(String value, String label) {
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(label + " inválido: " + value);
        }
    }
}
