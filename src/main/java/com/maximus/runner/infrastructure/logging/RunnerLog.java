package com.maximus.runner.infrastructure.logging;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/** One place to configure console and rotating file logs for the runner. */
public final class RunnerLog {

    private static final Logger LOGGER = configure();

    private RunnerLog() {
    }

    public static void info(String message) {
        LOGGER.info(message);
    }

    public static void warning(String message) {
        LOGGER.warning(message);
    }

    public static void error(String message, Throwable cause) {
        LOGGER.log(Level.SEVERE, message, cause);
    }

    private static Logger configure() {
        Logger logger = Logger.getLogger("com.maximus.runner");
        logger.setUseParentHandlers(false);
        logger.setLevel(Level.INFO);

        ConsoleHandler console = new ConsoleHandler();
        console.setLevel(Level.INFO);
        console.setFormatter(new SimpleFormatter());
        logger.addHandler(console);

        try {
            Path directory = logDirectory();
            Files.createDirectories(directory);
            FileHandler file = new FileHandler(
                    directory.resolve("runner-%g.log").toString(),
                    2 * 1024 * 1024,
                    5,
                    true
            );
            file.setLevel(Level.INFO);
            file.setFormatter(new SimpleFormatter());
            logger.addHandler(file);
            logger.info("Logs: " + directory.toAbsolutePath());
        } catch (IOException | SecurityException | IllegalArgumentException exception) {
            logger.log(Level.WARNING, "Could not open runner log file; console logging remains available", exception);
        }
        return logger;
    }

    private static Path logDirectory() {
        String explicit = System.getenv("RUNNER_LOG_DIR");
        if (explicit != null && !explicit.isBlank()) {
            return Path.of(explicit);
        }
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        if (os.contains("win")) {
            String appData = System.getenv("LOCALAPPDATA");
            if (appData != null && !appData.isBlank()) {
                return Path.of(appData, "runner-client", "logs");
            }
        } else {
            String stateHome = System.getenv("XDG_STATE_HOME");
            if (stateHome != null && !stateHome.isBlank()) {
                return Path.of(stateHome, "runner-client", "logs");
            }
        }
        return Path.of(System.getProperty("user.home"), ".local", "state", "runner-client", "logs");
    }
}
