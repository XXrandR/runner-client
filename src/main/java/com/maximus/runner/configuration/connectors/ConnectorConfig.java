package com.maximus.runner.configuration.connectors;

import com.google.gson.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/** Named connections belong to the machine; commands only reference their names. */
public final class ConnectorConfig {
    private final JsonObject document;
    private final Path workspace;

    public ConnectorConfig(JsonObject document, Path baseDirectory) throws IOException {
        this.document = document.deepCopy();
        Path configured = Path.of(string(document, "workspace", "runner-work"));
        this.workspace = (configured.isAbsolute() ? configured : baseDirectory.resolve(configured)).normalize();
        Files.createDirectories(workspace);
    }

    public static ConnectorConfig load() {
        try {
            String file = System.getenv("RUNNER_CONNECTORS_FILE");
            if (file == null || file.isBlank()) return new ConnectorConfig(new JsonObject(), Path.of(".").toAbsolutePath());
            Path path = Path.of(file).toAbsolutePath();
            try (var reader = Files.newBufferedReader(path)) {
                return new ConnectorConfig(JsonParser.parseReader(reader).getAsJsonObject(), path.getParent());
            }
        } catch (IOException | RuntimeException exception) {
            throw new IllegalStateException("Could not load runner connector configuration", exception);
        }
    }

    public Path workspace() { return workspace; }
    public int workers() { return boundedInt("workers", 4, 1, 32); }
    public int queueCapacity() { return boundedInt("queueCapacity", 32, 1, 1024); }
    public int maxResultBytes() { return boundedInt("maxResultBytes", 1_048_576, 1024, 2_097_152); }
    public int defaultTimeoutSeconds() { return boundedInt("defaultTimeoutSeconds", 30, 1, maxTimeoutSeconds()); }
    public int maxTimeoutSeconds() { return boundedInt("maxTimeoutSeconds", 300, 1, 3600); }

    public Profile connection(String name, String... allowedKinds) {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("connection_id is required");
        JsonObject connections = document.has("connections") ? document.getAsJsonObject("connections") : new JsonObject();
        if (!connections.has(name)) throw new IllegalArgumentException("Unknown connection: " + name);
        JsonObject values = connections.getAsJsonObject(name);
        String kind = string(values, "kind", "").toLowerCase(Locale.ROOT);
        if (!Arrays.asList(allowedKinds).contains(kind)) throw new IllegalArgumentException("Connection kind does not match the action");
        return new Profile(name, kind, values.deepCopy());
    }

    private int boundedInt(String key, int fallback, int min, int max) {
        int value = document.has(key) ? document.get(key).getAsInt() : fallback;
        if (value < min || value > max) throw new IllegalArgumentException("Invalid connector setting: " + key);
        return value;
    }

    private static String string(JsonObject object, String key, String fallback) {
        return object.has(key) ? object.get(key).getAsString() : fallback;
    }

    public record Profile(String name, String kind, JsonObject values) {
        public String text(String key, String fallback) {
            String value = string(values, key, fallback);
            if (value == null) return null;
            if (value.startsWith("${") && value.endsWith("}")) {
                String resolved = System.getenv(value.substring(2, value.length() - 1));
                if (resolved == null) throw new IllegalArgumentException("Required environment variable is missing for connection " + name);
                return resolved;
            }
            return value;
        }
        public String required(String key) {
            String value = text(key, "");
            if (value.isBlank()) throw new IllegalArgumentException("Connection " + name + " requires " + key);
            return value;
        }
        public int number(String key, int fallback) { return values.has(key) ? values.get(key).getAsInt() : fallback; }
        public boolean flag(String key, boolean fallback) { return values.has(key) ? values.get(key).getAsBoolean() : fallback; }
        public Map<String, String> strings(String key) {
            Map<String, String> result = new LinkedHashMap<>();
            if (values.has(key)) values.getAsJsonObject(key).entrySet().forEach(e -> {
                String value = e.getValue().getAsString();
                if (value.startsWith("${") && value.endsWith("}")) {
                    value = Objects.requireNonNull(System.getenv(value.substring(2, value.length() - 1)), "Missing connection environment variable");
                }
                result.put(e.getKey(), value);
            });
            return result;
        }
    }
}
