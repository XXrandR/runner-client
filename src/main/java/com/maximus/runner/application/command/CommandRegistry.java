package com.maximus.runner.application.command;

import com.maximus.runner.Command;
import java.util.*;

public final class CommandRegistry {
    private record Entry(Command.BodyCase body, CommandExecutor executor) { }
    private final Map<String, Entry> actions = new LinkedHashMap<>();
    public CommandRegistry register(String type, Command.BodyCase body, CommandExecutor executor) {
        if (actions.putIfAbsent(type, new Entry(body, executor)) != null) throw new IllegalArgumentException("Duplicate action: " + type);
        return this;
    }
    public Set<String> capabilities() { return Collections.unmodifiableSet(actions.keySet()); }
    public CommandExecutor resolve(Command command) {
        Entry entry = actions.get(command.getType());
        if (entry == null) throw new IllegalArgumentException("Unsupported action: " + command.getType());
        if (entry.body != command.getBodyCase()) throw new IllegalArgumentException("Command type and body do not match");
        return entry.executor;
    }
}
