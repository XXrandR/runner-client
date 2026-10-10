package com.maximus.runner.infrastructure.connectors;

import com.google.protobuf.Timestamp;
import com.maximus.runner.*;
import com.maximus.runner.application.command.*;
import com.maximus.runner.configuration.connectors.ConnectorConfig;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

public final class EventCommandExecutor implements CommandExecutor {
    private final WorkspacePaths paths;
    public EventCommandExecutor(ConnectorConfig config) throws Exception { paths = new WorkspacePaths(config.workspace()); }
    @Override public CommandResult.Builder execute(Command command, CommandContext context) throws Exception {
        var request = command.getEvent();
        if (!request.getTopic().matches("[A-Za-z0-9._-]{1,150}")) throw new IllegalArgumentException("Invalid event topic");
        int limit = request.getMaxEvents() == 0 ? 1 : request.getMaxEvents();
        if (limit < 1 || limit > 1000) throw new IllegalArgumentException("Event max_events must be between 1 and 1000");
        if (request.getOperation().equals("emit")) return event(request, "runner", request.getData());
        int emitted = 0;
        if (request.getOperation().equals("timer")) {
            long millis = request.getInterval().getSeconds() * 1000L + request.getInterval().getNanos() / 1_000_000L;
            if (request.getInterval().getSeconds() < 0 || request.getInterval().getSeconds() > 3600
                    || request.getInterval().getNanos() < 0 || request.getInterval().getNanos() >= 1_000_000_000 || millis < 10)
                throw new IllegalArgumentException("Timer interval must be at least 10ms and at most one hour");
            while (emitted < limit) {
                Thread.sleep(Math.min(millis, context.remainingMillis())); context.remainingMillis();
                context.progress(CommandResultStatus.STREAMING, event(request, "timer", request.getData())); emitted++;
            }
        } else if (request.getOperation().equals("watch_files")) {
            Path directory = paths.resolve(request.getPath());
            if (!Files.isDirectory(directory)) throw new IllegalArgumentException("Event path must be a directory");
            WatchService watcher = context.track(FileSystems.getDefault().newWatchService());
            directory.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.ENTRY_DELETE);
            while (emitted < limit) {
                WatchKey key = watcher.poll(Math.min(200, context.remainingMillis()), TimeUnit.MILLISECONDS);
                if (key == null) continue;
                for (WatchEvent<?> change : key.pollEvents()) {
                    var data = ResultValues.struct(Map.of("directory", directory.toString(), "name", Objects.toString(change.context(), ""), "kind", change.kind().name()));
                    context.progress(CommandResultStatus.STREAMING, event(request, "filesystem", data));
                    if (++emitted >= limit) break;
                }
                if (!key.reset()) throw new IllegalArgumentException("Watched directory is no longer available");
            }
        } else throw new IllegalArgumentException("Unsupported event operation");
        return ResultValues.generic(Map.of("events", emitted));
    }
    private static CommandResult.Builder event(EventRequest request, String source, com.google.protobuf.Struct data) {
        Instant now = Instant.now();
        return CommandResult.newBuilder().setEvent(EventResponse.newBuilder().setEventId(UUID.randomUUID().toString())
                .setTopic(request.getTopic()).setSource(source).setData(data)
                .setOccurredAt(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano())));
    }
}
