package com.maximus.runner.infrastructure.connectors;

import com.google.protobuf.ByteString;
import com.maximus.runner.*;
import com.maximus.runner.application.command.*;
import com.maximus.runner.configuration.connectors.ConnectorConfig;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;

public final class SshCommandExecutor implements CommandExecutor {
    private final ConnectorConfig config;
    public SshCommandExecutor(ConnectorConfig config) { this.config = config; }
    @Override public CommandResult.Builder execute(Command command, CommandContext context) throws Exception {
        var request = command.getSsh();
        if (request.getCommand().isBlank()) throw new IllegalArgumentException("SSH command is required");
        var client = SshConnections.connect(config.connection(request.getConnectionId(), "ssh"), context);
        var session = context.track(client.startSession());
        var remote = context.track(session.exec(request.getCommand()));
        int limit = BoundedStreams.limit(request.getMaxOutputBytes(), context);
        ExecutorService readers = Executors.newFixedThreadPool(2, task -> {
            Thread thread = new Thread(task, "runner-ssh-output"); thread.setDaemon(true); return thread;
        });
        context.track(() -> readers.shutdownNow());
        Future<byte[]> stdout = readers.submit(() -> BoundedStreams.read(remote.getInputStream(), limit, context));
        Future<byte[]> stderr = readers.submit(() -> BoundedStreams.read(remote.getErrorStream(), limit, context));
        try (var input = remote.getOutputStream()) { input.write(request.getStdin().getBytes(StandardCharsets.UTF_8)); }
        remote.join(context.remainingMillis(), TimeUnit.MILLISECONDS);
        Integer exit = remote.getExitStatus();
        if (exit == null) throw new TimeoutException("SSH command did not finish");
        return CommandResult.newBuilder().setSsh(SshResponse.newBuilder().setExitCode(exit)
                .setStdout(ByteString.copyFrom(stdout.get(context.remainingMillis(), TimeUnit.MILLISECONDS)))
                .setStderr(ByteString.copyFrom(stderr.get(context.remainingMillis(), TimeUnit.MILLISECONDS))));
    }
}
