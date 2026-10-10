package com.maximus.runner.infrastructure.connectors;

import com.google.protobuf.ByteString;
import com.maximus.runner.*;
import com.maximus.runner.application.command.*;
import com.maximus.runner.configuration.connectors.ConnectorConfig;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/** Executes a named executable profile; arguments are passed individually, without an implicit shell. */
public final class ProcessCommandExecutor implements CommandExecutor {
    private final ConnectorConfig config;
    private final WorkspacePaths paths;
    public ProcessCommandExecutor(ConnectorConfig config) throws IOException { this.config = config; this.paths = new WorkspacePaths(config.workspace()); }
    @Override public CommandResult.Builder execute(Command command, CommandContext context) throws Exception {
        ProcessRequest request = command.getProcess();
        var profile = config.connection(request.getConnectionId(), "process");
        Path executable = Path.of(profile.required("executable"));
        if (!executable.isAbsolute() || !Files.isRegularFile(executable)) throw new IllegalArgumentException("Process profile needs an existing absolute executable path");
        List<String> arguments = new ArrayList<>(); arguments.add(executable.toString()); arguments.addAll(request.getArgumentsList());
        ProcessBuilder builder = new ProcessBuilder(arguments).directory(paths.resolve(request.getWorkingDirectory()).toFile());
        builder.environment().putAll(profile.strings("environment"));
        Process process = builder.start();
        context.track(() -> { process.descendants().forEach(ProcessHandle::destroyForcibly); process.destroyForcibly(); });
        int limit = BoundedStreams.limit(request.getMaxOutputBytes(), context);
        ExecutorService readers = Executors.newFixedThreadPool(2, task -> { Thread t = new Thread(task, "runner-process-output"); t.setDaemon(true); return t; });
        context.track(() -> readers.shutdownNow());
        Future<byte[]> out = readers.submit(() -> readOutput(process.getInputStream(), limit, context, process));
        Future<byte[]> err = readers.submit(() -> readOutput(process.getErrorStream(), limit, context, process));
        try (OutputStream input = process.getOutputStream()) { request.getStdin().writeTo(input); }
        if (!process.waitFor(context.remainingMillis(), TimeUnit.MILLISECONDS)) throw new InterruptedException("Process deadline reached");
        byte[] stdout = out.get(context.remainingMillis(), TimeUnit.MILLISECONDS);
        byte[] stderr = err.get(context.remainingMillis(), TimeUnit.MILLISECONDS);
        return CommandResult.newBuilder().setProcess(ProcessResponse.newBuilder().setExitCode(process.exitValue())
                .setStdout(ByteString.copyFrom(stdout)).setStderr(ByteString.copyFrom(stderr)));
    }
    private static byte[] readOutput(InputStream input, int limit, CommandContext context, Process process) throws Exception {
        try { return BoundedStreams.read(input, limit, context); }
        catch (Exception failure) { process.descendants().forEach(ProcessHandle::destroyForcibly); process.destroyForcibly(); throw failure; }
    }
}
