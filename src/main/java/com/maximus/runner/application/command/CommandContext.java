package com.maximus.runner.application.command;

import com.maximus.runner.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;

/** One ordered result stream and one terminal result, even during cancel/timeout races. */
public final class CommandContext implements AutoCloseable {
    public String commandId() { return commandId; }
    public int maxResultBytes() { return maxBytes; }
    private final String commandId;
    private final Consumer<CommandResult> sink;
    private final long deadline;
    private final int maxBytes;
    private final Deque<AutoCloseable> resources = new ConcurrentLinkedDeque<>();
    private long sequence;
    private volatile boolean finished;
    private volatile CommandResult latest;

    public CommandContext(String commandId, Consumer<CommandResult> sink, long timeoutMillis, int maxBytes) {
        this.commandId = commandId; this.sink = sink; this.maxBytes = maxBytes;
        this.deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    }
    public long remainingMillis() throws InterruptedException {
        if (finished || Thread.currentThread().isInterrupted()) throw new InterruptedException("Command stopped");
        long remaining = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
        if (remaining <= 0) throw new InterruptedException("Command deadline reached");
        return remaining;
    }
    public boolean deadlineExceeded() { return System.nanoTime() >= deadline; }
    public int maxBytes() { return maxBytes; }
    public boolean finished() { return finished; }
    public CommandResult latest() { return latest; }
    public synchronized void progress(CommandResultStatus status, CommandResult.Builder body) {
        if (!finished) send(status, false, body);
    }
    public synchronized boolean complete(CommandResultStatus status, CommandResult.Builder body) {
        if (finished) return false;
        finished = true;
        send(status, true, body);
        return true;
    }
    private void send(CommandResultStatus status, boolean terminal, CommandResult.Builder body) {
        CommandResult result = body.setCommandId(commandId).setSequence(sequence++)
                .setStatus(status).setFinal(terminal).setSuccess(status == CommandResultStatus.SUCCEEDED).build();
        if (result.getSerializedSize() > maxBytes) {
            finished = true;
            result = CommandResult.newBuilder().setCommandId(commandId).setSequence(result.getSequence())
                    .setStatus(CommandResultStatus.FAILED).setFinal(true).setError("Result exceeds maxResultBytes").build();
        }
        latest = result;
        sink.accept(result);
    }
    public <T extends AutoCloseable> T track(T resource) throws Exception {
        resources.addFirst(resource);
        if (finished && resources.remove(resource)) { resource.close(); throw new InterruptedException("Command stopped"); }
        return resource;
    }
    @Override public void close() {
        AutoCloseable resource;
        while ((resource = resources.pollFirst()) != null) {
            try { resource.close(); } catch (Exception ignored) { }
        }
    }
}
