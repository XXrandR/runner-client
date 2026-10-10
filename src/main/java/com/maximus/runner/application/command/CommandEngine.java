package com.maximus.runner.application.command;

import com.maximus.runner.*;
import com.maximus.runner.configuration.connectors.ConnectorConfig;
import com.maximus.runner.infrastructure.logging.RunnerLog;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/** Bounded, priority-ordered execution off the gRPC callback thread. */
public final class CommandEngine implements AutoCloseable {
    private final CommandRegistry registry;
    private final ConnectorConfig config;
    private final ThreadPoolExecutor workers;
    private final ScheduledExecutorService timer;
    private final Semaphore permits;
    private final AtomicLong order = new AtomicLong();
    private final LinkedHashMap<String, Job> jobs = new LinkedHashMap<>();
    private boolean closed;

    public CommandEngine(CommandRegistry registry, ConnectorConfig config) {
        this.registry = registry; this.config = config;
        ThreadFactory threads = task -> { Thread t = new Thread(task, "runner-command"); t.setDaemon(true); return t; };
        workers = new ThreadPoolExecutor(config.workers(), config.workers(), 0, TimeUnit.MILLISECONDS,
                new PriorityBlockingQueue<>(), threads);
        timer = Executors.newSingleThreadScheduledExecutor(threads);
        permits = new Semaphore(config.workers() + config.queueCapacity());
    }

    public synchronized void submit(Command command, Consumer<CommandResult> sink) {
        if (command.getCommandId().isBlank()) throw new IllegalArgumentException("command_id is required");
        Job previous = jobs.get(command.getCommandId());
        if (previous != null) {
            if (!previous.command.equals(command)) {
                sink.accept(CommandResult.newBuilder().setCommandId(command.getCommandId()).setStatus(CommandResultStatus.REJECTED)
                        .setFinal(true).setError("command_id already belongs to a different request").build());
            } else if (previous.context.latest() != null) sink.accept(previous.context.latest());
            return;
        }
        long timeout = config.defaultTimeoutSeconds() * 1000L;
        String invalid = null;
        if (command.hasTimeout()) {
            var duration = command.getTimeout();
            if (duration.getSeconds() < 0 || duration.getNanos() < 0 || duration.getNanos() >= 1_000_000_000
                    || duration.getSeconds() > config.maxTimeoutSeconds()) invalid = "Invalid command timeout";
            else timeout = duration.getSeconds() * 1000L + duration.getNanos() / 1_000_000L;
        }
        if (command.getPriorityValue() < 0 || command.getPriorityValue() > 4 || command.getVersion() > 1) invalid = "Unsupported command priority/version";
        if (timeout <= 0 || timeout > config.maxTimeoutSeconds() * 1000L) invalid = "Command timeout exceeds configured limits";
        Job job = new Job(command, new CommandContext(command.getCommandId(), sink, Math.max(1, timeout), config.maxResultBytes()));
        jobs.put(command.getCommandId(), job);
        trimHistory();
        if (closed || invalid != null) { reject(job, closed ? "Runner session is closed" : invalid); return; }
        if (command.getType().equals("command.cancel")) {
            if (!command.hasCancel() || command.getCancel().getTargetCommandId().isBlank() || command.getCancel().getCascade()) {
                reject(job, "Cancellation requires a target; cascade is not supported by this runner"); return;
            }
            Job target = jobs.get(command.getCancel().getTargetCommandId());
            boolean cancelled = target != null && stop(target, CommandResultStatus.CANCELLED, "Cancelled by request");
            job.context.complete(CommandResultStatus.SUCCEEDED, ResultValues.generic(Map.of("cancelled", cancelled)));
            return;
        }
        CommandExecutor executor;
        try { executor = registry.resolve(command); }
        catch (IllegalArgumentException exception) { reject(job, exception.getMessage()); return; }
        if (!permits.tryAcquire()) { reject(job, "Runner command queue is full"); return; }
        try { job.context.progress(CommandResultStatus.ACCEPTED, CommandResult.newBuilder()); }
        catch (RuntimeException failure) { permits.release(); job.context.close(); throw failure; }
        job.task = new PrioritizedTask(job, executor, command.getPriorityValue(), order.getAndIncrement());
        job.timeout = timer.schedule(() -> stop(job, CommandResultStatus.TIMED_OUT, "Command deadline exceeded"), timeout, TimeUnit.MILLISECONDS);
        try { workers.execute(job.task); }
        catch (RejectedExecutionException exception) { job.task.cancel(false); reject(job, "Runner session is closed"); }
    }

    private void reject(Job job, String message) { job.context.complete(CommandResultStatus.REJECTED, CommandResult.newBuilder().setError(message)); }
    private boolean stop(Job job, CommandResultStatus status, String message) {
        boolean stopped;
        try { stopped = job.context.complete(status, CommandResult.newBuilder().setError(message)); }
        catch (RuntimeException exception) { stopped = true; }
        if (stopped) {
            if (job.task != null) { job.task.cancel(true); workers.remove(job.task); }
            if (job.timeout != null) job.timeout.cancel(false);
            Thread cleanup = new Thread(job.context::close, "runner-command-cleanup"); cleanup.setDaemon(true); cleanup.start();
        }
        return stopped;
    }
    private void trimHistory() {
        if (jobs.size() <= 512) return;
        Iterator<Job> iterator = jobs.values().iterator();
        while (jobs.size() > 512 && iterator.hasNext()) if (iterator.next().context.finished()) iterator.remove();
    }
    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        jobs.values().forEach(job -> stop(job, CommandResultStatus.CANCELLED, "Runner session ended"));
        workers.shutdownNow(); timer.shutdownNow();
    }
    private static final class Job {
        final Command command; final CommandContext context;
        volatile RunnableFuture<?> task; volatile ScheduledFuture<?> timeout;
        Job(Command command, CommandContext context) { this.command = command; this.context = context; }
    }
    private final class PrioritizedTask extends FutureTask<Void> implements Comparable<PrioritizedTask> {
        final int priority; final long position;
        PrioritizedTask(Job job, CommandExecutor executor, int priority, long position) {
            super(() -> {
                try {
                    job.context.remainingMillis();
                    job.context.progress(CommandResultStatus.RUNNING, CommandResult.newBuilder());
                    CommandResult.Builder result = executor.execute(job.command, job.context);
                    CommandResultStatus status = result.hasSsh() && result.getSsh().getExitCode() != 0
                            || result.hasProcess() && result.getProcess().getExitCode() != 0
                            ? CommandResultStatus.FAILED : CommandResultStatus.SUCCEEDED;
                    job.context.complete(status, result);
                } catch (IllegalArgumentException exception) {
                    job.context.complete(CommandResultStatus.REJECTED, CommandResult.newBuilder().setError(exception.getMessage()));
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    job.context.complete(job.context.deadlineExceeded() ? CommandResultStatus.TIMED_OUT : CommandResultStatus.CANCELLED, CommandResult.newBuilder().setError("Command interrupted or deadline reached"));
                } catch (Exception exception) {
                    RunnerLog.warning("Command failed | id=" + job.command.getCommandId() + " | cause=" + exception.getClass().getSimpleName());
                    boolean timedOut = exception instanceof TimeoutException || exception instanceof java.net.http.HttpTimeoutException
                            || exception instanceof java.net.SocketTimeoutException || exception instanceof java.sql.SQLTimeoutException;
                    job.context.complete(timedOut ? CommandResultStatus.TIMED_OUT : CommandResultStatus.FAILED, CommandResult.newBuilder()
                            .setError("Connector operation failed (" + exception.getClass().getSimpleName() + ")"));
                } finally {
                    if (job.timeout != null) job.timeout.cancel(false);
                    job.context.close();
                }
                return null;
            });
            this.priority = priority == 0 ? 2 : priority; this.position = position;
        }
        @Override public int compareTo(PrioritizedTask other) {
            int value = Integer.compare(other.priority, priority);
            return value != 0 ? value : Long.compare(position, other.position);
        }
        @Override protected void done() { permits.release(); }
    }
}
