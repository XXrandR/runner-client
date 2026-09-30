package com.maximus.runner.infrastructure.grpc;

import com.maximus.runner.RunnerRequest;
import com.maximus.runner.RunnerServiceGrpc;
import com.maximus.runner.ServerResponse;
import com.maximus.runner.application.port.RunnerConnection;
import com.maximus.runner.configuration.RunnerConfig;
import com.maximus.runner.infrastructure.logging.RunnerLog;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Owns exactly one channel and bidirectional Connect() stream. */
public final class GrpcSession implements RunnerConnection {

    private static final long CHANNEL_READY_TIMEOUT_MS = 10_000;

    private final RunnerConfig config;
    private final Object sendLock = new Object();
    private ManagedChannel channel;
    private StreamObserver<RunnerRequest> requestStream;
    private boolean closed;

    public GrpcSession(RunnerConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    @Override
    public void open(ConnectionListener listener) {
        Objects.requireNonNull(listener, "listener");
        ManagedChannel created;
        synchronized (sendLock) {
            if (closed || channel != null) {
                throw new IllegalStateException("gRPC session can only be opened once");
            }
            created = NettyChannelBuilder.forAddress(config.serverHost(), config.serverPort())
                    .usePlaintext().build();
            channel = created;
        }
        try {
            RunnerLog.info("gRPC channel created; waiting for READY");
            waitForChannelReady(created);
            StreamObserver<ServerResponse> responses = new StreamObserver<>() {
                @Override
                public void onNext(ServerResponse response) {
                    listener.onResponse(response);
                }

                @Override
                public void onError(Throwable error) {
                    listener.onError(error);
                }

                @Override
                public void onCompleted() {
                    listener.onCompleted();
                }
            };
            synchronized (sendLock) {
                if (closed || channel != created) {
                    throw new GrpcSessionOpenException("Session closed during connection", null);
                }
                requestStream = RunnerServiceGrpc.newStub(created)
                        .withWaitForReady().connect(responses);
            }
            RunnerLog.info("Connect() stream created");
        } catch (RuntimeException failure) {
            synchronized (sendLock) {
                if (channel == created) {
                    requestStream = null;
                    channel = null;
                }
            }
            created.shutdownNow();
            if (failure instanceof GrpcSessionOpenException openException) {
                throw openException;
            }
            throw new GrpcSessionOpenException("Failed to create Connect() stream", failure);
        }
    }

    @Override
    public void send(RunnerRequest request) {
        synchronized (sendLock) {
            if (closed || requestStream == null) {
                throw new IllegalStateException("gRPC session is not open");
            }
            requestStream.onNext(Objects.requireNonNull(request, "request"));
        }
    }

    @Override
    public void close() {
        ManagedChannel previous;
        synchronized (sendLock) {
            if (closed) {
                return;
            }
            closed = true;
            if (requestStream != null) {
                try {
                    requestStream.onCompleted();
                } catch (RuntimeException failure) {
                    RunnerLog.error("Error closing request stream", failure);
                } finally {
                    requestStream = null;
                }
            }
            previous = channel;
            channel = null;
        }
        if (previous != null) {
            previous.shutdownNow();
            RunnerLog.info("gRPC channel shutdown");
        }
    }

    private void waitForChannelReady(ManagedChannel candidate) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(CHANNEL_READY_TIMEOUT_MS);
        ConnectivityState state = candidate.getState(true);
        while (state != ConnectivityState.READY) {
            if (state == ConnectivityState.SHUTDOWN || state == ConnectivityState.TRANSIENT_FAILURE) {
                throw new GrpcSessionOpenException("Channel failed to connect (" + state + ")", null);
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                throw new GrpcSessionOpenException("Channel not READY within "
                        + CHANNEL_READY_TIMEOUT_MS + "ms (state=" + state + ")",
                        new TimeoutException("channel not ready"));
            }
            CountDownLatch stateChange = new CountDownLatch(1);
            candidate.notifyWhenStateChanged(state, stateChange::countDown);
            try {
                stateChange.await(remaining, TimeUnit.NANOSECONDS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new GrpcSessionOpenException("Interrupted while waiting for channel READY", exception);
            }
            state = candidate.getState(true);
        }
        RunnerLog.info("Channel READY");
    }
}
