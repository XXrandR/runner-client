package com.maximus.runner.infrastructure.grpc;

import com.maximus.runner.RunnerRequest;
import com.maximus.runner.RunnerServiceGrpc;
import com.maximus.runner.ServerResponse;
import com.maximus.runner.application.port.RunnerConnection;
import com.maximus.runner.configuration.RunnerConfig;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class GrpcSession implements RunnerConnection {

    private static final long CHANNEL_READY_TIMEOUT_MS = 10_000;

    private final RunnerConfig config;
    private final Object sendLock = new Object();

    private ManagedChannel channel;
    private StreamObserver<RunnerRequest> requestStream;

    public GrpcSession(RunnerConfig config) {
        this.config = config;
    }

    @Override
    public void open(ConnectionListener listener) {
        channel = NettyChannelBuilder
                .forAddress(config.serverHost(), config.serverPort())
                .usePlaintext()
                .build();

        System.out.println("[RUNNER] gRPC channel created");
        System.out.println("[RUNNER] Waiting for channel READY...");

        waitForChannelReady();

        System.out.println("[RUNNER] Opening Connect() stream...");

        StreamObserver<ServerResponse> responseObserver = new StreamObserver<>() {

            @Override
            public void onNext(ServerResponse response) {
                listener.onResponse(response);
            }

            @Override
            public void onError(Throwable throwable) {
                listener.onError(throwable);
            }

            @Override
            public void onCompleted() {
                listener.onCompleted();
            }
        };

        RunnerServiceGrpc.RunnerServiceStub client =
                RunnerServiceGrpc.newStub(channel).withWaitForReady();

        try {
            requestStream = client.connect(responseObserver);
        } catch (Exception exception) {
            channel.shutdownNow();
            channel = null;
            throw new GrpcSessionOpenException(
                    "Failed to create Connect() stream",
                    exception
            );
        }

        System.out.println("[RUNNER] ✓ Connect() stream created");
    }

    @Override
    public void send(RunnerRequest request) {
        synchronized (sendLock) {
            if (requestStream == null) {
                throw new IllegalStateException("gRPC session is not open");
            }
            requestStream.onNext(request);
        }
    }

    public void completeRequestStream() {
        synchronized (sendLock) {
            if (requestStream == null) {
                return;
            }

            try {
                requestStream.onCompleted();
            } catch (Exception exception) {
                System.out.println(
                        "[RUNNER] Error closing request stream: "
                                + exception.getMessage()
                );
            } finally {
                requestStream = null;
            }
        }
    }

    @Override
    public void close() {
        completeRequestStream();

        if (channel != null) {
            channel.shutdown();
            channel = null;
        }

        System.out.println("[RUNNER] gRPC channel shutdown");
    }

    private void waitForChannelReady() {
        long deadline = System.currentTimeMillis() + CHANNEL_READY_TIMEOUT_MS;
        ConnectivityState state = channel.getState(true);

        while (state != ConnectivityState.READY) {

            if (state == ConnectivityState.SHUTDOWN) {
                throw new GrpcSessionOpenException(
                        "Channel shutdown before becoming READY",
                        null
                );
            }

            if (state == ConnectivityState.TRANSIENT_FAILURE) {
                throw new GrpcSessionOpenException(
                        "Channel failed to connect (TRANSIENT_FAILURE)",
                        null
                );
            }

            if (System.currentTimeMillis() >= deadline) {
                throw new GrpcSessionOpenException(
                        "Channel not READY within "
                                + CHANNEL_READY_TIMEOUT_MS
                                + "ms (state="
                                + state
                                + ")",
                        new TimeoutException("channel not ready")
                );
            }

            CountDownLatch stateChange = new CountDownLatch(1);
            ConnectivityState observedState = state;

            channel.notifyWhenStateChanged(observedState, stateChange::countDown);

            try {
                long remaining = deadline - System.currentTimeMillis();
                stateChange.await(Math.max(remaining, 1), TimeUnit.MILLISECONDS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new GrpcSessionOpenException(
                        "Interrupted while waiting for channel READY",
                        exception
                );
            }

            state = channel.getState(true);
        }

        System.out.println("[RUNNER] ✓ Channel READY");
    }
}
