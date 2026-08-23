package com.maximus.runner;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.atomic.AtomicBoolean;

public class App {

    private static final String SERVER_HOST = "localhost";
    private static final int SERVER_PORT = 9090;

    private static final long HEARTBEAT_INTERVAL_MS = 5_000;

    public static void main(String[] args) throws Exception {

        System.out.println("==================================================");
        System.out.println("[RUNNER] Starting Runner");
        System.out.println("[RUNNER] Target: " + SERVER_HOST + ":" + SERVER_PORT);
        System.out.println("==================================================");


        // ============================================================
        // STATE
        // ============================================================

        AtomicBoolean streamActive = new AtomicBoolean(false);
        AtomicBoolean shutdown = new AtomicBoolean(false);


        // ============================================================
        // gRPC CHANNEL
        // ============================================================

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(SERVER_HOST, SERVER_PORT)
                .usePlaintext()
                .build();

        System.out.println("[RUNNER] gRPC channel created");


        // ============================================================
        // SERVER → RUNNER
        // ============================================================

        StreamObserver<ServerResponse> responseObserver =  
                new StreamObserver<>() {

                    @Override
                    public void onNext(ServerResponse response) {

                        System.out.println(
                                "[RUNNER] ← Response received"
                        );

                        if (response.hasHeartbeat()) {

                            Heartbeat heartbeat =
                                    response.getHeartbeat();

                            System.out.println(
                                    "[RUNNER] ← HEARTBEAT"
                                            + " | timestamp="
                                            + heartbeat.getTimestamp()
                            );

                        } else {

                            System.out.println(
                                    "[RUNNER] ← Unknown response payload"
                            );
                        }
                    }


                    @Override
                    public void onError(Throwable throwable) {

                        streamActive.set(false);

                        System.out.println();
                        System.out.println(
                                "[RUNNER] ✗ gRPC stream ERROR"
                        );

                        System.out.println(
                                "[RUNNER] Error type: "
                                        + throwable.getClass().getName()
                        );

                        System.out.println(
                                "[RUNNER] Error message: "
                                        + throwable.getMessage()
                        );


                        // ------------------------------------------------
                        // Extract gRPC status
                        // ------------------------------------------------

                        if (throwable instanceof StatusRuntimeException exception) {

                            Status status = exception.getStatus();

                            System.out.println(
                                    "[RUNNER] gRPC status: "
                                            + status.getCode()
                            );

                            System.out.println(
                                    "[RUNNER] gRPC description: "
                                            + status.getDescription()
                            );
                        }


                        System.out.println(
                                "[RUNNER] Heartbeat loop will stop"
                        );
                    }


                    @Override
                    public void onCompleted() {

                        streamActive.set(false);

                        System.out.println();
                        System.out.println(
                                "[RUNNER] ✗ Server closed the stream"
                        );

                        System.out.println(
                                "[RUNNER] Heartbeat loop will stop"
                        );
                    }
                };


        // ============================================================
        // RUNNER → SERVER
        // ============================================================

        RunnerServiceGrpc.RunnerServiceStub client =
                RunnerServiceGrpc.newStub(channel);


        System.out.println(
                "[RUNNER] Opening Connect() stream..."
        );


        StreamObserver<RunnerRequest> requestStream;

        try {

            requestStream = client.connect(responseObserver);

        } catch (Exception exception) {

            System.out.println();
            System.out.println(
                    "[RUNNER] ✗ Failed to create Connect() stream"
            );

            System.out.println(
                    "[RUNNER] Error: "
                            + exception.getMessage()
            );

            channel.shutdownNow();

            return;
        }


        // ============================================================
        // IMPORTANT:
        //
        // connect() itself does NOT prove that the RPC succeeded.
        //
        // The server can still immediately call:
        //
        //     responseObserver.onError(...)
        //
        // ============================================================

        streamActive.set(true);

        System.out.println(
                "[RUNNER] ✓ Connect() stream created"
        );

        System.out.println(
                "[RUNNER] Starting heartbeat"
        );


        // ============================================================
        // HEARTBEAT LOOP
        // ============================================================

        try {

            while (
                    streamActive.get()
                            && !shutdown.get()
            ) {

                long timestamp =
                        System.currentTimeMillis();


                // ----------------------------------------------------
                // Build Heartbeat
                // ----------------------------------------------------

                Heartbeat heartbeat =
                        Heartbeat.newBuilder()
                                .setTimestamp(timestamp)
                                .build();


                // ----------------------------------------------------
                // Build RunnerRequest
                // ----------------------------------------------------

                RunnerRequest request =
                        RunnerRequest.newBuilder()
                                .setHeartbeat(heartbeat)
                                .build();


                // ----------------------------------------------------
                // Send
                // ----------------------------------------------------

                try {

                    synchronized (requestStream) {

                        requestStream.onNext(request);
                    }

                    System.out.println(
                            "[RUNNER] → HEARTBEAT sent"
                                    + " | timestamp="
                                    + timestamp
                    );

                } catch (Exception exception) {

                    streamActive.set(false);

                    System.out.println();
                    System.out.println(
                            "[RUNNER] ✗ Failed to send heartbeat"
                    );

                    System.out.println(
                            "[RUNNER] Error: "
                                    + exception.getMessage()
                    );

                    break;
                }


                // ----------------------------------------------------
                // Wait
                // ----------------------------------------------------

                Thread.sleep(
                        HEARTBEAT_INTERVAL_MS
                );
            }

        } catch (InterruptedException exception) {

            Thread.currentThread().interrupt();

            System.out.println(
                    "[RUNNER] Heartbeat thread interrupted"
            );
        }

        // ===========================================================
        // HEALTH STATUS
        // ===========================================================



        // ============================================================
        // SHUTDOWN
        // ============================================================

        shutdown.set(true);

        System.out.println();
        System.out.println(
                "[RUNNER] Shutting down..."
        );


        try {

            requestStream.onCompleted();

        } catch (Exception exception) {

            System.out.println(
                    "[RUNNER] Error closing request stream: "
                            + exception.getMessage()
            );
        }


        channel.shutdown();

        System.out.println(
                "[RUNNER] gRPC channel shutdown"
        );

        System.out.println(
                "[RUNNER] Runner stopped"
        );
    }
}