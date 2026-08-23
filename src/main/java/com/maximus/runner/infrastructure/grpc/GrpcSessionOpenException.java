package com.maximus.runner.infrastructure.grpc;

public class GrpcSessionOpenException extends RuntimeException {

    public GrpcSessionOpenException(String message, Throwable cause) {
        super(message, cause);
    }
}
