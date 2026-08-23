package com.maximus.runner.infrastructure.grpc;

import com.maximus.runner.ServerResponse;

public interface GrpcSessionListener {

    void onResponse(ServerResponse response);

    void onError(Throwable error);

    void onCompleted();
}
