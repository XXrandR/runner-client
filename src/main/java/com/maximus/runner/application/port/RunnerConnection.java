package com.maximus.runner.application.port;

import com.maximus.runner.RunnerRequest;
import com.maximus.runner.ServerResponse;

public interface RunnerConnection {

    void open(ConnectionListener listener);

    void send(RunnerRequest request);

    void close();

    interface ConnectionListener {

        void onResponse(ServerResponse response);

        void onError(Throwable error);

        void onCompleted();
    }
}
