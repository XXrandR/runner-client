package com.maximus.runner.application.lifecycle;

import com.maximus.runner.ServerResponse;
import com.maximus.runner.application.port.RunnerConnection;
import com.maximus.runner.domain.SessionContext;

public interface ActiveSessionHandler {

    void onSessionStarted(SessionContext sessionContext, RunnerConnection connection);

    void onSessionStopped();

    void onActiveResponse(ServerResponse response);
}
