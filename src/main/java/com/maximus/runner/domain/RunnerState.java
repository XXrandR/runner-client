package com.maximus.runner.domain;

public enum RunnerState {

    /**
     * Runner is registered in the API but has not established
     * its first connection yet.
     */
    PROVISIONED,

    /**
     * Runner has no active gRPC session.
     *
     * This is a recoverable state. The Runner is expected to
     * continuously attempt reconnection.
     */
    DISCONNECTED,

    /**
     * A gRPC connection exists and the Runner is authenticating.
     */
    AUTHENTICATING,

    /**
     * Authentication succeeded, but the session handshake
     * has not yet completed.
     */
    AUTHENTICATED,

    /**
     * Authentication succeeded and the Runner is negotiating
     * the session/protocol with the server.
     */
    HANDSHAKING,

    /**
     * The Runner has completed authentication and handshake
     * and the session is fully operational.
     */
    ACTIVE
}