package com.maximus.runner.domain;

/**
 * Session data negotiated during handshake.
 */
public record SessionContext(
        String sessionId,
        int heartbeatIntervalSeconds,
        int protocolVersion
) {
}
