package com.maximus.runner.infrastructure.connectors;

import com.maximus.runner.application.command.CommandContext;
import java.io.*;

final class BoundedStreams {
    private BoundedStreams() { }
    static byte[] read(InputStream input, int limit, CommandContext context) throws IOException, InterruptedException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int count;
        while ((count = input.read(buffer)) != -1) {
            context.remainingMillis();
            if ((long) output.size() + count > limit) throw new IllegalArgumentException("Connector output exceeds configured limit");
            output.write(buffer, 0, count);
        }
        return output.toByteArray();
    }
    static int limit(long requested, CommandContext context) {
        int maximum = Math.max(1024, context.maxBytes() / 2 - 1024);
        if (requested == 0) return maximum;
        if (requested < 0 || requested > maximum) throw new IllegalArgumentException("Requested output limit exceeds maxResultBytes");
        return (int) requested;
    }
}
