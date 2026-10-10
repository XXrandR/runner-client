package com.maximus.runner.infrastructure.connectors;

import com.maximus.runner.application.command.CommandContext;
import com.maximus.runner.configuration.connectors.ConnectorConfig.Profile;
import net.schmizz.sshj.SSHClient;
import java.nio.file.*;

final class SshConnections {
    private SshConnections() {}
    static SSHClient connect(Profile profile, CommandContext context) throws Exception {
        SSHClient client = context.track(new SSHClient());
        Path knownHosts = Path.of(profile.required("knownHosts")).toAbsolutePath();
        if (!Files.isRegularFile(knownHosts)) throw new IllegalArgumentException("SSH knownHosts file is required");
        client.loadKnownHosts(knownHosts.toFile());
        client.setConnectTimeout((int) Math.min(context.remainingMillis(), 15_000));
        client.setTimeout((int) Math.min(context.remainingMillis(), Integer.MAX_VALUE));
        client.connect(profile.required("host"), profile.number("port", 22));
        String username = profile.required("username");
        if (profile.values().has("privateKey")) {
            Path key = Path.of(profile.required("privateKey")).toAbsolutePath();
            if (!Files.isRegularFile(key)) throw new IllegalArgumentException("SSH privateKey file does not exist");
            client.authPublickey(username, client.loadKeys(key.toString(), profile.text("passphrase", null)));
        } else client.authPassword(username, profile.required("password"));
        return client;
    }
}
