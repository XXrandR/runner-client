package com.maximus.runner.infrastructure.connectors;

import com.maximus.runner.*;
import com.maximus.runner.application.command.*;
import com.maximus.runner.configuration.connectors.ConnectorConfig;
import net.schmizz.sshj.sftp.*;
import java.nio.file.*;
import java.util.*;

public final class SftpCommandExecutor implements CommandExecutor {
    private final ConnectorConfig config;
    private final WorkspacePaths paths;
    public SftpCommandExecutor(ConnectorConfig config) throws Exception { this.config = config; paths = new WorkspacePaths(config.workspace()); }
    @Override public CommandResult.Builder execute(Command command, CommandContext context) throws Exception {
        var request = command.getSftp();
        var profile = config.connection(request.getConnectionId(), "ssh", "sftp");
        var client = SshConnections.connect(profile, context);
        var sftp = context.track(client.newSFTPClient());
        String remote = remotePath(profile.text("remoteRoot", "/"), request.getRemotePath());
        String realRoot = sftp.canonicalize(remotePath("/", profile.text("remoteRoot", "/")));
        validateRemote(sftp, realRoot, remote);
        String operation = request.getOperation();
        SftpResponse.Builder result = SftpResponse.newBuilder().setRemotePath(remote);
        long maxTransfer = profile.number("maxTransferBytes", 16_777_216);
        if (maxTransfer <= 0) throw new IllegalArgumentException("Invalid SFTP maxTransferBytes");
        switch (operation) {
            case "list" -> {
                var files = sftp.ls(remote);
                if (files.size() > 1000) throw new IllegalArgumentException("SFTP listing exceeds 1000 entries");
                for (var file : files) result.addFiles(info(file.getPath(), file.getName(), file.getAttributes()));
            }
            case "stat" -> result.addFiles(info(remote, Path.of(remote).getFileName().toString(), sftp.stat(remote)));
            case "get" -> {
                long size = sftp.stat(remote).getSize();
                if (size > maxTransfer) throw new IllegalArgumentException("SFTP file exceeds transfer limit");
                Path local = paths.resolve(request.getLocalPath());
                if (Files.exists(local) && !request.getOverwrite()) throw new IllegalArgumentException("Destination exists; overwrite must be explicit");
                Files.createDirectories(local.getParent());
                // Download to a temporary file, then move only after successful completion.
                Path temporary = Files.createTempFile(local.getParent(), ".sftp-", ".part");
                try {
                    try (RemoteFile file = sftp.open(remote); var output = Files.newOutputStream(temporary)) {
                        copy(file.new RemoteFileInputStream(), output, maxTransfer, context);
                    }
                    if (request.getOverwrite()) Files.move(temporary, local, StandardCopyOption.REPLACE_EXISTING);
                    else Files.move(temporary, local);
                    result.setTransferredBytes(Files.size(local)).setLocalPath(local.toString());
                } finally { Files.deleteIfExists(temporary); }
            }
            case "put" -> {
                Path local = paths.resolve(request.getLocalPath());
                if (!Files.isRegularFile(local) || Files.size(local) > maxTransfer) throw new IllegalArgumentException("SFTP source exceeds transfer limit or is not a file");
                Set<OpenMode> modes = EnumSet.of(OpenMode.WRITE, OpenMode.CREAT);
                if (request.getOverwrite()) modes.add(OpenMode.TRUNC); else modes.add(OpenMode.EXCL);
                try (RemoteFile file = sftp.open(remote, modes); var input = Files.newInputStream(local)) {
                    long transferred = copy(input, file.new RemoteFileOutputStream(), maxTransfer, context);
                    result.setTransferredBytes(transferred).setLocalPath(local.toString());
                }
            }
            case "mkdir" -> sftp.mkdir(remote);
            case "rename" -> {
                String destination = remotePath(profile.text("remoteRoot", "/"), request.getDestination());
                validateRemote(sftp, realRoot, destination);
                if (!request.getOverwrite() && sftp.statExistence(destination) != null) throw new IllegalArgumentException("Destination exists");
                sftp.rename(remote, destination);
            }
            case "delete" -> sftp.rm(remote); // Only a file; no recursive removal.
            default -> throw new IllegalArgumentException("Unsupported SFTP operation");
        }
        return CommandResult.newBuilder().setSftp(result);
    }
    private static long copy(java.io.InputStream input, java.io.OutputStream output, long max, CommandContext context) throws Exception {
        long size = 0; byte[] buffer = new byte[65_536];
        try (input; output) {
            for (int read; (read = input.read(buffer)) >= 0;) {
                context.remainingMillis(); size += read;
                if (size > max) throw new IllegalArgumentException("SFTP transfer exceeds configured limit");
                output.write(buffer, 0, read);
            }
        }
        return size;
    }
    private static FileInfo info(String path, String name, FileAttributes attributes) {
        FileType type = switch (attributes.getType()) {
            case DIRECTORY -> FileType.DIRECTORY;
            case REGULAR -> FileType.FILE;
            case SYMLINK -> FileType.SYMBOLIC_LINK;
            default -> FileType.OTHER;
        };
        return FileInfo.newBuilder().setPath(path).setName(name).setType(type).setSize(Math.max(0, attributes.getSize())).build();
    }
    private static void validateRemote(SFTPClient client, String root, String path) throws Exception {
        String actual;
        if (client.statExistence(path) != null) actual = client.canonicalize(path);
        else {
            int slash = path.lastIndexOf('/');
            actual = client.canonicalize(slash <= 0 ? "/" : path.substring(0, slash)) + path.substring(slash);
        }
        if (!root.equals("/") && !actual.equals(root) && !actual.startsWith(root + "/"))
            throw new IllegalArgumentException("SFTP symlink resolves outside remoteRoot");
    }
    static String remotePath(String root, String path) {
        if (!root.startsWith("/")) throw new IllegalArgumentException("SFTP remoteRoot must be absolute");
        // SFTP uses POSIX paths even on a Windows runner.
        String normalizedRoot = normalize(root);
        String normalized = normalize(path.startsWith("/") ? path : normalizedRoot + "/" + path);
        if (!normalizedRoot.equals("/") && !normalized.equals(normalizedRoot) && !normalized.startsWith(normalizedRoot + "/"))
            throw new IllegalArgumentException("SFTP path is outside remoteRoot");
        if (path.isBlank()) throw new IllegalArgumentException("SFTP remote_path is required");
        return normalized;
    }
    private static String normalize(String path) {
        if (path.contains("\\") || path.indexOf('\0') >= 0) throw new IllegalArgumentException("Invalid SFTP path");
        Deque<String> parts = new ArrayDeque<>();
        for (String part : path.split("/")) {
            if (part.isEmpty() || part.equals(".")) continue;
            if (part.equals("..")) { if (parts.isEmpty()) throw new IllegalArgumentException("Invalid SFTP traversal"); parts.removeLast(); }
            else parts.add(part);
        }
        return "/" + String.join("/", parts);
    }
}
