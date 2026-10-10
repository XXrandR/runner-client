package com.maximus.runner.infrastructure.connectors;

import com.google.protobuf.ByteString;
import com.maximus.runner.*;
import com.maximus.runner.application.command.*;
import com.maximus.runner.configuration.connectors.ConnectorConfig;
import java.io.*;
import java.nio.file.*;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;

public final class FileCommandExecutor implements CommandExecutor {
    private final WorkspacePaths paths;
    public FileCommandExecutor(ConnectorConfig config) throws IOException { paths = new WorkspacePaths(config.workspace()); }
    @Override public CommandResult.Builder execute(Command command, CommandContext context) throws Exception {
        FileRequest request = command.getFile();
        Path path = paths.resolve(request.getPath());
        if ((request.getOperation() == FileOperation.DELETE || request.getOperation() == FileOperation.MOVE) && path.equals(paths.resolve(".")))
            throw new IllegalArgumentException("The workspace root cannot be deleted or moved");
        return switch (request.getOperation()) {
            case LIST -> {
                List<Map<String, ?>> files = new ArrayList<>();
                try (var stream = Files.list(path)) {
                    Iterator<Path> entries = stream.iterator();
                    while (entries.hasNext()) {
                        context.remainingMillis();
                        if (files.size() >= 1000) throw new IllegalArgumentException("Directory listing exceeds 1000 entries");
                        Path entry = entries.next();
                        var attributes = Files.readAttributes(entry, java.nio.file.attribute.BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                        files.add(Map.of("name", entry.getFileName().toString(), "directory", attributes.isDirectory(), "size", attributes.size()));
                    }
                }
                yield ResultValues.generic(Map.of("files", files));
            }
            case STAT -> ResultValues.generic(Map.of("path", request.getPath(), "size", Files.size(path), "directory", Files.isDirectory(path)));
            case READ, DOWNLOAD -> read(command, context, path);
            case WRITE, UPLOAD -> {
                if (request.getOffset() < 0) throw new IllegalArgumentException("Invalid file offset");
                if (!request.getChunkSha256().isBlank() && !sha256(request.getData().toByteArray()).equalsIgnoreCase(request.getChunkSha256()))
                    throw new IllegalArgumentException("File chunk checksum does not match");
                if (!request.getOverwrite() && request.getOffset() == 0 && Files.exists(path)) throw new IllegalArgumentException("File already exists");
                if (request.getOffset() > 0 && (!Files.exists(path) || Files.size(path) != request.getOffset()))
                    throw new IllegalArgumentException("File chunk offset does not match the existing size");
                try (var channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                    if (request.getOffset() == 0) channel.truncate(0);
                    channel.position(request.getOffset()); ByteBuffer bytes = request.getData().asReadOnlyByteBuffer();
                    while (bytes.hasRemaining()) { context.remainingMillis(); channel.write(bytes); }
                }
                yield ResultValues.generic(Map.of("bytes_written", request.getData().size(), "size", Files.size(path)));
            }
            case MKDIR -> { Files.createDirectories(path); yield ResultValues.generic(Map.of("created", request.getPath())); }
            case COPY, MOVE -> {
                Path destination = paths.resolve(request.getDestination());
                CopyOption[] options = request.getOverwrite() ? new CopyOption[]{StandardCopyOption.REPLACE_EXISTING} : new CopyOption[0];
                if (request.getOperation() == FileOperation.COPY) Files.copy(path, destination, options); else Files.move(path, destination, options);
                yield ResultValues.generic(Map.of("destination", request.getDestination()));
            }
            case DELETE -> {
                if (request.getRecursive()) throw new IllegalArgumentException("Recursive deletion is not enabled");
                Files.delete(path); yield ResultValues.generic(Map.of("deleted", request.getPath()));
            }
            default -> throw new IllegalArgumentException("Unsupported file operation");
        };
    }
    private CommandResult.Builder read(Command command, CommandContext context, Path path) throws Exception {
        FileRequest request = command.getFile();
        if (request.getOffset() < 0 || request.getOffset() > Files.size(path)) throw new IllegalArgumentException("Invalid read offset");
        int size = request.getChunkSize() == 0 ? Math.min(65536, context.maxBytes() / 2) : BoundedStreams.limit(request.getChunkSize(), context);
        try (InputStream input = context.track(Files.newInputStream(path))) {
            input.skipNBytes(request.getOffset());
            if (!command.getStreaming()) {
                byte[] data = BoundedStreams.read(input, BoundedStreams.limit(0, context), context);
                return CommandResult.newBuilder().setData(ByteString.copyFrom(data)).setContentType("application/octet-stream");
            }
            long offset = request.getOffset(), sequence = 0;
            byte[] bytes;
            while ((bytes = input.readNBytes(size)).length > 0) {
                context.remainingMillis();
                FileTransferChunk chunk = FileTransferChunk.newBuilder().setTransferId(request.getTransferId().isBlank() ? command.getCommandId() : request.getTransferId())
                        .setOffset(offset).setSequence(sequence++).setData(ByteString.copyFrom(bytes)).setSize(bytes.length)
                        .setTotalSize(Files.size(path)).setSha256(sha256(bytes)).setFinal(offset + bytes.length >= Files.size(path)).build();
                context.progress(CommandResultStatus.STREAMING, CommandResult.newBuilder().setFileChunk(chunk)); offset += bytes.length;
            }
            return ResultValues.generic(Map.of("bytes_read", offset - request.getOffset(), "size", offset));
        }
    }
    private static String sha256(byte[] data) throws Exception { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data)); }
}
