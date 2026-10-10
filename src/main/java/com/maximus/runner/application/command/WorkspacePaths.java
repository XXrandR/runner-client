package com.maximus.runner.application.command;

import java.io.IOException;
import java.nio.file.*;

/** Resolves both lexical traversal and existing symbolic-link ancestors. */
public final class WorkspacePaths {
    private final Path root;
    public WorkspacePaths(Path root) throws IOException { this.root = root.toRealPath(); }
    public Path resolve(String value) throws IOException {
        Path input = value == null || value.isBlank() ? Path.of(".") : Path.of(value);
        Path target = (input.isAbsolute() ? input : root.resolve(input)).toAbsolutePath().normalize();
        if (!target.startsWith(root)) throw new IllegalArgumentException("Path is outside the runner workspace");
        Path ancestor = target;
        while (ancestor != null && !Files.exists(ancestor, LinkOption.NOFOLLOW_LINKS)) ancestor = ancestor.getParent();
        if (ancestor == null || !ancestor.toRealPath().startsWith(root)) throw new IllegalArgumentException("Path escapes the workspace through a symbolic link");
        return target;
    }
}
