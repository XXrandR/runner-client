package com.maximus.runner.application.command;

import com.maximus.runner.Command;
import com.maximus.runner.CommandResult;

@FunctionalInterface
public interface CommandExecutor {
    CommandResult.Builder execute(Command command, CommandContext context) throws Exception;
}
