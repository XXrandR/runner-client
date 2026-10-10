package com.maximus.runner.infrastructure.connectors;

import com.maximus.runner.*;
import com.maximus.runner.application.command.*;
import com.maximus.runner.configuration.connectors.ConnectorConfig;
import java.util.*;

public final class ConnectorRegistry {
    private ConnectorRegistry() {}
    public static CommandRegistry create(ConnectorConfig config) {
        try {
            CommandRegistry registry = new CommandRegistry();
            var http = new HttpCommandExecutor(config);
            for (String type : List.of("api.request", "http.request", "soap.request")) registry.register(type, Command.BodyCase.API, http);
            registry.register("ssh.exec", Command.BodyCase.SSH, new SshCommandExecutor(config));
            var sftp = new SftpCommandExecutor(config);
            for (String operation : List.of("list", "stat", "get", "put", "mkdir", "rename", "delete"))
                registry.register("sftp." + operation, Command.BodyCase.SFTP, (command, context) -> {
                    require(operation.equals(command.getSftp().getOperation())); return sftp.execute(command, context);
                });
            var mail = new MailCommandExecutor(config);
            for (String operation : List.of("send", "list", "read"))
                registry.register("mail." + operation, Command.BodyCase.MAIL, (command, context) -> {
                    require(operation.equals(command.getMail().getOperation())); return mail.execute(command, context);
                });
            var messaging = new MessagingCommandExecutor(config);
            for (String operation : List.of("publish", "receive"))
                registry.register("messaging." + operation, Command.BodyCase.MESSAGING, (command, context) -> {
                    require(operation.equals(command.getMessaging().getOperation())); return messaging.execute(command, context);
                });
            var event = new EventCommandExecutor(config);
            for (String operation : List.of("emit", "timer", "watch_files"))
                registry.register("event." + operation, Command.BodyCase.EVENT, (command, context) -> {
                    require(operation.equals(command.getEvent().getOperation())); return event.execute(command, context);
                });
            registry.register("process.exec", Command.BodyCase.PROCESS, new ProcessCommandExecutor(config));
            var file = new FileCommandExecutor(config);
            for (FileOperation operation : FileOperation.values()) {
                if (operation == FileOperation.FILE_OPERATION_UNSPECIFIED || operation == FileOperation.UNRECOGNIZED) continue;
                registry.register("file." + operation.name().toLowerCase(Locale.ROOT), Command.BodyCase.FILE, (command, context) -> {
                    require(operation == command.getFile().getOperation()); return file.execute(command, context);
                });
            }
            var database = new DatabaseCommandExecutor(config);
            for (DatabaseOperation operation : List.of(DatabaseOperation.QUERY, DatabaseOperation.EXECUTE, DatabaseOperation.PROCEDURE, DatabaseOperation.STREAM, DatabaseOperation.CUSTOM)) {
                CommandExecutor action = (command, context) -> {
                    require(operation == command.getDatabase().getOperation()); return database.execute(command, context);
                };
                registry.register("db." + operation.name().toLowerCase(Locale.ROOT), Command.BodyCase.DATABASE, action);
                registry.register("database." + operation.name().toLowerCase(Locale.ROOT), Command.BodyCase.DATABASE, action);
            }
            registry.register("runner.ping", Command.BodyCase.GENERIC, (command, context) -> ResultValues.generic(Map.of("alive", true)));
            registry.register("command.ping", Command.BodyCase.GENERIC, (command, context) -> ResultValues.generic(Map.of("alive", true)));
            registry.register("runner.capabilities", Command.BodyCase.GENERIC, (command, context) -> {
                List<String> actions = new ArrayList<>(registry.capabilities()); actions.add("command.cancel");
                return ResultValues.generic(Map.of("actions", actions, "maxTimeoutSeconds", config.maxTimeoutSeconds(), "maxResultBytes", config.maxResultBytes()));
            });
            return registry;
        } catch (Exception exception) { throw new IllegalStateException("Could not initialize connector registry", exception); }
    }
    private static void require(boolean valid) { if (!valid) throw new IllegalArgumentException("Command type and operation do not match"); }
}
