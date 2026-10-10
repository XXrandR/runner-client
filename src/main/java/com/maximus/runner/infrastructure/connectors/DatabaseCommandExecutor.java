package com.maximus.runner.infrastructure.connectors;

import com.google.gson.*;
import com.google.protobuf.util.JsonFormat;
import com.maximus.runner.*;
import com.maximus.runner.application.command.*;
import com.maximus.runner.configuration.connectors.ConnectorConfig;
import com.mongodb.*;
import com.mongodb.client.*;
import org.bson.Document;
import java.math.*;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

public final class DatabaseCommandExecutor implements CommandExecutor {
    private final ConnectorConfig config;
    public DatabaseCommandExecutor(ConnectorConfig config) { this.config = config; }
    @Override public CommandResult.Builder execute(Command command, CommandContext context) throws Exception {
        var request = command.getDatabase();
        var profile = config.connection(request.getDatasource(), "jdbc", "postgresql", "sqlserver", "mysql", "sqlite", "mongodb");
        if (!request.getDatabaseType().isBlank() && !request.getDatabaseType().equalsIgnoreCase(profile.kind())
                && !profile.kind().equals("jdbc")) throw new IllegalArgumentException("database_type does not match datasource kind");
        if (request.hasTransaction() && !request.getTransaction().getTransactionId().isBlank())
            throw new IllegalArgumentException("Transactions across commands are not supported");
        return profile.kind().equals("mongodb") ? mongo(command, profile, context) : jdbc(command, profile, context);
    }
    private CommandResult.Builder jdbc(Command command, ConnectorConfig.Profile profile, CommandContext context) throws Exception {
        var request = command.getDatabase();
        if (!Set.of(DatabaseOperation.QUERY, DatabaseOperation.EXECUTE, DatabaseOperation.PROCEDURE, DatabaseOperation.STREAM).contains(request.getOperation()))
            throw new IllegalArgumentException("Unsupported SQL operation; use query, execute, procedure or stream");
        if (request.getStatement().isBlank()) throw new IllegalArgumentException("SQL statement is required");
        Properties properties = new Properties(); properties.putAll(profile.strings("properties"));
        if (profile.values().has("username")) properties.setProperty("user", profile.required("username"));
        if (profile.values().has("password")) properties.setProperty("password", profile.required("password"));
        // Driver-specific login/socket timeouts can be supplied through the profile's properties.
        Connection connection = context.track(DriverManager.getConnection(profile.required("url"), properties));
        connection.setAutoCommit(false);
        if (!request.getCatalog().isBlank()) connection.setCatalog(request.getCatalog());
        if (!request.getSchema().isBlank()) connection.setSchema(request.getSchema());
        if (request.hasTransaction() && !request.getTransaction().getIsolationLevel().isBlank())
            connection.setTransactionIsolation(switch (request.getTransaction().getIsolationLevel().toUpperCase(Locale.ROOT)) {
                case "READ_COMMITTED" -> Connection.TRANSACTION_READ_COMMITTED;
                case "READ_UNCOMMITTED" -> Connection.TRANSACTION_READ_UNCOMMITTED;
                case "REPEATABLE_READ" -> Connection.TRANSACTION_REPEATABLE_READ;
                case "SERIALIZABLE" -> Connection.TRANSACTION_SERIALIZABLE;
                default -> throw new IllegalArgumentException("Unsupported transaction isolation_level");
            });
        PreparedStatement statement = context.track(request.getOperation() == DatabaseOperation.PROCEDURE
                ? connection.prepareCall(request.getStatement()) : connection.prepareStatement(request.getStatement()));
        context.track(statement::cancel);
        long timeout = request.hasTimeout() ? ConnectorTimeouts.within(request.getTimeout(), context) : context.remainingMillis();
        statement.setQueryTimeout((int) Math.max(1, (timeout + 999) / 1000));
        int maxRows = maxRows(request);
        if (request.getOperation() == DatabaseOperation.QUERY || request.getOperation() == DatabaseOperation.STREAM)
            statement.setMaxRows(maxRows + 1);
        Set<Integer> positions = new HashSet<>();
        for (int i = 0; i < request.getParametersCount(); i++) {
            var parameter = request.getParameters(i); int position = parameter.getPosition() == 0 ? i + 1 : parameter.getPosition();
            if (position < 1 || !positions.add(position)) throw new IllegalArgumentException("Invalid or repeated SQL parameter position");
            if (parameter.getIsNull()) statement.setNull(position, Types.NULL);
            else statement.setObject(position, switch (parameter.getValueCase()) {
                case STRING_VALUE -> parameter.getStringValue();
                case INT64_VALUE -> parameter.getInt64Value();
                case UINT64_VALUE -> new BigDecimal(Long.toUnsignedString(parameter.getUint64Value()));
                case DOUBLE_VALUE -> parameter.getDoubleValue();
                case BOOL_VALUE -> parameter.getBoolValue();
                case BYTES_VALUE -> parameter.getBytesValue().toByteArray();
                case TIMESTAMP_VALUE -> java.sql.Timestamp.from(Instant.ofEpochSecond(parameter.getTimestampValue().getSeconds(), parameter.getTimestampValue().getNanos()));
                case OBJECT_VALUE -> JsonFormat.printer().print(parameter.getObjectValue());
                case ARRAY_VALUE -> JsonFormat.printer().print(parameter.getArrayValue());
                default -> throw new IllegalArgumentException("SQL parameter value is missing");
            });
        }
        try {
            boolean hasRows = statement.execute();
            DatabaseResponse.Builder response = DatabaseResponse.newBuilder();
            if (hasRows) {
                try (ResultSet rows = statement.getResultSet()) {
                    ResultSetMetaData metadata = rows.getMetaData(); List<String> names = new ArrayList<>();
                    for (int i = 1; i <= metadata.getColumnCount(); i++) {
                        String name = metadata.getColumnLabel(i);
                        if (names.contains(name)) name += "_" + i;
                        names.add(name);
                        response.addColumns(DatabaseColumn.newBuilder().setName(name).setDatabaseType(metadata.getColumnTypeName(i))
                                .setNullable(metadata.isNullable(i) != ResultSetMetaData.columnNoNulls).setPrecision(metadata.getPrecision(i)).setScale(metadata.getScale(i)));
                    }
                    int read = 0, bytes = 0; boolean streaming = command.getStreaming() || request.getStreamResults() || request.getOperation() == DatabaseOperation.STREAM;
                    int maxBytes = resultLimit(request, context);
                    while (rows.next()) {
                        context.remainingMillis();
                        if (read >= maxRows) { response.setHasMore(true); break; }
                        Map<String, Object> values = new LinkedHashMap<>();
                        for (int i = 1; i <= names.size(); i++) values.put(names.get(i - 1), sqlValue(rows.getObject(i)));
                        var row = ResultValues.struct(values);
                        if (row.getSerializedSize() + response.getColumnsList().stream().mapToInt(DatabaseColumn::getSerializedSize).sum() > maxBytes - 2048)
                            throw new IllegalArgumentException("One SQL row exceeds result limit");
                        if (bytes + row.getSerializedSize() > maxBytes - 2048) {
                            if (!streaming) { response.setHasMore(true); break; }
                            context.progress(CommandResultStatus.STREAMING, CommandResult.newBuilder().setDatabase(response.clone().setHasMore(true)));
                            response.clearRows(); bytes = 0;
                        }
                        response.addRows(row); bytes += row.getSerializedSize(); read++;
                    }
                    response.setAffectedRows(read);
                }
            } else response.setAffectedRows(Math.max(0, statement.getLargeUpdateCount()));
            context.remainingMillis(); connection.commit();
            return CommandResult.newBuilder().setDatabase(response);
        } catch (Exception exception) { try { connection.rollback(); } catch (SQLException ignored) { } throw exception; }
    }
    private static Object sqlValue(Object value) throws Exception {
        if (value == null) return null;
        if (value instanceof byte[] bytes) return Base64.getEncoder().encodeToString(bytes);
        if (value instanceof BigDecimal || value instanceof BigInteger || value instanceof Long) return value.toString();
        if (value instanceof Number || value instanceof Boolean || value instanceof String) return value;
        if (value instanceof java.sql.Blob || value instanceof java.sql.Clob) throw new IllegalArgumentException("SQL LOB values require an explicit bounded cast in the query");
        return value.toString();
    }
    private CommandResult.Builder mongo(Command command, ConnectorConfig.Profile profile, CommandContext context) throws Exception {
        var request = command.getDatabase();
        if (request.getParametersCount() > 0 || request.hasTransaction()) throw new IllegalArgumentException("Mongo parameters/transactions are not supported by these actions");
        JsonObject options = JsonParser.parseString(JsonFormat.printer().print(request.getOptions())).getAsJsonObject();
        String collectionName = options.has("collection") ? options.get("collection").getAsString() : "";
        if (collectionName.isBlank()) throw new IllegalArgumentException("Mongo options.collection is required");
        long timeout = request.hasTimeout() ? ConnectorTimeouts.within(request.getTimeout(), context) : context.remainingMillis();
        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(profile.required("uri")))
                .timeout(timeout, TimeUnit.MILLISECONDS).build();
        MongoClient client = context.track(MongoClients.create(settings));
        var collection = client.getDatabase(profile.required("database")).getCollection(collectionName);
        String operation = options.has("operation") ? options.get("operation").getAsString() : "find";
        DatabaseResponse.Builder response = DatabaseResponse.newBuilder();
        String json = request.getStatement().isBlank() ? "{}" : request.getStatement();
        if (Set.of("find", "aggregate").contains(operation)) {
            if (!Set.of(DatabaseOperation.QUERY, DatabaseOperation.STREAM, DatabaseOperation.CUSTOM).contains(request.getOperation())) throw new IllegalArgumentException("Mongo read requires query/stream/custom");
            MongoIterable<Document> iterable;
            if (operation.equals("aggregate")) {
                List<Document> pipeline = new ArrayList<>();
                for (JsonElement stage : JsonParser.parseString(json).getAsJsonArray()) pipeline.add(Document.parse(stage.toString()));
                iterable = collection.aggregate(pipeline).maxTime(context.remainingMillis(), TimeUnit.MILLISECONDS);
            } else iterable = collection.find(Document.parse(json)).limit(maxRows(request) + 1).maxTime(context.remainingMillis(), TimeUnit.MILLISECONDS);
            try (MongoCursor<Document> cursor = context.track(iterable.iterator())) {
                int read = 0, bytes = 0, max = resultLimit(request, context);
                while (cursor.hasNext()) {
                    context.remainingMillis();
                    if (read >= maxRows(request)) { response.setHasMore(true); break; }
                    var rowBuilder = com.google.protobuf.Struct.newBuilder(); JsonFormat.parser().merge(cursor.next().toJson(), rowBuilder);
                    var row = rowBuilder.build();
                    if (row.getSerializedSize() > max - 2048) throw new IllegalArgumentException("Mongo document exceeds result limit");
                    if (bytes + row.getSerializedSize() > max - 2048) {
                        if (!command.getStreaming() && !request.getStreamResults() && request.getOperation() != DatabaseOperation.STREAM) { response.setHasMore(true); break; }
                        context.progress(CommandResultStatus.STREAMING, CommandResult.newBuilder().setDatabase(response.clone().setHasMore(true)));
                        response.clearRows(); bytes = 0;
                    }
                    response.addRows(row); bytes += row.getSerializedSize(); read++;
                }
                response.setAffectedRows(read);
            }
        } else {
            if (request.getOperation() != DatabaseOperation.EXECUTE && request.getOperation() != DatabaseOperation.CUSTOM) throw new IllegalArgumentException("Mongo writes require execute/custom");
            Document filter = options.has("filter") ? Document.parse(options.get("filter").toString()) : new Document();
            switch (operation) {
                case "insertOne" -> {
                    var result = collection.insertOne(Document.parse(json));
                    response.setAffectedRows(1).setGeneratedId(Objects.toString(result.getInsertedId(), ""));
                }
                case "updateOne" -> response.setAffectedRows(collection.updateOne(filter, Document.parse(json)).getModifiedCount());
                case "deleteOne" -> response.setAffectedRows(collection.deleteOne(Document.parse(json)).getDeletedCount());
                default -> throw new IllegalArgumentException("Unsupported Mongo operation");
            }
        }
        return CommandResult.newBuilder().setDatabase(response);
    }
    private static int maxRows(DatabaseRequest request) {
        long max = request.getMaxRows() == 0 ? 1000 : request.getMaxRows();
        if (max < 1 || max > 10_000) throw new IllegalArgumentException("max_rows must be between 1 and 10000");
        return (int) max;
    }
    private static int resultLimit(DatabaseRequest request, CommandContext context) {
        long max = request.getMaxResultBytes() == 0 ? context.maxResultBytes() : request.getMaxResultBytes();
        if (max < 4096 || max > context.maxResultBytes()) throw new IllegalArgumentException("Invalid database max_result_bytes");
        return (int) max - 512;
    }
}
