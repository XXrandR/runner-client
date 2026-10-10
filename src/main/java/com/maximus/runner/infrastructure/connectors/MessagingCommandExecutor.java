package com.maximus.runner.infrastructure.connectors;

import com.google.protobuf.ByteString;
import com.maximus.runner.*;
import com.maximus.runner.Command;
import com.maximus.runner.application.command.*;
import com.maximus.runner.configuration.connectors.ConnectorConfig;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.rabbitmq.client.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class MessagingCommandExecutor implements CommandExecutor {
    private final ConnectorConfig config;
    public MessagingCommandExecutor(ConnectorConfig config) { this.config = config; }
    @Override public CommandResult.Builder execute(Command command, CommandContext context) throws Exception {
        var request = command.getMessaging();
        if (!Set.of("publish", "receive").contains(request.getOperation())) throw new IllegalArgumentException("Unsupported messaging operation");
        var profile = config.connection(request.getConnectionId(), "mqtt", "rabbitmq", "kafka");
        if (request.getData().size() > context.maxResultBytes() / 2) throw new IllegalArgumentException("Message exceeds size limit");
        MessagingResponse response = switch (profile.kind()) {
            case "mqtt" -> mqtt(request, profile, context);
            case "rabbitmq" -> rabbit(request, profile, context);
            case "kafka" -> kafka(request, profile, context);
            default -> throw new IllegalArgumentException("Unknown broker kind");
        };
        return CommandResult.newBuilder().setMessaging(response);
    }
    private static int count(MessagingRequest request) {
        int count = request.getMaxMessages() == 0 ? 1 : request.getMaxMessages();
        if (count < 1 || count > 100) throw new IllegalArgumentException("max_messages must be between 1 and 100");
        return count;
    }
    private static long waitMillis(MessagingRequest request, CommandContext context) throws Exception {
        if (!request.hasWaitTimeout()) return Math.min(5000, context.remainingMillis());
        var value = request.getWaitTimeout();
        if (value.getSeconds() < 0 || value.getSeconds() > 3600 || value.getNanos() < 0 || value.getNanos() >= 1_000_000_000)
            throw new IllegalArgumentException("Invalid messaging wait_timeout");
        long millis = value.getSeconds() * 1000L + value.getNanos() / 1_000_000L;
        if (millis <= 0) throw new IllegalArgumentException("wait_timeout must be positive");
        return Math.min(millis, context.remainingMillis());
    }
    private static void append(MessagingResponse.Builder response, MessagingItem item, CommandContext context) {
        if (item.getSerializedSize() > context.maxResultBytes() / 2
                || response.build().getSerializedSize() + item.getSerializedSize() > context.maxResultBytes() - 2048)
            throw new IllegalArgumentException("Received messages exceed result size limit");
        response.addMessages(item); response.setCount(response.getMessagesCount());
    }
    private static MessagingResponse mqtt(MessagingRequest request, ConnectorConfig.Profile profile, CommandContext context) throws Exception {
        if (request.getTopic().isBlank()) throw new IllegalArgumentException("MQTT topic is required");
        if (request.getQos() > 2 || request.getQos() < 0) throw new IllegalArgumentException("MQTT qos must be 0, 1 or 2");
        var builder = MqttClient.builder().useMqttVersion3().identifier(profile.text("clientIdPrefix", "runner") + "-" + UUID.randomUUID())
                .serverHost(profile.required("host")).serverPort(profile.number("port", profile.flag("ssl", false) ? 8883 : 1883));
        if (profile.flag("ssl", false)) builder.sslWithDefaultConfig();
        if (profile.values().has("username")) builder.simpleAuth().username(profile.required("username"))
                .password(profile.required("password").getBytes(StandardCharsets.UTF_8)).applySimpleAuth();
        var client = builder.buildAsync();
        context.track(() -> client.disconnect());
        var connected = client.connect();
        connected.whenComplete((ack, error) -> { if (error == null && context.finished()) client.disconnect(); });
        connected.get(context.remainingMillis(), TimeUnit.MILLISECONDS);
        var response = MessagingResponse.newBuilder();
        MqttQos qos = MqttQos.fromCode(request.getQos());
        if (request.getOperation().equals("publish")) {
            if (!request.getHeadersMap().isEmpty() || !request.getKey().isBlank()) throw new IllegalArgumentException("MQTT 3.1.1 does not support message headers or keys");
            client.publishWith().topic(request.getTopic()).qos(qos).retain(request.getRetained())
                    .payload(request.getData().toByteArray()).send().get(context.remainingMillis(), TimeUnit.MILLISECONDS);
            response.setCount(1);
        } else {
            BlockingQueue<com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish> received = new ArrayBlockingQueue<>(128);
            AtomicBoolean overflow = new AtomicBoolean();
            client.subscribeWith().topicFilter(request.getTopic()).qos(qos).callback(message -> {
                if (!received.offer(message)) overflow.set(true);
            }).send().get(context.remainingMillis(), TimeUnit.MILLISECONDS);
            long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMillis(request, context));
            while (response.getMessagesCount() < count(request) && System.nanoTime() < end) {
                context.remainingMillis();
                if (overflow.get()) throw new IllegalArgumentException("MQTT receive queue is full");
                var message = received.poll(Math.max(1, Math.min(100, TimeUnit.NANOSECONDS.toMillis(end - System.nanoTime()))), TimeUnit.MILLISECONDS);
                if (message != null) append(response, MessagingItem.newBuilder().setTopic(message.getTopic().toString())
                        .setData(ByteString.copyFrom(message.getPayloadAsBytes())).build(), context);
            }
        }
        return response.build();
    }
    private static MessagingResponse rabbit(MessagingRequest request, ConnectorConfig.Profile profile, CommandContext context) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(profile.required("host")); factory.setPort(profile.number("port", profile.flag("ssl", false) ? 5671 : 5672));
        factory.setUsername(profile.required("username")); factory.setPassword(profile.required("password"));
        factory.setVirtualHost(profile.text("virtualHost", "/")); factory.setAutomaticRecoveryEnabled(false);
        factory.setConnectionTimeout((int) Math.min(context.remainingMillis(), 15_000));
        factory.setHandshakeTimeout((int) Math.min(context.remainingMillis(), 15_000));
        factory.setChannelRpcTimeout((int) Math.min(context.remainingMillis(), Integer.MAX_VALUE));
        factory.setShutdownTimeout(1000);
        if (profile.flag("ssl", false)) { factory.useSslProtocol(); factory.enableHostnameVerification(); }
        var connection = context.track(factory.newConnection("runner-" + context.commandId()));
        var channel = context.track(connection.createChannel());
        var response = MessagingResponse.newBuilder();
        if (request.getOperation().equals("publish")) {
            channel.confirmSelect(); AtomicBoolean returned = new AtomicBoolean();
            channel.addReturnListener((code, text, exchange, route, props, body) -> returned.set(true));
            Map<String, Object> headers = new LinkedHashMap<>(request.getHeadersMap());
            var properties = new AMQP.BasicProperties.Builder().messageId(context.commandId()).deliveryMode(2).headers(headers).build();
            channel.basicPublish(request.getExchange(), request.getRoutingKey().isBlank() ? request.getTopic() : request.getRoutingKey(), true, properties, request.getData().toByteArray());
            channel.waitForConfirmsOrDie(context.remainingMillis());
            if (returned.get()) throw new IllegalArgumentException("RabbitMQ message could not be routed");
            response.setCount(1);
        } else {
            if (request.getTopic().isBlank()) throw new IllegalArgumentException("RabbitMQ queue is required");
            long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMillis(request, context));
            while (response.getMessagesCount() < count(request) && System.nanoTime() < end) {
                context.remainingMillis();
                GetResponse message = channel.basicGet(request.getTopic(), false);
                if (message == null) { Thread.sleep(25); continue; }
                var item = MessagingItem.newBuilder().setTopic(request.getTopic()).setData(ByteString.copyFrom(message.getBody()))
                        .setMessageId(Objects.toString(message.getProps().getMessageId(), ""));
                if (message.getProps().getHeaders() != null) message.getProps().getHeaders().forEach((key, value) -> item.putHeaders(key, Objects.toString(value)));
                append(response, item.build(), context);
                if (request.getAcknowledge()) channel.basicAck(message.getEnvelope().getDeliveryTag(), false);
                // Unacknowledged deliveries are requeued when the channel closes.
            }
        }
        return response.build();
    }
    private static MessagingResponse kafka(MessagingRequest request, ConnectorConfig.Profile profile, CommandContext context) throws Exception {
        if (request.getTopic().isBlank()) throw new IllegalArgumentException("Kafka topic is required");
        Properties properties = new Properties(); properties.putAll(profile.strings("properties"));
        properties.setProperty("bootstrap.servers", profile.required("bootstrapServers"));
        properties.setProperty("client.id", "runner-" + context.commandId());
        properties.setProperty("default.api.timeout.ms", Long.toString(context.remainingMillis()));
        properties.setProperty("request.timeout.ms", Long.toString(Math.min(30_000, context.remainingMillis())));
        var response = MessagingResponse.newBuilder();
        if (request.getOperation().equals("publish")) {
            properties.setProperty("key.serializer", StringSerializer.class.getName());
            properties.setProperty("value.serializer", ByteArraySerializer.class.getName());
            properties.setProperty("acks", "all"); properties.setProperty("enable.idempotence", "true");
            properties.setProperty("max.block.ms", Long.toString(context.remainingMillis()));
            KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);
            context.track(() -> producer.close(Duration.ofSeconds(1)));
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(request.getTopic(), request.getKey().isEmpty() ? null : request.getKey(), request.getData().toByteArray());
            request.getHeadersMap().forEach((key, value) -> record.headers().add(key, value.getBytes(StandardCharsets.UTF_8)));
            RecordMetadata result = producer.send(record).get(context.remainingMillis(), TimeUnit.MILLISECONDS);
            response.setCount(1).addMessages(MessagingItem.newBuilder().setTopic(result.topic()).setMessageId(result.partition() + ":" + result.offset()));
        } else {
            properties.setProperty("key.deserializer", StringDeserializer.class.getName());
            properties.setProperty("value.deserializer", ByteArrayDeserializer.class.getName());
            properties.setProperty("group.id", profile.required("groupId")); properties.setProperty("enable.auto.commit", "false");
            properties.setProperty("auto.offset.reset", profile.text("autoOffsetReset", "earliest"));
            properties.setProperty("max.poll.records", Integer.toString(count(request)));
            KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);
            // wakeup() is safe from the cancellation thread; the consumer itself is owned by this worker.
            AutoCloseable wakeup = consumer::wakeup; context.track(wakeup);
            Map<org.apache.kafka.common.TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            try {
                consumer.subscribe(List.of(request.getTopic()));
                long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMillis(request, context));
                while (response.getMessagesCount() < count(request) && System.nanoTime() < end) {
                    context.remainingMillis();
                    var records = consumer.poll(Duration.ofMillis(Math.max(1, Math.min(200, TimeUnit.NANOSECONDS.toMillis(end - System.nanoTime())))));
                    for (var record : records) {
                        if (response.getMessagesCount() >= count(request)) break;
                        var item = MessagingItem.newBuilder().setTopic(record.topic()).setKey(Objects.toString(record.key(), ""))
                                .setData(ByteString.copyFrom(record.value() == null ? new byte[0] : record.value())).setMessageId(record.partition() + ":" + record.offset());
                        record.headers().forEach(header -> item.putHeaders(header.key(), header.value() == null ? "" : new String(header.value(), StandardCharsets.UTF_8)));
                        append(response, item.build(), context);
                        offsets.put(new org.apache.kafka.common.TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                    }
                }
                if (request.getAcknowledge() && !offsets.isEmpty()) consumer.commitSync(offsets, Duration.ofMillis(context.remainingMillis()));
            } finally { consumer.close(Duration.ofSeconds(1)); }
        }
        return response.build();
    }
}
