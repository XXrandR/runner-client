package com.maximus.runner.infrastructure.connectors;

import com.google.protobuf.ByteString;
import com.maximus.runner.*;
import com.maximus.runner.application.command.*;
import com.maximus.runner.configuration.connectors.ConnectorConfig;
import java.io.InputStream;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/** HTTP carries JSON, XML and raw SOAP envelopes without fixing an application format. */
public final class HttpCommandExecutor implements CommandExecutor {
    private final ConnectorConfig config;
    public HttpCommandExecutor(ConnectorConfig config) { this.config = config; }
    @Override public CommandResult.Builder execute(Command command, CommandContext context) throws Exception {
        ApiRequest request = command.getApi();
        String url = request.getUrl();
        Map<String, String> headers = new LinkedHashMap<>();
        if (!request.getConnectionId().isBlank()) {
            var profile = config.connection(request.getConnectionId(), "http");
            URI base = URI.create(profile.required("baseUrl"));
            URI resolved = base.resolve(url);
            if (!sameOrigin(base, resolved)) throw new IllegalArgumentException("URL does not belong to the configured HTTP connection");
            url = resolved.toString(); headers.putAll(profile.strings("headers"));
        }
        URI uri = URI.create(url);
        if (!Set.of("http", "https").contains(uri.getScheme()) || uri.getHost() == null || uri.getUserInfo() != null || uri.getFragment() != null)
            throw new IllegalArgumentException("A valid HTTP(S) URL without embedded credentials is required");
        if (request.hasTls() && (!request.getTls().getVerifyCertificate() || !request.getTls().getVerifyHostname()
                || !request.getTls().getClientCertificateId().isBlank()))
            throw new IllegalArgumentException("This HTTP executor requires certificate verification; custom client certificates need a dedicated connector");
        StringBuilder target = new StringBuilder(uri.toString());
        for (KeyValue value : request.getQueryList()) {
            target.append(target.indexOf("?") >= 0 ? '&' : '?').append(URLEncoder.encode(value.getKey(), StandardCharsets.UTF_8))
                    .append('=').append(URLEncoder.encode(value.getValue(), StandardCharsets.UTF_8));
        }
        headers.putAll(request.getHeadersMap());
        if (!request.getContentType().isBlank()) headers.putIfAbsent("Content-Type", request.getContentType());
        if (request.getFollowRedirects() && headers.keySet().stream().anyMatch(h -> h.equalsIgnoreCase("Authorization") || h.equalsIgnoreCase("Cookie")))
            throw new IllegalArgumentException("Credential-bearing HTTP requests cannot automatically follow redirects");
        long timeout = context.remainingMillis();
        if (request.hasTimeout()) {
            timeout = ConnectorTimeouts.within(request.getTimeout(), context);
        }
        HttpClient client = context.track(HttpClient.newBuilder().connectTimeout(Duration.ofMillis(timeout))
                .followRedirects(request.getFollowRedirects() ? HttpClient.Redirect.NORMAL : HttpClient.Redirect.NEVER).build());
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(target.toString())).timeout(Duration.ofMillis(timeout));
        headers.forEach(builder::header);
        String method = request.getMethod().isBlank() ? "GET" : request.getMethod().toUpperCase(Locale.ROOT);
        if (!Set.of("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS").contains(method))
            throw new IllegalArgumentException("Unsupported HTTP method");
        builder.method(method, HttpRequest.BodyPublishers.ofByteArray(request.getBody().toByteArray()));
        var response = client.send(builder.build(), HttpResponse.BodyHandlers.ofInputStream());
        ApiResponse.Builder result = ApiResponse.newBuilder().setStatusCode(response.statusCode())
                .setContentType(response.headers().firstValue("content-type").orElse(""));
        response.headers().map().forEach((key, values) -> result.putHeaders(key, String.join(", ", values)));
        if (command.getStreaming() || request.getStreamResponse()) {
            long max = request.getMaxResponseBytes() == 0 ? 16_777_216 : request.getMaxResponseBytes();
            if (max < 1 || max > 67_108_864) throw new IllegalArgumentException("HTTP stream limit must be between 1 byte and 64MiB");
            long total = 0;
            try (InputStream body = context.track(response.body())) {
                byte[] bytes;
                while ((bytes = body.readNBytes(Math.min(65_536, BoundedStreams.limit(0, context)))).length > 0) {
                    context.remainingMillis(); total += bytes.length;
                    if (total > max) throw new IllegalArgumentException("HTTP stream exceeds max_response_bytes");
                    context.progress(CommandResultStatus.STREAMING, CommandResult.newBuilder()
                            .setApi(result.clone().setBody(ByteString.copyFrom(bytes)).setBodySize(total).setHasBody(true))
                            .putMetadata("stream_offset", Long.toString(total - bytes.length)));
                }
            }
            return CommandResult.newBuilder().setApi(result.setBodySize(total).setHasBody(total > 0));
        }
        byte[] bytes;
        try (InputStream body = context.track(response.body())) {
            bytes = BoundedStreams.read(body, BoundedStreams.limit(request.getMaxResponseBytes(), context), context);
        }
        result.setBody(ByteString.copyFrom(bytes)).setBodySize(bytes.length).setHasBody(bytes.length > 0);
        return CommandResult.newBuilder().setApi(result);
    }
    private static boolean sameOrigin(URI a, URI b) {
        return Objects.equals(a.getScheme(), b.getScheme()) && Objects.equals(a.getHost(), b.getHost()) && port(a) == port(b);
    }
    private static int port(URI uri) { return uri.getPort() >= 0 ? uri.getPort() : "https".equals(uri.getScheme()) ? 443 : 80; }
}
