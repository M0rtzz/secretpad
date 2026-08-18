/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.config;

import org.secretflow.secretpad.web.service.DataSandboxMvpService;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.PingMessage;
import org.springframework.web.socket.PongMessage;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.SubProtocolCapable;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;
import org.springframework.web.socket.server.support.WebSocketHandlerMapping;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Standard WebSocket bridge for Jupyter kernels and terminals. */
@Slf4j
@Configuration
@EnableWebSocket
public class SandboxWebSocketConfig implements WebSocketConfigurer {

    private static final String PROXY_PREFIX = "/api/v1alpha1/data-sandbox/proxy/";
    private static final String ATTR_UPSTREAM = "sandbox.ws.upstream";
    private static final String ATTR_HOST = "sandbox.ws.host";
    private static final String ATTR_COOKIE = "sandbox.ws.cookie";

    static {
        System.setProperty("jdk.httpclient.allowRestrictedHeaders", "host");
    }

    private final DataSandboxMvpService service;
    private final String gateway;
    private final String websocketGateway;

    public SandboxWebSocketConfig(
            DataSandboxMvpService service,
            @Value("${secretpad.gateway:127.0.0.1:80}") String gateway,
            @Value("${secretpad.data-sandbox.websocket-gateway:}") String websocketGateway) {
        this.service = service;
        this.gateway = gateway;
        this.websocketGateway = websocketGateway == null || websocketGateway.isBlank()
                ? gateway
                : websocketGateway;
    }

    /** WebSocket paths must win over the controller's generic /proxy/{id}/** mapping. */
    @Bean
    public static BeanPostProcessor sandboxWebSocketHandlerMappingOrder() {
        return new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(Object bean, String beanName) {
                if (bean instanceof WebSocketHandlerMapping mapping) {
                    mapping.setOrder(-1);
                }
                return bean;
            }
        };
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        SandboxBridgeHandler handler = new SandboxBridgeHandler();
        SandboxHandshakeInterceptor interceptor = new SandboxHandshakeInterceptor();
        registry.addHandler(
                        handler,
                        PROXY_PREFIX + "*/api/kernels/*/channels",
                        PROXY_PREFIX + "*/terminals/websocket/*")
                .addInterceptors(interceptor)
                .setAllowedOriginPatterns("*");
    }

    private final class SandboxHandshakeInterceptor implements HandshakeInterceptor {

        @Override
        public boolean beforeHandshake(
                ServerHttpRequest request,
                ServerHttpResponse response,
                org.springframework.web.socket.WebSocketHandler wsHandler,
                Map<String, Object> attributes) {
            URI uri = request.getURI();
            String sandboxId = sandboxId(uri.getPath());
            String token = queryParameter(uri.getRawQuery(), "token");
            if (token == null || token.isBlank()) {
                token = cookie(request.getHeaders().getFirst(HttpHeaders.COOKIE), "Data-Sandbox-Token");
            }
            service.validateDevToken(sandboxId, token);

            String endpoint = service.proxyTarget(sandboxId);
            String route = routeAuthority(websocketGateway);
            String scheme = websocketGateway.startsWith("https://") ? "wss" : "ws";
            String query = withoutToken(uri.getRawQuery());
            attributes.put(ATTR_UPSTREAM, scheme + "://" + route + uri.getRawPath()
                    + (query.isBlank() ? "" : "?" + query));
            attributes.put(ATTR_HOST, endpoint);
            attributes.put(ATTR_COOKIE, jupyterCookies(request.getHeaders().getFirst(HttpHeaders.COOKIE)));
            return true;
        }

        @Override
        public void afterHandshake(
                ServerHttpRequest request,
                ServerHttpResponse response,
                org.springframework.web.socket.WebSocketHandler wsHandler,
                Exception exception) {
            // no-op
        }
    }

    private static final class SandboxBridgeHandler extends AbstractWebSocketHandler
            implements SubProtocolCapable {

        private static final String ATTR_CLIENT = "sandbox.ws.client";

        private final HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        @Override
        public void afterConnectionEstablished(WebSocketSession session) {
            String upstream = String.valueOf(session.getAttributes().get(ATTR_UPSTREAM));
            String host = String.valueOf(session.getAttributes().get(ATTR_HOST));
            WebSocket.Builder builder = client.newWebSocketBuilder()
                    .connectTimeout(Duration.ofSeconds(5))
                    .header("Host", host);
            String cookie = (String) session.getAttributes().get(ATTR_COOKIE);
            if (cookie != null && !cookie.isBlank()) {
                builder.header("Cookie", cookie);
            }
            String protocols = session.getAcceptedProtocol();
            if (protocols != null && !protocols.isBlank()) {
                String[] values = Arrays.stream(protocols.split(","))
                        .map(String::trim)
                        .filter(value -> !value.isBlank())
                        .toArray(String[]::new);
                if (values.length > 0) {
                    builder.subprotocols(values[0], Arrays.copyOfRange(values, 1, values.length));
                }
            }
            CompletableFuture<WebSocket> future = builder.buildAsync(
                    URI.create(upstream), new UpstreamListener(session));
            session.getAttributes().put(ATTR_CLIENT, future);
            future.exceptionally(error -> {
                log.warn("Jupyter WebSocket connection to {} failed: {}", host, error.getMessage());
                close(session, CloseStatus.SERVER_ERROR);
                return null;
            });
        }

        @Override
        public java.util.List<String> getSubProtocols() {
            return java.util.List.of("v1.kernel.websocket.jupyter.org");
        }

        @Override
        protected void handleTextMessage(WebSocketSession session, TextMessage message) {
            client(session).thenAccept(ws -> ws.sendText(message.getPayload(), message.isLast()));
        }

        @Override
        protected void handleBinaryMessage(WebSocketSession session, BinaryMessage message) {
            client(session).thenAccept(ws -> ws.sendBinary(message.getPayload(), message.isLast()));
        }

        @Override
        protected void handlePongMessage(WebSocketSession session, PongMessage message) {
            client(session).thenAccept(ws -> ws.sendPong(message.getPayload()));
        }

        @Override
        public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
            client(session).thenAccept(ws -> ws.sendClose(status.getCode(), status.getReason()));
        }

        @SuppressWarnings("unchecked")
        private CompletableFuture<WebSocket> client(WebSocketSession session) {
            return (CompletableFuture<WebSocket>) session.getAttributes().get(ATTR_CLIENT);
        }
    }

    private static final class UpstreamListener implements WebSocket.Listener {

        private final WebSocketSession downstream;
        private final StringBuilder text = new StringBuilder();

        private UpstreamListener(WebSocketSession downstream) {
            this.downstream = downstream;
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            text.append(data);
            if (last) {
                send(downstream, new TextMessage(text.toString()));
                text.setLength(0);
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
            send(downstream, new BinaryMessage(data, last));
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onPing(WebSocket webSocket, ByteBuffer message) {
            send(downstream, new PingMessage(message));
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onPong(WebSocket webSocket, ByteBuffer message) {
            send(downstream, new PongMessage(message));
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            close(downstream, new CloseStatus(statusCode, reason));
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            log.warn("Jupyter WebSocket bridge failed: {}", error.getMessage());
            close(downstream, CloseStatus.SERVER_ERROR);
        }
    }

    private static void send(WebSocketSession session, WebSocketMessage<?> message) {
        try {
            synchronized (session) {
                if (session.isOpen()) {
                    session.sendMessage(message);
                }
            }
        } catch (Exception e) {
            close(session, CloseStatus.SERVER_ERROR);
        }
    }

    private static void close(WebSocketSession session, CloseStatus status) {
        try {
            if (session.isOpen()) {
                session.close(status);
            }
        } catch (Exception ignored) {
            // already closed
        }
    }

    private static String sandboxId(String path) {
        int start = path.indexOf(PROXY_PREFIX);
        if (start < 0) {
            throw new IllegalArgumentException("Invalid sandbox WebSocket path");
        }
        String remainder = path.substring(start + PROXY_PREFIX.length());
        int slash = remainder.indexOf('/');
        return slash < 0 ? remainder : remainder.substring(0, slash);
    }

    private static String queryParameter(String query, String expected) {
        if (query == null) {
            return null;
        }
        for (String item : query.split("&")) {
            int equals = item.indexOf('=');
            String name = equals < 0 ? item : item.substring(0, equals);
            if (expected.equals(name)) {
                return equals < 0 ? "" : item.substring(equals + 1);
            }
        }
        return null;
    }

    private static String withoutToken(String query) {
        if (query == null || query.isBlank()) {
            return "";
        }
        return Arrays.stream(query.split("&"))
                .filter(item -> !item.startsWith("token="))
                .reduce((left, right) -> left + "&" + right)
                .orElse("");
    }

    private static String cookie(String header, String expected) {
        if (header == null) {
            return null;
        }
        for (String item : header.split(";")) {
            String value = item.trim();
            int equals = value.indexOf('=');
            if (equals > 0 && expected.equals(value.substring(0, equals).trim())) {
                return value.substring(equals + 1);
            }
        }
        return null;
    }

    private static String jupyterCookies(String header) {
        if (header == null || header.isBlank()) {
            return "";
        }
        return Arrays.stream(header.split(";"))
                .map(String::trim)
                .filter(item -> item.startsWith("_xsrf=") || item.startsWith("username-"))
                .reduce((left, right) -> left + "; " + right)
                .orElse("");
    }

    private static String routeAuthority(String route) {
        if (route.startsWith("http://") || route.startsWith("https://")) {
            return URI.create(route).getAuthority();
        }
        return route;
    }
}
