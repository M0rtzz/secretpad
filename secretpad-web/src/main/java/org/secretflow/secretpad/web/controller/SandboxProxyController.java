/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.controller;

import org.secretflow.secretpad.web.service.DataSandboxMvpService;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpUpgradeHandler;
import jakarta.servlet.http.WebConnection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 开发端点跳板：把沙箱的 Kuscia 开发端点（DB endpoint 列，如 10.x.x.x:31234）
 * 转发到 SecretPad 同域路径，鉴权全部收敛在本层（一次性 token 由
 * {@link org.secretflow.secretpad.web.interceptor.LoginInterceptor} 强制校验）。
 *
 * <p>安全约束：
 * <ul>
 *   <li>目标地址仅允许来自 DB endpoint 列（防 SSRF，不接受任何用户输入主机/端口）；</li>
 *   <li>转发头/参数白名单：剥离 token、Cookie、User-Token、Authorization 等凭证；</li>
 *   <li>WebSocket 经 Servlet 3.1 upgrade 后做字节级双向管道（无需 WebSocket 依赖）。</li>
 * </ul>
 * </p>
 */
@Slf4j
@RestController
@RequestMapping("/api/v1alpha1/data-sandbox/proxy")
public class SandboxProxyController {

    private static final Set<String> REQUEST_HEADER_ALLOW = Set.of(
            "accept", "accept-language", "content-type", "content-encoding", "origin",
            "sec-websocket-key", "sec-websocket-version", "sec-websocket-protocol", "sec-websocket-extensions");
    private static final Set<String> RESPONSE_HEADER_ALLOW = Set.of(
            "content-type", "content-disposition", "cache-control", "expires", "etag",
            "last-modified", "set-cookie", "content-encoding", "www-authenticate");
    private static final String WS_MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    static {
        // Kuscia 集群端跳板需覆盖 Host 头（envoy 按 Host 头路由到沙箱容器）；
        // JDK HttpClient 默认禁止设置 host，须在首个请求构建前放行（Spring 启动时已设置）。
        System.setProperty("jdk.httpclient.allowRestrictedHeaders", "host");
    }

    private final DataSandboxMvpService service;
    private final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @Value("${secretpad.data-sandbox.dev-endpoint.proxy-timeout-seconds:30}")
    private int proxyTimeoutSeconds;

    public SandboxProxyController(DataSandboxMvpService service) {
        this.service = service;
    }

    /**
     * 跳板入口。token 已在拦截器校验，这里只取 DB endpoint 并转发。
     * 匹配 /proxy/{sandboxId} 与 /proxy/{sandboxId}/** 两种路径。
     */
    @GetMapping({"/{sandboxId}", "/{sandboxId}/**"})
    public void proxy(@PathVariable String sandboxId, HttpServletRequest request, HttpServletResponse response) throws IOException, jakarta.servlet.ServletException {
        DataSandboxMvpService.DevEndpointTarget target = service.proxyTarget(sandboxId);
        if (isWebSocketUpgrade(request)) {
            proxyWebSocket(sandboxId, target, request);
        } else {
            proxyHttp(target, request, response);
        }
    }

    /* ------------------------------- HTTP streaming ------------------------------- */

    private void proxyHttp(DataSandboxMvpService.DevEndpointTarget target, HttpServletRequest request, HttpServletResponse response) throws IOException {
        try {
            String targetUrl = buildTargetUrl(target, request, false);
            String method = request.getMethod().toUpperCase(Locale.ROOT);
            boolean hasBody = !Set.of("GET", "HEAD", "DELETE", "OPTIONS", "TRACE").contains(method);
            // 请求头白名单（凭证类一律不转发）；先收集头再一次性 build，避免丢失 body
            var names = request.getHeaderNames();
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(targetUrl))
                    .timeout(Duration.ofSeconds(Math.max(1, proxyTimeoutSeconds)))
                    .header("user-agent", "SecretPad-DevProxy")
                    // Kuscia envoy 按 Host 头路由到具体沙箱（Virtual Host），而非按连接地址
                    .header("host", target.virtualHost());
            while (names != null && names.hasMoreElements()) {
                String name = names.nextElement().toLowerCase(Locale.ROOT);
                if (REQUEST_HEADER_ALLOW.contains(name)) {
                    var values = request.getHeaders(name);
                    while (values.hasMoreElements()) {
                        builder.header(name, values.nextElement());
                    }
                }
            }
            HttpRequest outgoing = hasBody
                    ? builder.method(method, HttpRequest.BodyPublishers.ofInputStream(() -> {
                        try {
                            return request.getInputStream();
                        } catch (IOException e) {
                            throw new java.io.UncheckedIOException(e);
                        }
                    })).build()
                    : builder.method(method, HttpRequest.BodyPublishers.noBody()).build();
            HttpResponse<InputStream> upstream = httpClient.send(outgoing, HttpResponse.BodyHandlers.ofInputStream());
            response.setStatus(upstream.statusCode());
            upstream.headers().map().forEach((name, values) -> {
                String lower = name.toLowerCase(Locale.ROOT);
                if (RESPONSE_HEADER_ALLOW.contains(lower)) {
                    for (String value : values) {
                        response.addHeader(name, value);
                    }
                }
            });
            try (InputStream in = upstream.body(); OutputStream out = response.getOutputStream()) {
                in.transferTo(out);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("dev proxy interrupted: {}", e.getMessage());
        } catch (Exception e) {
            // 目标不可达（沙箱容器重启等）：返回 502，浏览器展示明确错误
            if (!response.isCommitted()) {
                response.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
                response.setContentType("text/plain;charset=UTF-8");
                response.getWriter().write("开发环境暂时不可达，请确认沙箱仍在运行后重试");
            }
            log.warn("dev proxy to {} failed: {}", target.virtualHost(), e.getMessage());
        }
    }

    /* ------------------------------- WebSocket tunnel ------------------------------- */

    private boolean isWebSocketUpgrade(HttpServletRequest request) {
        String upgrade = request.getHeader("Upgrade");
        return upgrade != null && "websocket".equalsIgnoreCase(upgrade.trim());
    }

    private void proxyWebSocket(String sandboxId, DataSandboxMvpService.DevEndpointTarget target, HttpServletRequest request) throws IOException, jakarta.servlet.ServletException {
        Map<String, String> handshake = new HashMap<>();
        handshake.put("connectHost", target.connectHost());
        handshake.put("connectPort", Integer.toString(target.connectPort()));
        handshake.put("virtualHost", target.virtualHost());
        handshake.put("path", buildTargetUrl(target, request, true));
        for (String header : new String[]{"Sec-WebSocket-Key", "Sec-WebSocket-Version", "Sec-WebSocket-Protocol", "Sec-WebSocket-Extensions", "Origin"}) {
            String value = request.getHeader(header);
            if (value != null) {
                handshake.put(header, value);
            }
        }
        WsTunnelUpgradeHandler.TARGET.set(handshake);
        // Servlet 3.1 upgrade：容器在请求完成后调用 handler.init(WebConnection)，
        // 连接从 HTTP 解析剥离，后续字节（WS 帧）经 WebConnection 流双向透传
        request.upgrade(WsTunnelUpgradeHandler.class);
    }

    /**
     * 拼接目标 URL：/proxy/{sandboxId} 之后的部分 + 查询串（token 参数剥除）。
     * ws=true 时仅返回原始路径（不含 scheme/host），供 WS 握手重建请求行。
     */
    private String buildTargetUrl(DataSandboxMvpService.DevEndpointTarget target, HttpServletRequest request, boolean rawPathOnly) {
        String uri = request.getRequestURI();
        String prefix = request.getContextPath() + "/api/v1alpha1/data-sandbox/proxy/";
        String sub = uri.startsWith(prefix) ? uri.substring(prefix.length()) : "";
        int slash = sub.indexOf('/');
        String path = (slash >= 0 ? sub.substring(slash) : "");
        if (path.isEmpty()) {
            path = "/";
        }
        String query = "";
        if (request.getQueryString() != null) {
            String[] params = request.getQueryString().split("&");
            StringBuilder kept = new StringBuilder();
            for (String param : params) {
                if (param.startsWith("token=")) {
                    continue; // 跳板凭证不进入容器内应用
                }
                if (kept.length() > 0) {
                    kept.append('&');
                }
                kept.append(param);
            }
            if (kept.length() > 0) {
                query = "?" + kept;
            }
        }
        if (rawPathOnly) {
            return path + query;
        }
        return "http://" + target.connectHost() + ":" + target.connectPort() + path + query;
    }

    /**
     * WebSocket 字节级隧道：client 与目标容器 socket 之间双向拷贝。
     * 目标握手请求由本类重建（剥除凭证头），101 响应由本类计算 Sec-WebSocket-Accept 手写返回。
     */
    public static class WsTunnelUpgradeHandler implements HttpUpgradeHandler {

        static final ThreadLocal<Map<String, String>> TARGET = new ThreadLocal<>();

        private Socket socket;

        @Override
        public void init(WebConnection wc) {
            Map<String, String> target = TARGET.get();
            TARGET.remove();
            if (target == null) {
                closeQuietly(wc);
                return;
            }
            String connectHost = target.get("connectHost");
            int connectPort = Integer.parseInt(target.get("connectPort"));
            String virtualHost = target.get("virtualHost");
            String path = target.get("path");
            String key = target.get("Sec-WebSocket-Key");
            try {
                socket = new Socket(connectHost, connectPort);
                socket.setSoTimeout(0);
                OutputStream toTarget = socket.getOutputStream();
                StringBuilder handshake = new StringBuilder("GET ").append(path).append(" HTTP/1.1\r\n")
                        .append("Host: ").append(virtualHost).append("\r\n")
                        .append("Upgrade: websocket\r\n")
                        .append("Connection: Upgrade\r\n");
                if (key == null) {
                    key = Base64.getEncoder().encodeToString(new byte[16]);
                }
                handshake.append("Sec-WebSocket-Key: ").append(key).append("\r\n");
                for (String header : new String[]{"Sec-WebSocket-Version", "Sec-WebSocket-Protocol", "Sec-WebSocket-Extensions", "Origin"}) {
                    String value = target.get(header);
                    if (value != null) {
                        handshake.append(header).append(": ").append(value).append("\r\n");
                    }
                }
                handshake.append("\r\n");
                toTarget.write(handshake.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
                toTarget.flush();
                // 读取目标 101 响应头并丢弃（客户端侧响应由本类构造）
                readHeaders(socket.getInputStream());
                // 向客户端手写 101 + Sec-WebSocket-Accept
                String accept = Base64.getEncoder().encodeToString(
                        MessageDigest.getInstance("SHA-1").digest((key + WS_MAGIC).getBytes(java.nio.charset.StandardCharsets.UTF_8)));
                OutputStream toClient = wc.getOutputStream();
                toClient.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "
                        + accept + "\r\n\r\n").getBytes(java.nio.charset.StandardCharsets.UTF_8));
                toClient.flush();
                // 双向管道：任一方向结束即关闭整个隧道
                Thread clientToTarget = pipe(wc.getInputStream(), toTarget, wc, "client->target");
                Thread targetToClient = pipe(socket.getInputStream(), toClient, wc, "target->client");
                clientToTarget.join(0);
                targetToClient.join(0);
            } catch (Exception e) {
                log.warn("ws tunnel to {} failed: {}", virtualHost, e.getMessage());
                closeQuietly(wc);
            }
        }

        private Thread pipe(InputStream in, OutputStream out, WebConnection wc, String name) {
            Thread thread = new Thread(() -> {
                try (in; out) {
                    byte[] buffer = new byte[8192];
                    int read;
                    while ((read = in.read(buffer)) != -1) {
                        out.write(buffer, 0, read);
                        out.flush();
                    }
                } catch (IOException e) {
                    // 任一端关闭即结束该方向
                } finally {
                    closeQuietly(wc);
                }
            }, "sandbox-ws-" + name);
            thread.setDaemon(true);
            thread.start();
            return thread;
        }

        private void readHeaders(InputStream in) throws IOException {
            int state = 0;
            int b;
            while ((b = in.read()) != -1) {
                if (b == '\r') {
                    state = (state == 1) ? 2 : state;
                } else if (b == '\n') {
                    if (state == 2) {
                        return; // \r\n\r\n
                    }
                    state = 1;
                } else {
                    state = 0;
                }
            }
            throw new IOException("target closed before upgrade response");
        }

        @Override
        public void destroy() {
            closeQuietly(socket);
        }

        private void closeQuietly(WebConnection wc) {
            try {
                wc.getInputStream().close();
            } catch (Exception ignored) {
            }
            try {
                wc.getOutputStream().close();
            } catch (Exception ignored) {
            }
            closeQuietly(socket);
        }

        private void closeQuietly(Socket s) {
            if (s != null) {
                try {
                    s.close();
                } catch (IOException ignored) {
                }
            }
        }
    }
}
