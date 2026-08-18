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
import org.apache.http.Header;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.security.MessageDigest;
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
            "x-xsrftoken", "sec-websocket-key", "sec-websocket-version", "sec-websocket-protocol",
            "sec-websocket-extensions");
    private static final Set<String> RESPONSE_HEADER_ALLOW = Set.of(
            "content-type", "content-disposition", "cache-control", "expires", "etag",
            "last-modified", "set-cookie", "content-encoding", "www-authenticate", "location");
    private static final String WS_MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    private final DataSandboxMvpService service;
    private final CloseableHttpClient httpClient = HttpClients.custom()
            .disableRedirectHandling()
            .disableCookieManagement()
            .build();

    @Value("${secretpad.gateway:127.0.0.1:80}")
    private String gateway;

    @Value("${secretpad.data-sandbox.dev-endpoint.proxy-timeout-seconds:30}")
    private int proxyTimeoutSeconds;

    public SandboxProxyController(DataSandboxMvpService service) {
        this.service = service;
    }

    /**
     * 跳板入口。token 已在拦截器校验，这里只取 DB endpoint 并转发。
     * 匹配 /proxy/{sandboxId} 与 /proxy/{sandboxId}/** 两种路径。
     */
    @RequestMapping({"/{sandboxId}", "/{sandboxId}/**"})
    public void proxy(@PathVariable String sandboxId, HttpServletRequest request, HttpServletResponse response) throws IOException, jakarta.servlet.ServletException {
        String endpoint = service.proxyTarget(sandboxId);
        if (isWebSocketUpgrade(request)) {
            proxyWebSocket(sandboxId, endpoint, request, response);
        } else {
            proxyHttp(sandboxId, endpoint, request, response);
        }
    }

    /* ------------------------------- HTTP streaming ------------------------------- */

    private void proxyHttp(String sandboxId, String endpoint, HttpServletRequest request, HttpServletResponse response) throws IOException {
        try {
            String targetUrl = buildTargetUrl(endpoint, request, false);
            String method = request.getMethod().toUpperCase(Locale.ROOT);
            boolean hasBody = !Set.of("GET", "HEAD", "DELETE", "OPTIONS", "TRACE").contains(method);
            // 请求头白名单（凭证类一律不转发）；先收集头再一次性 build，避免丢失 body
            var names = request.getHeaderNames();
            RequestBuilder builder = RequestBuilder.create(method)
                    .setUri(targetUrl)
                    .setConfig(RequestConfig.custom()
                            .setConnectTimeout(5_000)
                            .setConnectionRequestTimeout(5_000)
                            .setSocketTimeout(Math.max(1, proxyTimeoutSeconds) * 1_000)
                            .build())
                    .setHeader("user-agent", "SecretPad-DevProxy");
            if (isClusterService(endpoint)) {
                builder.setHeader("Host", endpoint);
            }
            while (names != null && names.hasMoreElements()) {
                String name = names.nextElement().toLowerCase(Locale.ROOT);
                if (REQUEST_HEADER_ALLOW.contains(name)) {
                    var values = request.getHeaders(name);
                    while (values.hasMoreElements()) {
                        builder.addHeader(name, values.nextElement());
                    }
                }
            }
            String jupyterCookies = jupyterCookies(request.getHeader("Cookie"));
            if (!jupyterCookies.isBlank()) {
                builder.setHeader("Cookie", jupyterCookies);
            }
            if (hasBody) {
                builder.setEntity(new InputStreamEntity(request.getInputStream(), request.getContentLengthLong()));
            }
            HttpUriRequest outgoing = builder.build();
            try (CloseableHttpResponse upstream = httpClient.execute(outgoing)) {
                response.setStatus(upstream.getStatusLine().getStatusCode());
                for (Header header : upstream.getAllHeaders()) {
                    String lower = header.getName().toLowerCase(Locale.ROOT);
                    if (RESPONSE_HEADER_ALLOW.contains(lower)) {
                        String value = "location".equals(lower)
                                ? rewriteLocation(sandboxId, request, header.getValue())
                                : header.getValue();
                        response.addHeader(header.getName(), value);
                    }
                }
                if (upstream.getEntity() != null) {
                    try (InputStream in = upstream.getEntity().getContent(); OutputStream out = response.getOutputStream()) {
                        in.transferTo(out);
                    }
                }
            }
        } catch (Exception e) {
            // 目标不可达（沙箱容器重启等）：返回 502，浏览器展示明确错误
            if (!response.isCommitted()) {
                response.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
                response.setContentType("text/plain;charset=UTF-8");
                response.getWriter().write("开发环境暂时不可达，请确认沙箱仍在运行后重试");
            }
            log.warn("dev proxy to {} failed: {}", endpoint, e.getMessage());
        }
    }

    /* ------------------------------- WebSocket tunnel ------------------------------- */

    private boolean isWebSocketUpgrade(HttpServletRequest request) {
        String upgrade = request.getHeader("Upgrade");
        return upgrade != null && "websocket".equalsIgnoreCase(upgrade.trim());
    }

    private void proxyWebSocket(String sandboxId, String endpoint, HttpServletRequest request,
                                HttpServletResponse response) throws IOException, jakarta.servlet.ServletException {
        Map<String, String> handshake = new HashMap<>();
        handshake.put("endpoint", endpoint);
        handshake.put("route", isClusterService(endpoint) ? routeAuthority(gateway) : endpoint);
        handshake.put("path", buildTargetUrl(endpoint, request, true));
        for (String header : new String[]{"Sec-WebSocket-Key", "Sec-WebSocket-Version", "Sec-WebSocket-Protocol", "Sec-WebSocket-Extensions", "Origin"}) {
            String value = request.getHeader(header);
            if (value != null) {
                handshake.put(header, value);
            }
        }
        String jupyterCookies = jupyterCookies(request.getHeader("Cookie"));
        if (!jupyterCookies.isBlank()) {
            handshake.put("Cookie", jupyterCookies);
        }
        WsTunnelUpgradeHandler.TARGET.set(handshake);
        String key = handshake.get("Sec-WebSocket-Key");
        if (key == null || key.isBlank()) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Missing Sec-WebSocket-Key");
            WsTunnelUpgradeHandler.TARGET.remove();
            return;
        }
        response.setStatus(HttpServletResponse.SC_SWITCHING_PROTOCOLS);
        response.setHeader("Upgrade", "websocket");
        response.setHeader("Connection", "Upgrade");
        response.setHeader("Sec-WebSocket-Accept", websocketAccept(key));
        String protocol = handshake.get("Sec-WebSocket-Protocol");
        if (protocol != null && !protocol.isBlank()) {
            response.setHeader("Sec-WebSocket-Protocol", protocol.split(",")[0].trim());
        }
        // Servlet 3.1 upgrade：容器在请求完成后调用 handler.init(WebConnection)，
        // 连接从 HTTP 解析剥离，后续字节（WS 帧）经 WebConnection 流双向透传
        request.upgrade(WsTunnelUpgradeHandler.class);
    }

    private static String websocketAccept(String key) {
        try {
            return Base64.getEncoder().encodeToString(
                    MessageDigest.getInstance("SHA-1")
                            .digest((key + WS_MAGIC).getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-1 is unavailable", e);
        }
    }

    /**
     * 拼接目标 URL：/proxy/{sandboxId} 之后的部分 + 查询串（token 参数剥除）。
     * ws=true 时仅返回原始路径（不含 scheme/host），供 WS 握手重建请求行。
     */
    private String buildTargetUrl(String endpoint, HttpServletRequest request, boolean rawPathOnly) {
        String uri = request.getRequestURI();
        String prefix = request.getContextPath() + "/api/v1alpha1/data-sandbox/proxy/";
        String sub = uri.startsWith(prefix) ? uri.substring(prefix.length()) : "";
        int slash = sub.indexOf('/');
        String path = isClusterService(endpoint)
                ? request.getRequestURI().substring(request.getContextPath().length())
                : (slash >= 0 ? sub.substring(slash) : "");
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
        String route = isClusterService(endpoint) ? gateway : endpoint;
        if (route.startsWith("http://") || route.startsWith("https://")) {
            return route + path + query;
        }
        return "http://" + route + path + query;
    }

    private boolean isClusterService(String endpoint) {
        return endpoint != null && (endpoint.endsWith(".svc") || endpoint.contains(".svc:"));
    }

    private String rewriteLocation(String sandboxId, HttpServletRequest request, String location) {
        String pathAndQuery = location;
        if (location.startsWith("http://") || location.startsWith("https://")) {
            URI uri = URI.create(location);
            pathAndQuery = uri.getRawPath();
            if (uri.getRawQuery() != null && !uri.getRawQuery().isBlank()) {
                pathAndQuery += "?" + uri.getRawQuery();
            }
        }
        if (!pathAndQuery.startsWith("/")) {
            pathAndQuery = "/" + pathAndQuery;
        }
        String proxyPrefix = request.getContextPath() + "/api/v1alpha1/data-sandbox/proxy/" + sandboxId;
        String token = request.getParameter("token");
        String separator = pathAndQuery.contains("?")
                ? (pathAndQuery.endsWith("?") || pathAndQuery.endsWith("&") ? "" : "&")
                : "?";
        String rewritten = pathAndQuery.startsWith(proxyPrefix) ? pathAndQuery : proxyPrefix + pathAndQuery;
        return rewritten + separator + "token=" + token;
    }

    private String routeAuthority(String route) {
        if (route.startsWith("http://") || route.startsWith("https://")) {
            return URI.create(route).getAuthority();
        }
        return route;
    }

    /**
     * WebSocket 字节级隧道：client 与目标容器 socket 之间双向拷贝。
     * 目标握手请求由本类重建（剥除凭证头），101 响应由本类计算 Sec-WebSocket-Accept 手写返回。
     */
    public static class WsTunnelUpgradeHandler implements HttpUpgradeHandler {

        static final ThreadLocal<Map<String, String>> TARGET = new ThreadLocal<>();

        private final Map<String, String> target;
        private Socket socket;

        public WsTunnelUpgradeHandler() {
            // Tomcat constructs the handler synchronously in request.upgrade(), but invokes init()
            // after request processing and may use another thread. Capture the request-scoped target
            // now instead of reading the ThreadLocal later.
            target = TARGET.get();
            TARGET.remove();
        }

        @Override
        public void init(WebConnection wc) {
            if (target == null) {
                log.warn("ws tunnel target was unavailable during upgrade");
                closeQuietly(wc);
                return;
            }
            String endpoint = target.get("endpoint");
            String path = target.get("path");
            String key = target.get("Sec-WebSocket-Key");
            try {
                String route = target.getOrDefault("route", endpoint);
                socket = new Socket(route.substring(0, route.lastIndexOf(':')), Integer.parseInt(route.substring(route.lastIndexOf(':') + 1)));
                socket.setSoTimeout(0);
                OutputStream toTarget = socket.getOutputStream();
                StringBuilder handshake = new StringBuilder("GET ").append(path).append(" HTTP/1.1\r\n")
                        .append("Host: ").append(endpoint).append("\r\n")
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
                String cookies = target.get("Cookie");
                if (cookies != null && !cookies.isBlank()) {
                    handshake.append("Cookie: ").append(cookies).append("\r\n");
                }
                handshake.append("\r\n");
                toTarget.write(handshake.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
                toTarget.flush();
                // 读取目标 101 响应头并丢弃（客户端侧响应由本类构造）
                readHeaders(socket.getInputStream());
                OutputStream toClient = wc.getOutputStream();
                // 双向管道：任一方向结束即关闭整个隧道
                Thread clientToTarget = pipe(wc.getInputStream(), toTarget, wc, "client->target");
                Thread targetToClient = pipe(socket.getInputStream(), toClient, wc, "target->client");
                clientToTarget.join(0);
                targetToClient.join(0);
            } catch (Exception e) {
                log.warn("ws tunnel to {} failed: {}", endpoint, e.getMessage());
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

    /** Only Jupyter's path-scoped browser state may enter the sandbox. */
    private static String jupyterCookies(String cookieHeader) {
        if (cookieHeader == null || cookieHeader.isBlank()) {
            return "";
        }
        StringBuilder allowed = new StringBuilder();
        for (String item : cookieHeader.split(";")) {
            String cookie = item.trim();
            int equals = cookie.indexOf('=');
            String name = equals < 0 ? cookie : cookie.substring(0, equals).trim();
            if (!"_xsrf".equals(name) && !name.startsWith("username-")) {
                continue;
            }
            if (allowed.length() > 0) {
                allowed.append("; ");
            }
            allowed.append(cookie);
        }
        return allowed.toString();
    }
}
