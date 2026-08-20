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

import org.secretflow.secretpad.common.dto.UserContextDTO;
import org.secretflow.secretpad.common.enums.PlatformTypeEnum;
import org.secretflow.secretpad.common.enums.UserOwnerTypeEnum;
import org.secretflow.secretpad.common.errorcode.AuthErrorCode;
import org.secretflow.secretpad.common.errorcode.SystemErrorCode;
import org.secretflow.secretpad.common.util.JsonUtils;
import org.secretflow.secretpad.common.util.UserContext;
import org.secretflow.secretpad.persistence.entity.TokensDO;
import org.secretflow.secretpad.persistence.repository.UserTokensRepository;
import org.secretflow.secretpad.web.SecretPadApplication;
import org.secretflow.secretpad.web.service.DataSandboxMvpService;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

/**
 * Controller tests for the Z-01 dev endpoint: POST /sandboxes/dev-token issues a
 * one-time token for a RUNNING sandbox (owner/admin only), and the /proxy/{id} jump
 * board enforces the token independently of login state, then forwards to the
 * sandbox endpoint stored in DB (SSRF-safe).
 *
 * <p>auth.enabled=true on purpose: the dev-token endpoint must go through the normal
 * login flow so the "non-owner rejected" branch is exercised. The proxy branch is
 * enforced by {@link org.secretflow.secretpad.web.interceptor.LoginInterceptor}
 * regardless of auth.enabled.
 */
@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD) // surefire parallel=all 会并行执行类内方法，@BeforeEach 清理会互相删除数据，必须串行
@ActiveProfiles("test")
@SpringBootTest(classes = SecretPadApplication.class)
@AutoConfigureMockMvc
@TestPropertySource(properties = {
        // 本类测试真实用户 token 鉴权流程（非 owner 拒绝分支），interceptor 不再直接放行 admin
        "secretpad.auth.enabled=true",
        "secretpad.data-sandbox.kuscia.enabled=false",
        "secretpad.node-id=kuscia-system",
        "secretpad.data-sandbox.snapshot-root=${java.io.tmpdir}/ds-sandbox-ctrl-snapshots",
        "secretpad.data-sandbox.backup-root=${java.io.tmpdir}/ds-sandbox-ctrl-backups",
        // 测试工作目录（模块目录）没有 db/ 目录，SQLite 不自动建目录：数据源指到 /tmp 下的全新库
        "spring.datasource.default.jdbc-url=jdbc:sqlite:${java.io.tmpdir}/ds-sandbox-ctrl.sqlite",
        "spring.datasource.quartz.jdbc-url=jdbc:h2:${java.io.tmpdir}/ds-sandbox-ctrl-quartz.mv.db;DB_CLOSE_ON_EXIT=FALSE",
})
public class DataSandboxControllerTest {

    private static final String ADMIN_TOKEN = "admin-token-0001";
    private static final String BOB_TOKEN = "bob-token-0002";

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Resource
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbc;

    @Resource
    private UserTokensRepository userTokensRepository;

    @Resource
    private DataSandboxMvpService service;

    @Resource
    private MockMvc mockMvc;

    @BeforeEach
    public void reset() {
        jdbc.update("delete from ds_sandbox");
        jdbc.update("delete from ds_sandbox_snapshot");
        // Z-02：生命周期感知用量统计下，跨用例残留的分配行会累积占用配额，必须一并复位
        jdbc.update("delete from ds_resource_allocation");
        jdbc.update("update ds_gpu_ledger set status='AVAILABLE',owner_id='',allocated_at=''");
        // TokensDO 走 @SQLDelete 软删除（is_deleted=1），deleteAll 不会真正移除行，同 PK 再插入会冲突，用 JDBC 硬删
        jdbc.update("delete from user_tokens");
        // 平台管理员（ownerId=kuscia-system + name=admin 判定）与普通用户 bob
        userTokensRepository.saveAndFlush(TokensDO.builder()
                .token(ADMIN_TOKEN).name("admin")
                .gmtToken(LocalDateTime.now())
                .sessionData(JsonUtils.toJSONString(UserContextDTO.builder()
                        .ownerId("kuscia-system").name("admin")
                        .platformType(PlatformTypeEnum.CENTER).platformNodeId("kuscia-system")
                        .ownerType(UserOwnerTypeEnum.CENTER).build()))
                .build());
        userTokensRepository.saveAndFlush(TokensDO.builder()
                .token(BOB_TOKEN).name("bob")
                .gmtToken(LocalDateTime.now())
                .sessionData(JsonUtils.toJSONString(UserContextDTO.builder()
                        .ownerId("bob").name("bob")
                        .platformType(PlatformTypeEnum.CENTER).platformNodeId("bob")
                        .ownerType(UserOwnerTypeEnum.CENTER).build()))
                .build());
        UserContext.setBaseUser(UserContextDTO.builder().ownerId("alice").name("alice")
                .platformType(PlatformTypeEnum.CENTER).platformNodeId("alice")
                .ownerType(UserOwnerTypeEnum.CENTER).projectIds(Set.of("p1")).build());
    }

    /* ------------------------------- helpers ------------------------------- */

    private String createSandbox() {
        return createSandbox("INTERNAL_ONLY");
    }

    private String createSandbox(String networkPolicy) {
        Map<String, Object> created = service.createSandbox(Map.of(
                "name", "ctrl-sandbox", "ownerId", "alice", "imageId", "img-secretflow",
                "networkPolicy", networkPolicy, "cpuCores", 1, "memoryGb", 2, "gpuCount", 0,
                "storageGb", 10, "validDays", 7));
        return String.valueOf(created.get("id"));
    }

    private void setRunning(String id, String endpoint) {
        jdbc.update("update ds_sandbox set status='RUNNING', endpoint=? where id=?", endpoint, id);
    }

    private String sha256(String value) throws Exception {
        return java.util.HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
    }

    /** 以 admin 身份调用 service 签发 dev token（沙箱需 RUNNING + endpoint），返回完整跳板 URL。 */
    private String issueProxyUrl(String id) throws Exception {
        Map<String, Object> issued = service.generateDevToken(id);
        return String.valueOf(issued.get("url"));
    }

    private JsonNode postDevToken(String id, String loginToken) throws Exception {
        MockHttpServletResponse response = mockMvc.perform(post("/api/v1alpha1/data-sandbox/sandboxes/dev-token")
                        .header("User-Token", loginToken)
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content("{\"id\":\"" + id + "\"}"))
                .andReturn().getResponse();
        assertEquals(200, response.getStatus());
        return objectMapper.readTree(response.getContentAsString());
    }

    private JsonNode getProxy(String url) throws Exception {
        MockHttpServletResponse response = mockMvc.perform(get(url))
                .andReturn().getResponse();
        return objectMapper.readTree(response.getContentAsString());
    }

    /* ------------------------------- dev-token ------------------------------- */

    @Test
    public void devTokenRejectedWhenSandboxNotRunning() throws Exception {
        String id = createSandbox(); // STOPPED
        JsonNode body = postDevToken(id, ADMIN_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("沙箱未运行"), body.toString());
    }

    @Test
    public void devTokenRejectedForNonOwner() throws Exception {
        String id = createSandbox(); // owner=alice
        JsonNode body = postDevToken(id, BOB_TOKEN); // bob 非 owner 非 admin
        assertEquals(AuthErrorCode.AUTH_FAILED.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("无权访问"), body.toString());
    }

    @Test
    public void devTokenIssuesProxyUrlForRunningSandbox() throws Exception {
        String id = createSandbox();
        setRunning(id, "10.0.0.1:31234");
        JsonNode body = postDevToken(id, ADMIN_TOKEN);
        assertEquals(0, body.path("status").path("code").asInt(), body.toString());
        String url = body.path("data").path("url").asText();
        assertTrue(url.startsWith("/api/v1alpha1/data-sandbox/proxy/" + id + "?token="), url);
        assertNotNull(body.path("data").path("expiresAt").asText());
        // 数据库只存 sha256，不存明文
        String stored = jdbc.queryForObject("select endpoint_token from ds_sandbox where id=?", String.class, id);
        String plain = url.substring(url.indexOf("token=") + 6);
        assertEquals(sha256(plain), stored);
    }

    /* ------------------------------- proxy jump board ------------------------------- */

    @Test
    public void proxyRejectsMissingToken() throws Exception {
        String id = createSandbox();
        JsonNode body = getProxy("/api/v1alpha1/data-sandbox/proxy/" + id);
        assertEquals(AuthErrorCode.AUTH_FAILED.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("缺少开发端点访问凭证"), body.toString());
    }

    @Test
    public void proxyRejectsExpiredToken() throws Exception {
        String id = createSandbox();
        setRunning(id, "10.0.0.1:31234");
        String url = issueProxyUrl(id);
        // 令牌过期（签发后立即改库）
        jdbc.update("update ds_sandbox set endpoint_token_expires_at=? where id=?",
                LocalDateTime.now().minusMinutes(1).toString(), id);
        JsonNode body = getProxy(url);
        assertEquals(AuthErrorCode.AUTH_FAILED.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("已过期"), body.toString());
    }

    @Test
    public void proxyRejectsNonRunningSandbox() throws Exception {
        String id = createSandbox();
        setRunning(id, "10.0.0.1:31234");
        String url = issueProxyUrl(id);
        // 令牌有效但沙箱已停止：跳板必须拒绝
        jdbc.update("update ds_sandbox set status='STOPPED' where id=?", id);
        JsonNode body = getProxy(url);
        assertEquals(AuthErrorCode.AUTH_FAILED.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("沙箱未运行"), body.toString());
    }

    @Test
    public void proxyRejectsWhenEndpointMissing() throws Exception {
        String id = createSandbox();
        setRunning(id, "");
        // 无法走 service 签发（endpoint 校验），直接手工写入 sha256 令牌
        String plain = "manual-token-999";
        jdbc.update("update ds_sandbox set endpoint_token=?,endpoint_token_expires_at=? where id=?",
                sha256(plain), LocalDateTime.now().plusMinutes(30).toString(), id);
        JsonNode body = getProxy("/api/v1alpha1/data-sandbox/proxy/" + id + "?token=" + plain);
        assertEquals(AuthErrorCode.AUTH_FAILED.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("尚未就绪"), body.toString());
    }

    @Test
    public void proxyForwardsToSandboxEndpoint() throws Exception {
        // 本地 HTTP 服务模拟沙箱容器内的开发端点（如 Jupyter）
        AtomicReference<String> capturedUri = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            capturedUri.set(exchange.getRequestURI().toString());
            byte[] body = "hello-from-sandbox".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try {
            String id = createSandbox();
            setRunning(id, "127.0.0.1:" + server.getAddress().getPort());
            String url = issueProxyUrl(id);
            String token = url.substring(url.indexOf("token=") + 6);
            MockHttpServletResponse response = mockMvc.perform(
                            get("/api/v1alpha1/data-sandbox/proxy/" + id + "/hello?x=1&token=" + token))
                    .andReturn().getResponse();
            assertEquals(200, response.getStatus());
            assertEquals("hello-from-sandbox", response.getContentAsString());
            // 跳板只转发真实路径：凭证 token 被剥除，不进入容器内应用
            assertEquals("/hello?x=1", capturedUri.get());
        } finally {
            server.stop(0);
        }
    }

    /* ------------------------------- Z-02 网络白名单 / 限制校验 ------------------------------- */

    @Test
    public void noNetworkRejectsDevTokenAtHttpLayer() throws Exception {
        String id = createSandbox("NO_NETWORK");
        setRunning(id, "10.0.0.1:31234");
        JsonNode body = postDevToken(id, ADMIN_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("NO_NETWORK"), body.toString());
    }

    @Test
    public void allowlistCrudThroughHttp() throws Exception {
        String id = createSandbox();
        // 新增（auth.enabled=true，需带登录 token）
        MockHttpServletResponse add = mockMvc.perform(post("/api/v1alpha1/data-sandbox/resources/network/allowlist")
                        .header("User-Token", ADMIN_TOKEN)
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content("{\"sandboxId\":\"" + id + "\",\"host\":\"db.internal\",\"port\":5432,\"proto\":\"tcp\",\"remark\":\"训练库\"}"))
                .andReturn().getResponse();
        assertEquals(200, add.getStatus());
        JsonNode addBody = objectMapper.readTree(add.getContentAsString());
        assertEquals(0, addBody.path("status").path("code").asInt(), addBody.toString());
        String entryId = addBody.path("data").path("id").asText();
        assertTrue(entryId.startsWith("al-"), entryId);

        // 列表
        MockHttpServletResponse list = mockMvc.perform(get("/api/v1alpha1/data-sandbox/resources/network/allowlist")
                        .header("User-Token", ADMIN_TOKEN)
                        .param("sandboxId", id))
                .andReturn().getResponse();
        JsonNode listBody = objectMapper.readTree(list.getContentAsString());
        assertEquals(1, listBody.path("data").size());

        // 删除
        MockHttpServletResponse del = mockMvc.perform(post("/api/v1alpha1/data-sandbox/resources/network/allowlist/delete")
                        .header("User-Token", ADMIN_TOKEN)
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content("{\"id\":\"" + entryId + "\"}"))
                .andReturn().getResponse();
        JsonNode delBody = objectMapper.readTree(del.getContentAsString());
        assertEquals(0, delBody.path("status").path("code").asInt(), delBody.toString());
        MockHttpServletResponse after = mockMvc.perform(get("/api/v1alpha1/data-sandbox/resources/network/allowlist")
                        .header("User-Token", ADMIN_TOKEN)
                        .param("sandboxId", id))
                .andReturn().getResponse();
        assertEquals(0, objectMapper.readTree(after.getContentAsString()).path("data").size());
    }

    @Test
    public void limitVerifyReturnsExpectedAndInstructions() throws Exception {
        String id = createSandbox();
        MockHttpServletResponse response = mockMvc.perform(post("/api/v1alpha1/data-sandbox/operations/limit-verify")
                        .header("User-Token", ADMIN_TOKEN)
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content("{\"sandboxId\":\"" + id + "\"}"))
                .andReturn().getResponse();
        assertEquals(200, response.getStatus());
        JsonNode body = objectMapper.readTree(response.getContentAsString());
        assertEquals(0, body.path("status").path("code").asInt(), body.toString());
        assertEquals(id, body.path("data").path("sandboxId").asText());
        assertEquals(1, body.path("data").path("expected").path("cpu").asInt());
        assertEquals(2, body.path("data").path("expected").path("memory_gb").asInt());
        assertTrue(body.path("data").path("instructions").asText().contains("verify-limits.sh"));
    }
}
