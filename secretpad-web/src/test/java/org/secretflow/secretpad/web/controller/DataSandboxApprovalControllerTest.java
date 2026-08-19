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
import org.secretflow.secretpad.common.util.JsonUtils;
import org.secretflow.secretpad.common.util.UserContext;
import org.secretflow.secretpad.persistence.entity.TokensDO;
import org.secretflow.secretpad.persistence.repository.UserTokensRepository;
import org.secretflow.secretpad.web.SecretPadApplication;
import org.secretflow.secretpad.web.service.DataSandboxMvpService;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

/**
 * Z-03 Controller 测试：门禁（非管理员直接创建/回收被拒、admin 直通、START 不设门禁）、
 * 审批角色校验（非供数方 APPROVE 被拒、申请人复审）、HTTP 两级审批全流程与审批历史、config 端点。
 *
 * <p>auth.enabled=true 走真实 token 鉴权；kuscia.enabled=false 下不触发执行引擎（@Scheduled 不跑），
 * 审批流可稳定停在 APPROVED。三个 token：admin（运营方/平台管理节点）、bob（申请方）、carol（供数方）。</p>
 */
@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
@ActiveProfiles("test")
@SpringBootTest(classes = SecretPadApplication.class)
@AutoConfigureMockMvc
@TestPropertySource(properties = {
        "secretpad.auth.enabled=true",
        "secretpad.data-sandbox.kuscia.enabled=false",
        "secretpad.data-sandbox.approval.required=true",
        "secretpad.node-id=kuscia-system",
        "secretpad.data-sandbox.snapshot-root=${java.io.tmpdir}/ds-sandbox-apr-ctrl-snapshots",
        "secretpad.data-sandbox.backup-root=${java.io.tmpdir}/ds-sandbox-apr-ctrl-backups",
        "spring.datasource.default.jdbc-url=jdbc:sqlite:${java.io.tmpdir}/ds-sandbox-apr-ctrl.sqlite",
        "spring.datasource.quartz.jdbc-url=jdbc:h2:${java.io.tmpdir}/ds-sandbox-apr-ctrl-quartz.mv.db;DB_CLOSE_ON_EXIT=FALSE",
})
public class DataSandboxApprovalControllerTest {

    private static final String ADMIN_TOKEN = "apr-admin-token";
    private static final String BOB_TOKEN = "apr-bob-token";
    private static final String CAROL_TOKEN = "apr-carol-token";
    private static final String IMAGE_ID = "img-secretflow";

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
        jdbc.update("delete from ds_sandbox_approval_history");
        jdbc.update("delete from ds_sandbox_approval");
        jdbc.update("delete from ds_sandbox");
        jdbc.update("delete from ds_sandbox_snapshot");
        jdbc.update("delete from ds_resource_allocation");
        jdbc.update("update ds_gpu_ledger set status='AVAILABLE',owner_id='',allocated_at=''");
        jdbc.update("delete from user_tokens");
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
        userTokensRepository.saveAndFlush(TokensDO.builder()
                .token(CAROL_TOKEN).name("carol")
                .gmtToken(LocalDateTime.now())
                .sessionData(JsonUtils.toJSONString(UserContextDTO.builder()
                        .ownerId("carol").name("carol")
                        .platformType(PlatformTypeEnum.CENTER).platformNodeId("carol")
                        .ownerType(UserOwnerTypeEnum.CENTER).build()))
                .build());
        UserContext.setBaseUser(UserContextDTO.builder().ownerId("alice").name("alice")
                .platformType(PlatformTypeEnum.CENTER).platformNodeId("alice")
                .ownerType(UserOwnerTypeEnum.CENTER).projectIds(Set.of("p1")).build());
    }

    /* ------------------------------- helpers ------------------------------- */

    private JsonNode call(String token, String method, String url, String body) throws Exception {
        var builder = "POST".equals(method) ? post(url) : get(url);
        var request = builder.header("User-Token", token)
                .contentType(org.springframework.http.MediaType.APPLICATION_JSON);
        if (body != null) {
            request = request.content(body);
        }
        MockHttpServletResponse response = mockMvc.perform(request).andReturn().getResponse();
        assertEquals(200, response.getStatus());
        return objectMapper.readTree(response.getContentAsString());
    }

    private String submitCreateAsBob() throws Exception {
        JsonNode body = call(BOB_TOKEN, "POST", "/api/v1alpha1/data-sandbox/approvals/submit",
                "{\"approvalType\":\"CREATE\",\"name\":\"http-sandbox\",\"imageId\":\"" + IMAGE_ID + "\","
                        + "\"networkPolicy\":\"INTERNAL_ONLY\",\"cpuCores\":1,\"memoryGb\":2,\"gpuCount\":0,"
                        + "\"storageGb\":10,\"validDays\":7,\"reason\":\"HTTP IT\"}");
        assertEquals(0, body.path("status").path("code").asInt(), body.toString());
        return body.path("data").path("id").asText();
    }

    private void approve(String token, String id, String action) throws Exception {
        JsonNode body = call(token, "POST", "/api/v1alpha1/data-sandbox/approvals/action",
                "{\"id\":\"" + id + "\",\"action\":\"" + action + "\",\"comment\":\"comment\"}");
        assertEquals(0, body.path("status").path("code").asInt(), body.toString());
    }

    /* ------------------------------- 门禁 ------------------------------- */

    @Test
    public void gateRejectsDirectCreateForNonAdmin() throws Exception {
        JsonNode body = call(BOB_TOKEN, "POST", "/api/v1alpha1/data-sandbox/sandboxes/create",
                "{\"name\":\"x\",\"imageId\":\"" + IMAGE_ID + "\",\"networkPolicy\":\"INTERNAL_ONLY\","
                        + "\"cpuCores\":1,\"memoryGb\":2,\"gpuCount\":0,\"storageGb\":10,\"validDays\":7}");
        assertEquals(AuthErrorCode.AUTH_FAILED.getCode(), body.path("status").path("code").asInt(), body.toString());
        assertTrue(body.path("status").path("msg").asText().contains("申请单审批"), body.toString());
    }

    @Test
    public void gateAllowsAdminDirectCreate() throws Exception {
        JsonNode body = call(ADMIN_TOKEN, "POST", "/api/v1alpha1/data-sandbox/sandboxes/create",
                "{\"name\":\"admin-sandbox\",\"imageId\":\"" + IMAGE_ID + "\",\"networkPolicy\":\"INTERNAL_ONLY\","
                        + "\"cpuCores\":1,\"memoryGb\":2,\"gpuCount\":0,\"storageGb\":10,\"validDays\":7}");
        assertEquals(0, body.path("status").path("code").asInt(), body.toString());
        assertTrue(body.path("data").path("id").asText().startsWith("sbx-"));
    }

    @Test
    public void gateRejectsDirectDestroyForNonAdmin() throws Exception {
        // bob 直建沙箱（服务层，绕过门禁）以便测 action 门禁
        Map<String, Object> created = service.createSandbox(Map.of(
                "name", "owner-sbx", "ownerId", "bob", "imageId", IMAGE_ID,
                "networkPolicy", "INTERNAL_ONLY", "cpuCores", 1, "memoryGb", 2, "gpuCount", 0,
                "storageGb", 10, "validDays", 7));
        String id = String.valueOf(created.get("id"));
        JsonNode body = call(BOB_TOKEN, "POST", "/api/v1alpha1/data-sandbox/sandboxes/action",
                "{\"id\":\"" + id + "\",\"action\":\"DESTROY\"}");
        assertEquals(AuthErrorCode.AUTH_FAILED.getCode(), body.path("status").path("code").asInt(), body.toString());
        assertTrue(body.path("status").path("msg").asText().contains("回收沙箱需提交回收申请单审批"), body.toString());
    }

    @Test
    public void gateDoesNotBlockStartForNonAdmin() throws Exception {
        Map<String, Object> created = service.createSandbox(Map.of(
                "name", "owner-sbx", "ownerId", "bob", "imageId", IMAGE_ID,
                "networkPolicy", "INTERNAL_ONLY", "cpuCores", 1, "memoryGb", 2, "gpuCount", 0,
                "storageGb", 10, "validDays", 7));
        String id = String.valueOf(created.get("id"));
        // START 不设门禁：kuscia 未启用会置 ERROR，但门禁不应拦截
        JsonNode body = call(BOB_TOKEN, "POST", "/api/v1alpha1/data-sandbox/sandboxes/action",
                "{\"id\":\"" + id + "\",\"action\":\"START\"}");
        assertEquals(0, body.path("status").path("code").asInt(), body.toString());
        assertEquals("ERROR", body.path("data").path("status").asText(), "kuscia 未启用应置 ERROR 而非门禁拒绝");
    }

    /* ------------------------------- config ------------------------------- */

    @Test
    public void approvalConfigReturnsGateAndTypes() throws Exception {
        JsonNode body = call(BOB_TOKEN, "GET", "/api/v1alpha1/data-sandbox/approvals/config", null);
        assertEquals(0, body.path("status").path("code").asInt(), body.toString());
        assertTrue(body.path("data").path("required").asBoolean());
        assertEquals(3, body.path("data").path("maxRetries").asInt());
        assertEquals(4, body.path("data").path("types").size());
    }

    /* ------------------------------- 审批角色与流程 ------------------------------- */

    @Test
    public void wrongRoleApproveRejected() throws Exception {
        String id = submitCreateAsBob();
        // 申请人本人不能当阶段1 供数方
        JsonNode body = call(BOB_TOKEN, "POST", "/api/v1alpha1/data-sandbox/approvals/action",
                "{\"id\":\"" + id + "\",\"action\":\"APPROVE\"}");
        assertEquals(AuthErrorCode.AUTH_FAILED.getCode(), body.path("status").path("code").asInt(), body.toString());
        assertTrue(body.path("status").path("msg").asText().contains("您不是供数方"), body.toString());
    }

    @Test
    public void twoStageApprovalThroughHttpWithHistory() throws Exception {
        String id = submitCreateAsBob();
        // 供数方 carol 阶段1 → 运营方 admin 阶段2
        approve(CAROL_TOKEN, id, "APPROVE");
        approve(ADMIN_TOKEN, id, "APPROVE");
        // kuscia.enabled=false 且 @Scheduled 不跑，审批停在 APPROVED 待执行
        JsonNode detail = call(BOB_TOKEN, "GET", "/api/v1alpha1/data-sandbox/approvals/detail?id=" + id, null);
        assertEquals(0, detail.path("status").path("code").asInt(), detail.toString());
        assertEquals("APPROVED", detail.path("data").path("status").asText());
        // 历史：SUBMIT + APPROVE + APPROVE
        JsonNode history = call(BOB_TOKEN, "GET", "/api/v1alpha1/data-sandbox/approvals/history?id=" + id, null);
        assertEquals(3, history.path("data").size(), history.toString());
        // history 按 id desc：最新动作（第二次 APPROVE）在前
        assertEquals("APPROVE", history.path("data").get(0).path("action").asText());
        assertTrue(history.path("data").get(2).path("action").asText().contains("SUBMIT"));
    }

    @Test
    public void rejectThenResubmitByApplicant() throws Exception {
        String id = submitCreateAsBob();
        approve(CAROL_TOKEN, id, "REJECT");
        JsonNode rejected = call(BOB_TOKEN, "GET", "/api/v1alpha1/data-sandbox/approvals/detail?id=" + id, null);
        assertEquals("REJECTED", rejected.path("data").path("status").asText());

        // 申请人 RESUBMIT → version=2
        JsonNode resubmit = call(BOB_TOKEN, "POST", "/api/v1alpha1/data-sandbox/approvals/action",
                "{\"id\":\"" + id + "\",\"action\":\"RESUBMIT\"}");
        assertEquals(0, resubmit.path("status").path("code").asInt(), resubmit.toString());
        assertEquals("DATA_PROVIDER_REVIEW", resubmit.path("data").path("status").asText());
        assertEquals(2, resubmit.path("data").path("version").asInt());
    }

    @Test
    public void listFiltersByStatusAndType() throws Exception {
        String id = submitCreateAsBob();
        JsonNode list = call(BOB_TOKEN, "GET",
                "/api/v1alpha1/data-sandbox/approvals?status=DATA_PROVIDER_REVIEW&type=CREATE", null);
        assertEquals(1, list.path("data").size(), list.toString());
        assertEquals(id, list.path("data").get(0).path("id").asText());
        // 过滤不匹配的状态返回空
        JsonNode none = call(BOB_TOKEN, "GET",
                "/api/v1alpha1/data-sandbox/approvals?status=COMPLETED&type=CREATE", null);
        assertEquals(0, none.path("data").size(), none.toString());
    }
}
