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
import org.secretflow.secretpad.common.errorcode.SystemErrorCode;
import org.secretflow.secretpad.common.util.JsonUtils;
import org.secretflow.secretpad.common.util.UserContext;
import org.secretflow.secretpad.kuscia.v1alpha1.DynamicKusciaChannelProvider;
import org.secretflow.secretpad.kuscia.v1alpha1.constant.KusciaAPIConstants;
import org.secretflow.secretpad.kuscia.v1alpha1.constant.KusciaProtocolEnum;
import org.secretflow.secretpad.kuscia.v1alpha1.mock.MockKusciaGrpcServer;
import org.secretflow.secretpad.kuscia.v1alpha1.mock.service.HealthService;
import org.secretflow.secretpad.kuscia.v1alpha1.mock.service.JobService;
import org.secretflow.secretpad.kuscia.v1alpha1.model.KusciaGrpcConfig;
import org.secretflow.secretpad.persistence.entity.TokensDO;
import org.secretflow.secretpad.persistence.repository.UserTokensRepository;
import org.secretflow.secretpad.web.SecretPadApplication;
import org.secretflow.secretpad.web.service.governance.DataGovernanceService;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.secretflow.v1alpha1.common.Common;
import org.secretflow.v1alpha1.kusciaapi.DomainDataServiceGrpc;
import org.secretflow.v1alpha1.kusciaapi.Domaindata;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Z-04 Stage 3 Controller 测试：数据治理 API 全端点（策略 CRUD / 任务提交取消重试 / 结果 / 血缘 /
 * 预览），真实 token 鉴权 + 权限门禁 + 参数/状态错误码。
 *
 * <p>独立 mock 端口 50054（避开两个 governance IT 的 50053）+ SQLite + 临时数据目录；
 * mock DomainDataService 提供源表元数据并记录 createDomainData。</p>
 */
@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
@ActiveProfiles("test")
@SpringBootTest(classes = SecretPadApplication.class)
@AutoConfigureMockMvc
@TestPropertySource(properties = {
        "secretpad.auth.enabled=true",
        "kuscia.nodes=",
        "secretpad.data-sandbox.kuscia.enabled=true",
        "secretpad.node-id=kuscia-system",
        "spring.datasource.default.jdbc-url=jdbc:sqlite:${java.io.tmpdir}/ds-gov-ctrl.sqlite",
        "spring.datasource.quartz.jdbc-url=jdbc:h2:${java.io.tmpdir}/ds-gov-ctrl-quartz.mv.db;DB_CLOSE_ON_EXIT=FALSE",
        "secretpad.data.dir-path=${java.io.tmpdir}/ds-gov-ctrl-data/",
        "secretpad.data-sandbox.governance.input-rows=10000",
        "secretpad.data-sandbox.governance.max-retries=3",
})
public class DataGovernanceControllerTest {

    private static final int MOCK_PORT = 50054;
    private static final String SOURCE_DT = "dt-sample";
    private static final String SOURCE_URI = "sample_full.csv";
    private static final String DATA_ROOT = System.getProperty("java.io.tmpdir") + "/ds-gov-ctrl-data";

    private static final String ADMIN_TOKEN = "gov-admin-token-0001";
    private static final String ALICE_TOKEN = "gov-alice-token-0002";
    private static final String CAROL_TOKEN = "gov-carol-token-0003";

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Resource
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbc;

    @Resource
    private UserTokensRepository userTokensRepository;

    @Resource
    private DataGovernanceService governance;

    @Resource
    private DynamicKusciaChannelProvider channelProvider;

    @Resource
    private MockMvc mockMvc;

    private MockKusciaGrpcServer mockServer;

    /* ------------------------------- 生命周期 ------------------------------- */

    @BeforeAll
    public void startMock() throws Exception {
        Files.createDirectories(Path.of(DATA_ROOT, "alice"));
        try (InputStream in = getClass().getResourceAsStream("/gov/sample_full.csv")) {
            assertNotNull(in, "test resource gov/sample_full.csv missing");
            Files.copy(in, Path.of(DATA_ROOT, "alice", SOURCE_URI), StandardCopyOption.REPLACE_EXISTING);
        }
        mockServer = new MockKusciaGrpcServer();
        mockServer.start(MOCK_PORT, KusciaProtocolEnum.NOTLS, List.of(
                new CtrlDomainDataService(), new JobService(), new HealthService()));
        KusciaGrpcConfig config = mockServer.buildKusciaGrpcConfig("kuscia-system");
        config.setPort(MOCK_PORT);
        channelProvider.registerKuscia(config);
    }

    @AfterAll
    public void stopMock() {
        mockServer.stop();
    }

    @BeforeEach
    public void reset() {
        jdbc.update("delete from ds_governance_lineage");
        jdbc.update("delete from ds_governance_task");
        jdbc.update("delete from ds_governance_policy");
        jdbc.update("delete from project_datatable where project_id in ('p1','p2')");
        jdbc.update("delete from node where node_id in ('alice','carol')");
        jdbc.update("delete from ds_alert_event where source='GOVERNANCE'");
        jdbc.update("delete from ds_unified_log where resource_type='GOVERNANCE_POLICY' or resource_type='GOVERNANCE_TASK' or action like 'GOVERNANCE%'");
        // TokensDO 走 @SQLDelete 软删除，同 PK 再插入会冲突，用 JDBC 硬删后重插
        jdbc.update("delete from user_tokens");
        CtrlDomainDataService.created.clear();
        CtrlDomainDataService.uriByDatatable.clear();
        CtrlDomainDataService.createCode = KusciaAPIConstants.OK;
        CtrlDomainDataService.relativeUri = SOURCE_URI;
        CtrlDomainDataService.datatableId = SOURCE_DT;
        CtrlDomainDataService.domainId = "alice";
        CtrlDomainDataService.columns = CtrlDomainDataService.defaultColumns();
        // 权限基础数据：alice/carol 节点 + (p1, alice, dt-sample) 授权
        jdbc.update("insert into node(node_id,name,control_node_id,type,mode) values('alice','alice-node','master','normal',0)");
        jdbc.update("insert into node(node_id,name,control_node_id,type,mode) values('carol','carol-node','master','normal',0)");
        jdbc.update("insert into project_datatable(project_id,node_id,datatable_id,table_configs,source,is_deleted) values('p1','alice','" + SOURCE_DT + "','[]','IMPORTED',0)");
        saveToken(ADMIN_TOKEN, "admin", "kuscia-system", Set.of());
        saveToken(ALICE_TOKEN, "alice", "alice", Set.of("p1"));
        saveToken(CAROL_TOKEN, "carol", "carol", Set.of("p2"));
        // 与 token 无关的 service 级调用兜底（本测试走 MockMvc，UserContext 由 interceptor 设置）
        UserContext.setBaseUser(UserContextDTO.builder().ownerId("alice").name("alice")
                .platformType(PlatformTypeEnum.CENTER).platformNodeId("alice")
                .ownerType(UserOwnerTypeEnum.CENTER).projectIds(Set.of("p1")).build());
    }

    private void saveToken(String token, String name, String ownerId, Set<String> projectIds) {
        userTokensRepository.saveAndFlush(TokensDO.builder()
                .token(token).name(name)
                .gmtToken(LocalDateTime.now())
                .sessionData(JsonUtils.toJSONString(UserContextDTO.builder()
                        .ownerId(ownerId).name(name)
                        .platformType(PlatformTypeEnum.CENTER).platformNodeId(ownerId)
                        .ownerType(UserOwnerTypeEnum.CENTER).projectIds(projectIds).build()))
                .build());
    }

    /* ------------------------------- mock ------------------------------- */

    /** 富数据 DomainData mock：query 返回真实元数据（relativeUri + schema），create 记录调用并按 datatableId 回放 relativeUri。 */
    public static class CtrlDomainDataService extends DomainDataServiceGrpc.DomainDataServiceImplBase {
        static volatile String relativeUri = SOURCE_URI;
        static volatile String datatableId = SOURCE_DT;
        static volatile String domainId = "alice";
        static volatile int createCode = KusciaAPIConstants.OK;
        static final List<Domaindata.CreateDomainDataRequest> created = new CopyOnWriteArrayList<>();
        static final Map<String, String> uriByDatatable = new ConcurrentHashMap<>();
        static volatile List<Common.DataColumn> columns = defaultColumns();

        static List<Common.DataColumn> defaultColumns() {
            return List.of(
                    column("id", "int"),
                    column("name", "str"),
                    column("phone", "str"),
                    column("id_card", "str"),
                    column("category", "str"),
                    column("amount", "str"),
                    column("score", "str"),
                    column("memo", "str"));
        }

        private static Common.DataColumn column(String name, String type) {
            return Common.DataColumn.newBuilder().setName(name).setType(type).build();
        }

        @Override
        public void queryDomainData(Domaindata.QueryDomainDataRequest request,
                                    StreamObserver<Domaindata.QueryDomainDataResponse> observer) {
            String qid = request.getData().getDomaindataId();
            String qUri = uriByDatatable.getOrDefault(qid, relativeUri);
            Domaindata.DomainData domainData = Domaindata.DomainData.newBuilder()
                    .setDomaindataId(qid)
                    .setDomainId(domainId)
                    .setAuthor(domainId)
                    .setName(qid)
                    .setType("table")
                    .setRelativeUri(qUri)
                    .setVendor("datatable")
                    .setFileFormat(Common.FileFormat.CSV)
                    .addAllColumns(columns)
                    .build();
            Domaindata.QueryDomainDataResponse response = Domaindata.QueryDomainDataResponse.newBuilder()
                    .setStatus(Common.Status.newBuilder().setCode(0).setMessage("success").build())
                    .setData(domainData)
                    .build();
            observer.onNext(response);
            observer.onCompleted();
        }

        @Override
        public void createDomainData(Domaindata.CreateDomainDataRequest request,
                                     StreamObserver<Domaindata.CreateDomainDataResponse> observer) {
            created.add(request);
            uriByDatatable.put(request.getDomaindataId(), request.getRelativeUri());
            int code = createCode;
            String message = code == KusciaAPIConstants.OK ? "success" : "mock create failed";
            observer.onNext(Domaindata.CreateDomainDataResponse.newBuilder()
                    .setStatus(Common.Status.newBuilder().setCode(code).setMessage(message).build())
                    .build());
            observer.onCompleted();
        }
    }

    /* ------------------------------- helpers ------------------------------- */

    private JsonNode doGet(String url, String token) throws Exception {
        MockHttpServletResponse response = mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get(url)
                        .header("User-Token", token))
                .andReturn().getResponse();
        assertEquals(200, response.getStatus());
        return objectMapper.readTree(response.getContentAsString());
    }

    private JsonNode doPost(String url, String body, String token) throws Exception {
        MockHttpServletResponse response = mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post(url)
                        .header("User-Token", token)
                        .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                        .content(body))
                .andReturn().getResponse();
        assertEquals(200, response.getStatus());
        return objectMapper.readTree(response.getContentAsString());
    }

    private String json(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** 走控制器提交一个 BUILTIN 抽样任务，返回任务 data JSON。 */
    private JsonNode submitSampling(String token) throws Exception {
        return doPost("/api/v1alpha1/data-governance/tasks/submit", json(Map.of(
                        "name", "ctrl-sampling",
                        "execMode", "BUILTIN",
                        "nodeId", "alice",
                        "datatableId", SOURCE_DT,
                        "sampling", Map.of("method", "RANDOM", "count", 3),
                        "masking", List.of())),
                token);
    }

    /** 走控制器提交一个 BUILTIN 手机号掩码任务，返回任务 data JSON。 */
    private JsonNode submitMasking(String token) throws Exception {
        return doPost("/api/v1alpha1/data-governance/tasks/submit", json(Map.of(
                        "name", "ctrl-masking",
                        "execMode", "BUILTIN",
                        "nodeId", "alice",
                        "datatableId", SOURCE_DT,
                        "sampling", Map.of(),
                        "masking", List.of(Map.of("column", "phone", "method", "MASK",
                                "params", Map.of("keepLeft", "3", "keepRight", "4"))))),
                token);
    }

    /* ------------------------------- 策略 ------------------------------- */

    @Test
    public void policyCrudThroughController() throws Exception {
        JsonNode created = doPost("/api/v1alpha1/data-governance/policies", json(Map.of(
                        "name", "手机号掩码策略",
                        "policyType", "MASKING",
                        "samplingParams", "{}",
                        "maskingColumns", "[{\"column\":\"phone\",\"method\":\"MASK\",\"params\":{\"keepLeft\":\"3\",\"keepRight\":\"4\"}}]")),
                ALICE_TOKEN);
        assertEquals(0, created.path("status").path("code").asInt(), created.toString());
        String id = created.path("data").path("id").asText();
        assertTrue(id.startsWith("gp-"), id);
        assertEquals("MASKING", created.path("data").path("policy_type").asText());

        // 同名拒绝（GOV_STATE_CONFLICT）
        JsonNode dup = doPost("/api/v1alpha1/data-governance/policies", json(Map.of(
                        "name", "手机号掩码策略", "policyType", "MASKING")),
                ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), dup.path("status").path("code").asInt());
        assertTrue(dup.path("status").path("msg").asText().contains("已存在"), dup.toString());

        // 列表 + 详情
        JsonNode list = doGet("/api/v1alpha1/data-governance/policies?type=MASKING", ALICE_TOKEN);
        assertEquals(1, list.path("data").size());
        JsonNode detail = doGet("/api/v1alpha1/data-governance/policies/detail?id=" + id, ALICE_TOKEN);
        assertEquals(id, detail.path("data").path("id").asText());

        // 更新（仅创建人）→ 软删
        JsonNode updated = doPost("/api/v1alpha1/data-governance/policies/update", json(Map.of(
                        "id", id, "description", "updated-desc")),
                ALICE_TOKEN);
        assertEquals(0, updated.path("status").path("code").asInt());
        JsonNode deleted = doPost("/api/v1alpha1/data-governance/policies/delete", json(Map.of("id", id)), ALICE_TOKEN);
        assertEquals(0, deleted.path("status").path("code").asInt());
        JsonNode after = doGet("/api/v1alpha1/data-governance/policies?type=MASKING", ALICE_TOKEN);
        assertEquals(0, after.path("data").size());
    }

    @Test
    public void policyInvalidTypeRejected() throws Exception {
        JsonNode body = doPost("/api/v1alpha1/data-governance/policies", json(Map.of(
                        "name", "bad-policy", "policyType", "BOGUS")),
                ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("policyType"), body.toString());
    }

    @Test
    public void policyNotCreatorCannotUpdate() throws Exception {
        JsonNode created = doPost("/api/v1alpha1/data-governance/policies", json(Map.of(
                        "name", "alice-policy", "policyType", "SAMPLING", "samplingParams", "{}", "maskingColumns", "[]")),
                ALICE_TOKEN);
        String id = created.path("data").path("id").asText();
        // carol 非创建人更新 → 拒绝
        JsonNode body = doPost("/api/v1alpha1/data-governance/policies/update", json(Map.of("id", id, "description", "hijack")),
                CAROL_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("创建人"), body.toString());
    }

    /* ------------------------------- 任务 ------------------------------- */

    @Test
    public void submitBuiltinSuccessRegistersResult() throws Exception {
        JsonNode task = submitSampling(ALICE_TOKEN);
        assertEquals(0, task.path("status").path("code").asInt(), task.toString());
        JsonNode data = task.path("data");
        assertEquals("SUCCEEDED", data.path("status").asText());
        assertEquals("alice", data.path("result_node_id").asText());
        assertTrue(data.path("result_datatable_id").asText().length() > 0, data.toString());
        assertEquals(3L, data.path("result_rows").asLong());
        assertEquals(1, CtrlDomainDataService.created.size());
        // 血缘
        JsonNode lineage = doGet("/api/v1alpha1/data-governance/lineage?nodeId=alice", ALICE_TOKEN);
        assertEquals(1, lineage.path("data").size());
        assertEquals(data.path("id").asText(), lineage.path("data").get(0).path("task_id").asText());
        // 结果列表
        JsonNode results = doGet("/api/v1alpha1/data-governance/tasks/results?nodeId=alice", ALICE_TOKEN);
        assertEquals(1, results.path("data").size());
    }

    @Test
    public void submitRejectedWithoutPermission() throws Exception {
        // carol 无权访问 alice 的表
        JsonNode body = submitSampling(CAROL_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("GOV_NO_PERMISSION"), body.toString());
        assertEquals(0L, jdbc.queryForObject("select count(1) from ds_governance_task", Long.class));
    }

    @Test
    public void submitMissingParamRejected() throws Exception {
        JsonNode body = doPost("/api/v1alpha1/data-governance/tasks/submit", json(Map.of(
                        "execMode", "BUILTIN", "datatableId", SOURCE_DT, "sampling", Map.of("method", "RANDOM"))),
                ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("nodeId"), body.toString());
    }

    @Test
    public void submitUnknownExecModeRejected() throws Exception {
        JsonNode body = doPost("/api/v1alpha1/data-governance/tasks/submit", json(Map.of(
                        "execMode", "FANCY", "nodeId", "alice", "datatableId", SOURCE_DT)),
                ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("execMode"), body.toString());
    }

    @Test
    public void cancelSucceededTaskRejected() throws Exception {
        JsonNode task = submitSampling(ALICE_TOKEN);
        String id = task.path("data").path("id").asText();
        JsonNode body = doPost("/api/v1alpha1/data-governance/tasks/cancel", json(Map.of("id", id)), ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("不可取消"), body.toString());
    }

    @Test
    public void retryNonFailedRejected() throws Exception {
        JsonNode task = submitSampling(ALICE_TOKEN);
        String id = task.path("data").path("id").asText();
        JsonNode body = doPost("/api/v1alpha1/data-governance/tasks/retry", json(Map.of("id", id)), ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("仅 FAILED"), body.toString());
    }

    @Test
    public void mountRequiresResultDatatable() throws Exception {
        // 直接插入一条 SUCCEEDED 但无结果数据集的假任务 → 挂载拒绝
        jdbc.update("insert into ds_governance_task(id,name,exec_mode,source_node_id,source_datatable_id,"
                        + "status,created_by,created_at,updated_at,deleted) "
                        + "values('gt-ctrl-mount','ctrl-mount','BUILTIN','alice','" + SOURCE_DT + "','SUCCEEDED','alice','2026-08-19 00:00:00','2026-08-19 00:00:00',0)");
        JsonNode body = doPost("/api/v1alpha1/data-governance/tasks/mount", json(Map.of(
                        "taskId", "gt-ctrl-mount", "projectId", "p1")),
                ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("仅 SUCCEEDED"), body.toString());
    }

    /* ------------------------------- 预览 ------------------------------- */

    @Test
    public void previewReturnsLimitedRows() throws Exception {
        JsonNode body = doGet("/api/v1alpha1/data-governance/preview?nodeId=alice&datatableId=" + SOURCE_DT + "&limit=5",
                ALICE_TOKEN);
        assertEquals(0, body.path("status").path("code").asInt(), body.toString());
        JsonNode data = body.path("data");
        assertEquals(202L, data.path("sourceRows").asLong());
        assertEquals(5, data.path("rows").size());
        assertEquals("id", data.path("header").get(0).asText());
        assertTrue(data.path("schema").size() >= 8);
    }

    @Test
    public void previewRejectsUnauthorized() throws Exception {
        JsonNode body = doGet("/api/v1alpha1/data-governance/preview?nodeId=alice&datatableId=" + SOURCE_DT + "&limit=5",
                CAROL_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("GOV_NO_PERMISSION"), body.toString());
    }

    /* ------------------------------- 结果展示 ------------------------------- */

    @Test
    public void viewMaskedResultReturnsRowsWithSource() throws Exception {
        JsonNode task = submitMasking(ALICE_TOKEN);
        String id = task.path("data").path("id").asText();
        assertEquals("SUCCEEDED", task.path("data").path("status").asText());
        JsonNode body = doGet("/api/v1alpha1/data-governance/tasks/results/view?taskId=" + id, ALICE_TOKEN);
        assertEquals(0, body.path("status").path("code").asInt(), body.toString());
        JsonNode data = body.path("data");
        assertTrue(data.path("masked").asBoolean(), data.toString());
        assertTrue(data.path("rows").size() > 0, data.toString());
        assertTrue(data.path("sourceName").asText().length() > 0, data.toString());
        assertEquals("phone", data.path("header").get(2).asText());
        // phone 列确已掩码（保留前3后4，含 *），且与源数据不同
        JsonNode preview = doGet("/api/v1alpha1/data-governance/preview?nodeId=alice&datatableId=" + SOURCE_DT + "&limit=1",
                ALICE_TOKEN);
        String srcPhone = preview.path("data").path("rows").get(0).get(2).asText();
        String resPhone = data.path("rows").get(0).get(2).asText();
        assertNotEquals(srcPhone, resPhone);
        assertTrue(resPhone.contains("*"), resPhone);
    }

    @Test
    public void viewSamplingResultNotExposed() throws Exception {
        // 纯抽样（未脱敏）结果：不返回行数据
        JsonNode task = submitSampling(ALICE_TOKEN);
        String id = task.path("data").path("id").asText();
        JsonNode body = doGet("/api/v1alpha1/data-governance/tasks/results/view?taskId=" + id, ALICE_TOKEN);
        assertEquals(0, body.path("status").path("code").asInt(), body.toString());
        JsonNode data = body.path("data");
        assertFalse(data.path("masked").asBoolean(), data.toString());
        assertFalse(data.has("rows"), data.toString());
        assertTrue(data.path("message").asText().contains("未经脱敏"), data.toString());
    }

    @Test
    public void viewResultRejectedForNonCreator() throws Exception {
        JsonNode task = submitMasking(ALICE_TOKEN);
        String id = task.path("data").path("id").asText();
        JsonNode body = doGet("/api/v1alpha1/data-governance/tasks/results/view?taskId=" + id, CAROL_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("GOV_NO_PERMISSION"), body.toString());
    }

    @Test
    public void viewResultRejectedWhenNotSucceeded() throws Exception {
        jdbc.update("insert into ds_governance_task(id,name,exec_mode,source_node_id,source_datatable_id,"
                        + "exec_params,status,created_by,created_at,updated_at,deleted) "
                        + "values('gt-ctrl-running','ctrl-running','BUILTIN','alice','" + SOURCE_DT + "','{}','RUNNING','alice','2026-08-19 00:00:00','2026-08-19 00:00:00',0)");
        JsonNode body = doGet("/api/v1alpha1/data-governance/tasks/results/view?taskId=gt-ctrl-running", ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("仅 SUCCEEDED"), body.toString());
    }
}
