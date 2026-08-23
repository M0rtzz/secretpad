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
import org.secretflow.secretpad.web.service.dev.DevJobExecutor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
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
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Z-06 Stage 3 Controller 测试：模型注册/两级审批/强制测试门禁/测试执行/发布 API/受控 invoke 全链路
 * （真实 token 鉴权 + X-APP-ID 凭证鉴权 + 权限/参数/状态错误码）。
 *
 * <p>mock 端口 50057 + SQLite + 临时数据目录；本地 HttpServer 模拟容器输出供测试收官与 invoke 取回。
 * 测试数据：4 行带标签 CSV（pass 二分类），结果 CSV 与输入行级 1:1 → accuracy=0.75。</p>
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
        "spring.datasource.default.jdbc-url=jdbc:sqlite:${java.io.tmpdir}/ds-model-ctrl.sqlite",
        "spring.datasource.quartz.jdbc-url=jdbc:h2:${java.io.tmpdir}/ds-model-ctrl-quartz.mv.db;DB_CLOSE_ON_EXIT=FALSE",
        "secretpad.data.dir-path=${java.io.tmpdir}/ds-model-ctrl-data/",
        "secretpad.data-sandbox.dev.input-rows=10000",
        "secretpad.data-sandbox.dev.max-retries=3",
        "secretpad.data-sandbox.dev.poll-interval-ms=3600000",
        "secretpad.data-sandbox.model.api.max-rows=1000",
})
public class ModelControllerTest {

    private static final int MOCK_PORT = 50057;
    private static final String LABELED_DT = "dt-labeled";
    private static final String LABELED_URI = "model_labeled.csv";
    private static final String DATA_ROOT = System.getProperty("java.io.tmpdir") + "/ds-model-ctrl-data";

    private static final String LABELED_CSV = "id,score,pass\n1,60,1\n2,80,1\n3,10,0\n4,20,0\n";
    private static final String CLASS_RESULT_CSV =
            "id,score,pass,prediction\n1,60,1,1\n2,80,1,1\n3,10,0,0\n4,20,0,1\n";

    private static final String ADMIN_TOKEN = "model-admin-token-0001";
    private static final String ALICE_TOKEN = "model-alice-token-0002";
    private static final String CAROL_TOKEN = "model-carol-token-0003";

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Resource
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbc;

    @Resource
    private UserTokensRepository userTokensRepository;

    @Resource
    private DynamicKusciaChannelProvider channelProvider;

    @Resource
    private DevJobExecutor devJobExecutor;

    @Resource
    private MockMvc mockMvc;

    private MockKusciaGrpcServer mockServer;
    private HttpServer containerServer;
    private int containerPort;

    private String resultCsv = CLASS_RESULT_CSV;

    /* ------------------------------- 生命周期 ------------------------------- */

    @BeforeAll
    public void startMock() throws Exception {
        Files.createDirectories(Path.of(DATA_ROOT, "alice"));
        Files.writeString(Path.of(DATA_ROOT, "alice", LABELED_URI), LABELED_CSV, StandardCharsets.UTF_8);
        containerServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        containerServer.createContext("/status", exchange -> {
            byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        containerServer.createContext("/result", exchange -> {
            byte[] body = resultCsv.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        containerServer.createContext("/log", exchange -> {
            byte[] body = "model ctrl test log".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        containerServer.start();
        containerPort = containerServer.getAddress().getPort();

        mockServer = new MockKusciaGrpcServer();
        mockServer.start(MOCK_PORT, KusciaProtocolEnum.NOTLS, List.of(
                new ModelCtrlDomainDataService(), new JobService(), new HealthService()));
        KusciaGrpcConfig config = mockServer.buildKusciaGrpcConfig("kuscia-system");
        config.setPort(MOCK_PORT);
        channelProvider.registerKuscia(config);
    }

    @AfterAll
    public void stopMock() {
        mockServer.stop();
        containerServer.stop(0);
    }

    @BeforeEach
    public void reset() {
        jdbc.update("delete from ds_model_test");
        jdbc.update("delete from ds_model_api");
        jdbc.update("delete from ds_model");
        jdbc.update("delete from ds_model_approval_history");
        jdbc.update("delete from ds_model_approval");
        jdbc.update("delete from ds_dev_run_log");
        jdbc.update("delete from ds_dev_task");
        jdbc.update("delete from ds_dev_artifact_version");
        jdbc.update("delete from ds_dev_artifact");
        jdbc.update("delete from ds_dev_dependency where created_by='it'");
        jdbc.update("delete from project_datatable where project_id in ('p1','p2')");
        jdbc.update("delete from project_node where project_id in ('p1','p2')");
        jdbc.update("delete from project where project_id in ('p1','p2')");
        jdbc.update("delete from node where node_id in ('alice','carol')");
        jdbc.update("delete from ds_alert_event where source in ('DATA_DEV','DATA_MODEL')");
        jdbc.update("delete from ds_unified_log where action like 'MODEL_%' or resource_type like 'MODEL%'");
        jdbc.update("delete from user_tokens");
        ModelCtrlDomainDataService.created.clear();
        jdbc.update("delete from user_accounts where name in ('alice','bob','carol')");
        jdbc.update("insert into user_accounts "
                + "(name,password_hash,owner_type,owner_id,display_name,account_status,is_deleted) "
                + "values ('alice','test','CENTER','alice','Alice','ENABLED',0)");
        jdbc.update("insert into user_accounts "
                + "(name,password_hash,owner_type,owner_id,display_name,account_status,is_deleted) "
                + "values ('bob','test','CENTER','alice','Bob','ENABLED',0)");
        ModelCtrlDomainDataService.createCode = KusciaAPIConstants.OK;
        ModelCtrlDomainDataService.relativeUri = LABELED_URI;
        ModelCtrlDomainDataService.datatableId = LABELED_DT;
        ModelCtrlDomainDataService.domainId = "alice";
        ModelCtrlDomainDataService.columns = ModelCtrlDomainDataService.labeledColumns();
        JobService.State.createJobCode = KusciaAPIConstants.OK;
        JobService.State.createJobMessage = "success";
        JobService.State.lastCreateJobRequest = null;
        JobService.State.jobQueryCode = KusciaAPIConstants.OK;
        JobService.State.jobState = "RUNNING";
        JobService.State.taskState = "";
        JobService.State.partyState = "";
        JobService.State.jobErrMsg = "";
        JobService.State.withEndpoints = false;
        JobService.State.endpointPortName = "jar";
        JobService.State.endpointScope = "Cluster";
        JobService.State.endpointAddress = "127.0.0.1:" + containerPort;
        JobService.State.stopJobCode = KusciaAPIConstants.OK;
        JobService.State.deleteJobCode = KusciaAPIConstants.OK;
        resultCsv = CLASS_RESULT_CSV;
        jdbc.update("insert into project(project_id,name,owner_id,is_deleted) values('p1','P1','alice',0)");
        jdbc.update("insert into project_node(project_id,node_id,is_deleted) values('p1','alice',0)");
        jdbc.update("insert into node(node_id,name,control_node_id,type,mode) values('alice','alice-node','master','normal',0)");
        jdbc.update("insert into node(node_id,name,control_node_id,type,mode) values('carol','carol-node','master','normal',0)");
        jdbc.update("insert into project_datatable(project_id,node_id,datatable_id,table_configs,source,is_deleted)"
                + " values('p1','alice','" + LABELED_DT + "','[]','IMPORTED',0)");
        saveToken(ADMIN_TOKEN, "admin", "kuscia-system", Set.of());
        saveToken(ALICE_TOKEN, "alice", "alice", Set.of("p1"));
        saveToken(CAROL_TOKEN, "carol", "carol", Set.of("p2"));
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

    public static class ModelCtrlDomainDataService extends DomainDataServiceGrpc.DomainDataServiceImplBase {
        static volatile String relativeUri = LABELED_URI;
        static volatile String datatableId = LABELED_DT;
        static volatile String domainId = "alice";
        static volatile int createCode = KusciaAPIConstants.OK;
        static final List<Domaindata.CreateDomainDataRequest> created = new CopyOnWriteArrayList<>();
        static volatile List<Common.DataColumn> columns = labeledColumns();

        static List<Common.DataColumn> labeledColumns() {
            return List.of(column("id", "int"), column("score", "int"), column("pass", "str"));
        }

        private static Common.DataColumn column(String name, String type) {
            return Common.DataColumn.newBuilder().setName(name).setType(type).build();
        }

        @Override
        public void queryDomainData(Domaindata.QueryDomainDataRequest request,
                StreamObserver<Domaindata.QueryDomainDataResponse> observer) {
            Domaindata.DomainData domainData = Domaindata.DomainData.newBuilder()
                    .setDomaindataId(datatableId)
                    .setDomainId(domainId)
                    .setAuthor(domainId)
                    .setName(datatableId)
                    .setType("table")
                    .setRelativeUri(relativeUri)
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
        MockHttpServletResponse response = mockMvc.perform(MockMvcRequestBuilders.get(url)
                        .header("User-Token", token))
                .andReturn().getResponse();
        assertEquals(200, response.getStatus());
        return objectMapper.readTree(response.getContentAsString());
    }

    private JsonNode doPost(String url, String body, String token) throws Exception {
        MockHttpServletResponse response = mockMvc.perform(MockMvcRequestBuilders.post(url)
                        .header("User-Token", token)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andReturn().getResponse();
        assertEquals(200, response.getStatus());
        return objectMapper.readTree(response.getContentAsString());
    }

    private JsonNode doPostWithHeaders(String url, String body, String token, Map<String, String> headers) throws Exception {
        var request = MockMvcRequestBuilders.post(url)
                .contentType(MediaType.APPLICATION_JSON)
                .content(body);
        if (token != null) {
            request.header("User-Token", token);
        }
        headers.forEach(request::header);
        MockHttpServletResponse response = mockMvc.perform(request).andReturn().getResponse();
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

    private byte[] validJar() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(bos)) {
            zos.putNextEntry(new ZipEntry("META-INF/MANIFEST.MF"));
            zos.write("Manifest-Version: 1.0\r\nMain-Class: com.example.Main\r\n\r\n".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
            zos.putNextEntry(new ZipEntry("com/example/Main.class"));
            zos.write(new byte[]{(byte) 0xca, (byte) 0xfe, (byte) 0xba, (byte) 0xbe});
            zos.closeEntry();
        }
        return bos.toByteArray();
    }

    /** 建 JAR 制品 + v1 版本，返回 {artifactId, artifactVersionId}。 */
    private Map<String, String> jarArtifact(String name) throws Exception {
        JsonNode art = doPost("/api/v1alpha1/data-dev/artifacts", json(Map.of(
                        "name", name, "type", "JAR", "description", "it")),
                ALICE_TOKEN);
        assertEquals(0, art.path("status").path("code").asInt(), art.toString());
        String artId = art.path("data").path("id").asText();
        MockMultipartFileForVersion file = new MockMultipartFileForVersion(validJar());
        var request = MockMvcRequestBuilders.multipart("/api/v1alpha1/data-dev/artifacts/versions/upload")
                .file(file.getFile())
                .param("artifactId", artId)
                .header("User-Token", ALICE_TOKEN);
        MockHttpServletResponse upload = mockMvc.perform(request).andReturn().getResponse();
        assertEquals(200, upload.getStatus());
        JsonNode up = objectMapper.readTree(upload.getContentAsString());
        assertEquals(0, up.path("status").path("code").asInt(), up.toString());
        return Map.of("artifactId", artId, "artifactVersionId", up.path("data").path("id").asText());
    }

    private static class MockMultipartFileForVersion {
        private final org.springframework.mock.web.MockMultipartFile file;

        MockMultipartFileForVersion(byte[] jar) {
            file = new org.springframework.mock.web.MockMultipartFile("file", "demo.jar", "application/java-archive", jar);
        }

        org.springframework.mock.web.MockMultipartFile getFile() {
            return file;
        }
    }

    private String registerModel(String name, Map<String, String> artifact) throws Exception {
        JsonNode model = doPost("/api/v1alpha1/models/register", json(Map.of(
                        "name", name, "projectId", "p1", "artifactId", artifact.get("artifactId"),
                        "artifactVersionId", artifact.get("artifactVersionId"), "description", "it")),
                ALICE_TOKEN);
        assertEquals(0, model.path("status").path("code").asInt(), model.toString());
        assertEquals("DRAFT", model.path("data").path("status").asText());
        return model.path("data").path("id").asText();
    }

    private String submitAndApproveStage1(String modelId) throws Exception {
        JsonNode approval = doPost("/api/v1alpha1/models/approvals/submit", json(Map.of("modelId", modelId, "comment", "review")), ALICE_TOKEN);
        assertEquals(0, approval.path("status").path("code").asInt(), approval.toString());
        assertEquals("MODEL_REVIEW", approval.path("data").path("status").asText());
        String approvalId = approval.path("data").path("id").asText();
        JsonNode stage1 = doPost("/api/v1alpha1/models/approvals/action", json(Map.of("id", approvalId, "action", "APPROVE", "comment", "ok")), ALICE_TOKEN);
        assertEquals(0, stage1.path("status").path("code").asInt(), stage1.toString());
        assertEquals("RESOURCE_REVIEW", stage1.path("data").path("status").asText());
        return approvalId;
    }

    private void executeTestAndFinalize(String modelId) throws Exception {
        JsonNode test = doPost("/api/v1alpha1/models/tests/execute", json(Map.of(
                        "modelId", modelId, "nodeId", "alice", "datatableId", LABELED_DT,
                        "labelColumn", "pass", "predictionColumn", "prediction", "metricType", "classification")),
                ALICE_TOKEN);
        assertEquals(0, test.path("status").path("code").asInt(), test.toString());
        assertEquals("RUNNING", test.path("data").path("status").asText());
        JobService.State.jobState = "Succeeded";
        JobService.State.withEndpoints = true;
        devJobExecutor.pollDevTasks();
    }

    private String publishAndCreateApi(String modelId) throws Exception {
        JsonNode approvals = doGet("/api/v1alpha1/models/approvals?status=APPROVED", ALICE_TOKEN);
        assertEquals(1, approvals.path("data").size(), approvals.toString());
        String approvalId = approvals.path("data").get(0).path("id").asText();
        JsonNode pub = doPost("/api/v1alpha1/models/approvals/action", json(Map.of("id", approvalId, "action", "PUBLISH", "comment", "")), ALICE_TOKEN);
        assertEquals(0, pub.path("status").path("code").asInt(), pub.toString());
        assertEquals("PUBLISHED", pub.path("data").path("status").asText());
        JsonNode api = doPost("/api/v1alpha1/model-api/create", json(Map.of(
                        "modelId", modelId, "name", "m-api", "description", "it")),
                ALICE_TOKEN);
        assertEquals(0, api.path("status").path("code").asInt(), api.toString());
        assertNotEquals("", api.path("data").path("secret").asText());
        return api.path("data").path("id").asText();
    }

    private String appIdOf(String apiId) throws Exception {
        JsonNode detail = doGet("/api/v1alpha1/model-api/detail?id=" + apiId, ALICE_TOKEN);
        return detail.path("data").path("app_id").asText();
    }

    /* ------------------------------- 用例 ------------------------------- */

    @Test
    public void fullApprovalPublishInvokeFlow() throws Exception {
        Map<String, String> jar = jarArtifact("ctrl-m-jar");
        String modelId = registerModel("ctrl-model-1", jar);
        String approvalId = submitAndApproveStage1(modelId);

        // 门禁负向：无成功测试 → MODEL_TEST_REQUIRED
        JsonNode gate = doPost("/api/v1alpha1/models/approvals/action", json(Map.of("id", approvalId, "action", "APPROVE", "comment", "")), ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), gate.path("status").path("code").asInt());
        assertTrue(gate.path("status").path("msg").asText().contains("MODEL_TEST_REQUIRED"), gate.toString());

        // 执行测试 + 容器成功 → 指标
        executeTestAndFinalize(modelId);
        JsonNode tests = doGet("/api/v1alpha1/models/tests?modelId=" + modelId, ALICE_TOKEN);
        assertEquals(1, tests.path("data").size());
        String testId = tests.path("data").get(0).path("id").asText();
        JsonNode detail = doGet("/api/v1alpha1/models/tests/detail?id=" + testId, ALICE_TOKEN);
        assertEquals("SUCCEEDED", detail.path("data").path("status").asText());
        assertEquals(0.75, detail.path("data").path("metrics").path("accuracy").asDouble(), 1e-6);
        assertEquals(0.75, detail.path("data").path("metrics").path("recall").asDouble(), 1e-6);
        JsonNode logBody = doGet("/api/v1alpha1/models/tests/log?id=" + testId + "&attempt=0", ALICE_TOKEN);
        assertEquals("model ctrl test log", logBody.path("data").path("logText").asText());

        // 门禁正向 → APPROVED
        JsonNode approved = doPost("/api/v1alpha1/models/approvals/action", json(Map.of("id", approvalId, "action", "APPROVE", "comment", "metrics ok")), ALICE_TOKEN);
        assertEquals(0, approved.path("status").path("code").asInt(), approved.toString());
        assertEquals("APPROVED", approved.path("data").path("status").asText());

        // PUBLISH → API 创建 → invoke（凭证）
        String apiId = publishAndCreateApi(modelId);
        String appId = appIdOf(apiId);
        String secret = doGet("/api/v1alpha1/model-api/detail?id=" + apiId, ALICE_TOKEN).path("data").path("secret").asText();
        assertEquals("", secret, "secret 不应回显");
        // 通过 create 返回值取 secret（一次性）
        JsonNode createdApi = doPost("/api/v1alpha1/model-api/create", json(Map.of(
                        "modelId", modelId, "name", "m-api-2")),
                ALICE_TOKEN);
        String onceSecret = createdApi.path("data").path("secret").asText();
        String onceAppId = createdApi.path("data").path("app_id").asText();
        assertTrue(onceSecret.matches("[A-Za-z0-9_-]{43}"), onceSecret);

        JobService.State.jobState = "Succeeded";
        JobService.State.withEndpoints = true;
        JsonNode invoke = doPostWithHeaders("/api/v1alpha1/model-api/invoke", json(Map.of(
                        "rows", List.of(Map.of("id", 1, "score", 60, "pass", 1), Map.of("id", 2, "score", 80, "pass", 1)))),
                null, Map.of("X-APP-ID", onceAppId, "X-APP-SECRET", onceSecret));
        assertEquals(0, invoke.path("status").path("code").asInt(), invoke.toString());
        assertEquals(4, invoke.path("data").path("resultRows").asInt());
        assertEquals("prediction", invoke.path("data").path("header").get(3).asText());
        assertTrue(invoke.path("data").path("elapsedMs").asLong() >= 0);
        // call_count 递增（invoke 走 m-api-2；m-api 未被调用保持 0）
        assertEquals(0L, jdbc.queryForObject("select call_count from ds_model_api where id=?", Long.class, apiId));
        assertEquals(1L, jdbc.queryForObject("select call_count from ds_model_api where app_id=?", Long.class, onceAppId));
        String onceApiId = createdApi.path("data").path("id").asText();

        // 复制时意外带入首尾空白仍可认证；逗号等非法字符不得被静默接受。
        JsonNode invokeWithWhitespace = doPostWithHeaders("/api/v1alpha1/model-api/invoke",
                json(Map.of("rows", List.of(Map.of("id", 1, "score", 60)))), null,
                Map.of("X-APP-ID", " " + onceAppId + " ", "X-APP-SECRET", " " + onceSecret + " "));
        assertEquals(0, invokeWithWhitespace.path("status").path("code").asInt(), invokeWithWhitespace.toString());
        JsonNode invalidFormat = doPostWithHeaders("/api/v1alpha1/model-api/invoke", json(Map.of("rows", List.of())),
                null, Map.of("X-APP-ID", onceAppId, "X-APP-SECRET", "," + onceSecret));
        assertTrue(invalidFormat.path("status").path("msg").asText().contains("model api credential"),
                invalidFormat.toString());

        // 错误凭证 → AUTH_FAILED
        JsonNode badCred = doPostWithHeaders("/api/v1alpha1/model-api/invoke", json(Map.of("rows", List.of())),
                null, Map.of("X-APP-ID", onceAppId, "X-APP-SECRET", "wrong-secret"));
        assertTrue(badCred.path("status").path("msg").asText().contains("model api credential"), badCred.toString());

        // 停用 → MODEL_API_DISABLED；启用恢复 → invoke 成功
        JsonNode disabled = doPost("/api/v1alpha1/model-api/disable", json(Map.of("id", onceApiId)), ALICE_TOKEN);
        assertEquals(0, disabled.path("status").path("code").asInt(), disabled.toString());
        assertEquals("DISABLED", disabled.path("data").path("status").asText());
        JsonNode denyDisabled = doPostWithHeaders("/api/v1alpha1/model-api/invoke", json(Map.of(
                        "rows", List.of(Map.of("id", 1, "score", 60)))),
                null, Map.of("X-APP-ID", onceAppId, "X-APP-SECRET", onceSecret));
        assertTrue(denyDisabled.path("status").path("msg").asText().contains("MODEL_API_DISABLED"), denyDisabled.toString());
        JsonNode enabled = doPost("/api/v1alpha1/model-api/enable", json(Map.of("id", onceApiId)), ALICE_TOKEN);
        assertEquals(0, enabled.path("status").path("code").asInt(), enabled.toString());
        JsonNode afterEnable = doPostWithHeaders("/api/v1alpha1/model-api/invoke", json(Map.of(
                        "rows", List.of(Map.of("id", 1, "score", 60)))),
                null, Map.of("X-APP-ID", onceAppId, "X-APP-SECRET", onceSecret));
        assertEquals(0, afterEnable.path("status").path("code").asInt(), afterEnable.toString());

        // regenerate-secret → 旧 secret 失效、新 secret 生效
        JsonNode regen = doPost("/api/v1alpha1/model-api/regenerate-secret", json(Map.of("id", onceApiId)), ALICE_TOKEN);
        assertEquals(0, regen.path("status").path("code").asInt(), regen.toString());
        String newSecret = regen.path("data").path("secret").asText();
        assertEquals(onceAppId, regen.path("data").path("app_id").asText());
        assertNotEquals(onceSecret, newSecret);
        JsonNode oldFails = doPostWithHeaders("/api/v1alpha1/model-api/invoke", json(Map.of(
                        "rows", List.of(Map.of("id", 1, "score", 60)))),
                null, Map.of("X-APP-ID", onceAppId, "X-APP-SECRET", onceSecret));
        assertTrue(oldFails.path("status").path("msg").asText().contains("model api credential"), oldFails.toString());
        JsonNode newWorks = doPostWithHeaders("/api/v1alpha1/model-api/invoke", json(Map.of(
                        "rows", List.of(Map.of("id", 1, "score", 60)))),
                null, Map.of("X-APP-ID", onceAppId, "X-APP-SECRET", newSecret));
        assertEquals(0, newWorks.path("status").path("code").asInt(), newWorks.toString());

        // 终态再审批 → MODEL_STATE_CONFLICT
        JsonNode conflict = doPost("/api/v1alpha1/models/approvals/action", json(Map.of("id", approvalId, "action", "APPROVE", "comment", "")), ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), conflict.path("status").path("code").asInt());
        assertTrue(conflict.path("status").path("msg").asText().contains("MODEL_STATE_CONFLICT"), conflict.toString());

        // 删除 API
        JsonNode deleted = doPost("/api/v1alpha1/model-api/delete", json(Map.of("id", apiId)), ALICE_TOKEN);
        assertEquals(0, deleted.path("status").path("code").asInt(), deleted.toString());
        assertEquals(1, doGet("/api/v1alpha1/model-api/list", ALICE_TOKEN).path("data").size());
    }

    @Test
    public void invokeViaUserTokenRespectsAuthorizedUsers() throws Exception {
        Map<String, String> jar = jarArtifact("ctrl-m-user");
        String modelId = registerModel("ctrl-model-user", jar);
        String approvalId = submitAndApproveStage1(modelId);
        executeTestAndFinalize(modelId);
        JsonNode approved = doPost("/api/v1alpha1/models/approvals/action", json(Map.of("id", approvalId, "action", "APPROVE", "comment", "ok")), ALICE_TOKEN);
        assertEquals(0, approved.path("status").path("code").asInt(), approved.toString());
        String apiId = publishAndCreateApi(modelId);
        String appId = appIdOf(apiId);

        JsonNode authorized = doPost("/api/v1alpha1/model-api/update", json(Map.of(
                        "id", apiId, "authorizedUsers", List.of("alice"))), ALICE_TOKEN);
        assertEquals(0, authorized.path("status").path("code").asInt(), authorized.toString());

        // 授权 alice → User-Token 调用者放行
        JobService.State.jobState = "Succeeded";
        JobService.State.withEndpoints = true;
        JsonNode ok = doPost("/api/v1alpha1/model-api/invoke", json(Map.of(
                        "appId", appId, "rows", List.of(Map.of("id", 1, "score", 60, "pass", 1)))),
                ALICE_TOKEN);
        assertEquals(0, ok.path("status").path("code").asInt(), ok.toString());
        // 更新授权用户 ["bob"] → alice 被拒
        JsonNode updated = doPost("/api/v1alpha1/model-api/update", json(Map.of(
                        "id", apiId, "authorizedUsers", List.of("bob"))),
                ALICE_TOKEN);
        assertEquals(0, updated.path("status").path("code").asInt(), updated.toString());
        jdbc.update("update user_accounts set account_status='DISABLED' where name='bob'");
        JsonNode retained = doPost("/api/v1alpha1/model-api/update", json(Map.of(
                        "id", apiId, "authorizedUsers", List.of("bob"),
                        "description", "retain disabled grant")),
                ALICE_TOKEN);
        assertEquals(0, retained.path("status").path("code").asInt(), retained.toString());
        JsonNode denied = doPost("/api/v1alpha1/model-api/invoke", json(Map.of(
                        "appId", appId, "rows", List.of(Map.of("id", 1, "score", 60)))),
                ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), denied.path("status").path("code").asInt());
        assertTrue(denied.path("status").path("msg").asText().contains("MODEL_API_USER_DENIED"), denied.toString());
        // 凭证调用者不受授权用户名单约束
        JsonNode createdApi = doPost("/api/v1alpha1/model-api/create", json(Map.of("modelId", modelId, "name", "m-user-2")), ALICE_TOKEN);
        String onceAppId = createdApi.path("data").path("app_id").asText();
        String onceSecret = createdApi.path("data").path("secret").asText();
        JobService.State.jobState = "Succeeded";
        JobService.State.withEndpoints = true;
        JsonNode viaCred = doPostWithHeaders("/api/v1alpha1/model-api/invoke", json(Map.of(
                        "rows", List.of(Map.of("id", 1, "score", 60)))),
                null, Map.of("X-APP-ID", onceAppId, "X-APP-SECRET", onceSecret));
        assertEquals(0, viaCred.path("status").path("code").asInt(), viaCred.toString());
    }

    @Test
    public void invokeExpiredApiDenied() throws Exception {
        Map<String, String> jar = jarArtifact("ctrl-m-exp");
        String modelId = registerModel("ctrl-model-exp", jar);
        String approvalId = submitAndApproveStage1(modelId);
        executeTestAndFinalize(modelId);
        JsonNode approved = doPost("/api/v1alpha1/models/approvals/action", json(Map.of("id", approvalId, "action", "APPROVE", "comment", "ok")), ALICE_TOKEN);
        assertEquals(0, approved.path("status").path("code").asInt(), approved.toString());
        String apiId = publishAndCreateApi(modelId);
        String appId = appIdOf(apiId);
        // 过期窗口
        JsonNode updated = doPost("/api/v1alpha1/model-api/update", json(Map.of(
                        "id", apiId, "validTo", "2020-01-01 00:00:00")),
                ALICE_TOKEN);
        assertEquals(0, updated.path("status").path("code").asInt(), updated.toString());
        JsonNode denied = doPost("/api/v1alpha1/model-api/invoke", json(Map.of(
                        "appId", appId, "rows", List.of(Map.of("id", 1, "score", 60)))),
                ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), denied.path("status").path("code").asInt());
        assertTrue(denied.path("status").path("msg").asText().contains("MODEL_API_EXPIRED"), denied.toString());
    }

    @Test
    public void modelPermissionAndValidationThroughController() throws Exception {
        Map<String, String> jar = jarArtifact("ctrl-m-perm");
        String modelId = registerModel("ctrl-model-perm", jar);
        String approvalId = submitAndApproveStage1(modelId);

        // carol 不能审批
        JsonNode hijack = doPost("/api/v1alpha1/models/approvals/action", json(Map.of("id", approvalId, "action", "APPROVE", "comment", "")), CAROL_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), hijack.path("status").path("code").asInt());
        assertTrue(hijack.path("status").path("msg").asText().contains("MODEL_NO_PERMISSION"), hijack.toString());
        // carol 不能删除
        JsonNode del = doPost("/api/v1alpha1/models/delete", json(Map.of("id", modelId)), CAROL_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), del.path("status").path("code").asInt());
        assertTrue(del.path("status").path("msg").asText().contains("MODEL_NO_PERMISSION"), del.toString());
        // carol 不能执行测试
        JsonNode testDenied = doPost("/api/v1alpha1/models/tests/execute", json(Map.of(
                        "modelId", modelId, "nodeId", "alice", "datatableId", LABELED_DT,
                        "labelColumn", "pass", "predictionColumn", "prediction")),
                CAROL_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), testDenied.path("status").path("code").asInt());
        assertTrue(testDenied.path("status").path("msg").asText().contains("MODEL_NO_PERMISSION"), testDenied.toString());

        // 缺 artifactId → MODEL_PARAM_INVALID
        JsonNode missing = doPost("/api/v1alpha1/models/register", json(Map.of("name", "m", "projectId", "p1", "artifactVersionId", "x")), ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), missing.path("status").path("code").asInt());
        assertTrue(missing.path("status").path("msg").asText().contains("MODEL_PARAM_INVALID"), missing.toString());

        // SQL 制品注册拒绝
        JsonNode sqlArt = doPost("/api/v1alpha1/data-dev/artifacts", json(Map.of("name", "ctrl-sql-model", "type", "SQL")), ALICE_TOKEN);
        assertEquals(0, sqlArt.path("status").path("code").asInt(), sqlArt.toString());
        String sqlArtId = sqlArt.path("data").path("id").asText();
        JsonNode sqlVersion = doPost("/api/v1alpha1/data-dev/artifacts/versions", json(Map.of(
                        "artifactId", sqlArtId, "contentText", "SELECT 1", "dependencyNames", List.of())),
                ALICE_TOKEN);
        assertEquals(0, sqlVersion.path("status").path("code").asInt(), sqlVersion.toString());
        JsonNode sqlModel = doPost("/api/v1alpha1/models/register", json(Map.of(
                        "name", "m-sql", "projectId", "p1", "artifactId", sqlArtId,
                        "artifactVersionId", sqlVersion.path("data").path("id").asText())),
                ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), sqlModel.path("status").path("code").asInt());
        assertTrue(sqlModel.path("status").path("msg").asText().contains("MODEL_PARAM_INVALID"), sqlModel.toString());

        // 同项目同制品重复注册 → MODEL_ALREADY_EXISTS
        JsonNode dup = doPost("/api/v1alpha1/models/register", json(Map.of(
                        "name", "ctrl-model-perm-dup", "projectId", "p1", "artifactId", jar.get("artifactId"),
                        "artifactVersionId", jar.get("artifactVersionId"))),
                ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), dup.path("status").path("code").asInt());
        assertTrue(dup.path("status").path("msg").asText().contains("MODEL_ALREADY_EXISTS"), dup.toString());
    }

    @Test
    public void rejectAndResubmitThroughController() throws Exception {
        Map<String, String> jar = jarArtifact("ctrl-m-rs");
        String modelId = registerModel("ctrl-model-rs", jar);
        String approvalId = submitAndApproveStage1(modelId);
        JsonNode rejected = doPost("/api/v1alpha1/models/approvals/action", json(Map.of("id", approvalId, "action", "REJECT", "comment", "no")), ALICE_TOKEN);
        assertEquals(0, rejected.path("status").path("code").asInt(), rejected.toString());
        assertEquals("REJECTED", rejected.path("data").path("status").asText());
        assertEquals("REJECTED", doGet("/api/v1alpha1/models/detail?id=" + modelId, ALICE_TOKEN).path("data").path("status").asText());
        JsonNode resubmit = doPost("/api/v1alpha1/models/approvals/action", json(Map.of("id", approvalId, "action", "RESUBMIT", "comment", "retry")), ALICE_TOKEN);
        assertEquals(0, resubmit.path("status").path("code").asInt(), resubmit.toString());
        assertEquals("MODEL_REVIEW", resubmit.path("data").path("status").asText());
        assertEquals(2, resubmit.path("data").path("version").asInt());
        // 审批历史
        JsonNode history = doGet("/api/v1alpha1/models/approvals/history?id=" + approvalId, ALICE_TOKEN);
        assertTrue(history.path("data").size() >= 3, history.toString());
    }
}
