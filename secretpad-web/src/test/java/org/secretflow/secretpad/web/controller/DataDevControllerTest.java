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
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Z-05 Stage 3 Controller 测试：计算任务开发 API 全端点（制品/版本/JAR 上传/依赖/任务提交操作/预览/
 * 结果/日志/挂载），真实 token 鉴权 + 权限门禁 + 参数/状态错误码。
 *
 * <p>独立 mock 端口 50056 + SQLite + 临时数据目录；mock DomainDataService 提供源表元数据并记录
 * createDomainData。JAR/PYTHON 运行取回流程由 Stage 2 DataDevCustomIT 覆盖，此处仅验契约。</p>
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
        "spring.datasource.default.jdbc-url=jdbc:sqlite:${java.io.tmpdir}/ds-dev-ctrl.sqlite",
        "spring.datasource.quartz.jdbc-url=jdbc:h2:${java.io.tmpdir}/ds-dev-ctrl-quartz.mv.db;DB_CLOSE_ON_EXIT=FALSE",
        "secretpad.data.dir-path=${java.io.tmpdir}/ds-dev-ctrl-data/",
        "secretpad.data-sandbox.dev.input-rows=10000",
        "secretpad.data-sandbox.dev.max-retries=3",
        "secretpad.data-sandbox.dev.sql-limit=50",
})
public class DataDevControllerTest {

    private static final int MOCK_PORT = 50056;
    private static final String SOURCE_DT = "dt-sample";
    private static final String SOURCE_URI = "sample_full.csv";
    private static final String DATA_ROOT = System.getProperty("java.io.tmpdir") + "/ds-dev-ctrl-data";

    private static final String ADMIN_TOKEN = "dev-admin-token-0001";
    private static final String ALICE_TOKEN = "dev-alice-token-0002";
    private static final String CAROL_TOKEN = "dev-carol-token-0003";

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Resource
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbc;

    @Resource
    private UserTokensRepository userTokensRepository;

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
        jdbc.update("delete from ds_dev_run_log");
        jdbc.update("delete from ds_dev_task");
        jdbc.update("delete from ds_dev_artifact_version");
        jdbc.update("delete from ds_dev_artifact");
        jdbc.update("delete from ds_dev_dependency where created_by='it'");
        jdbc.update("delete from project_datatable where project_id in ('p1','p2')");
        jdbc.update("delete from node where node_id in ('alice','carol')");
        jdbc.update("delete from ds_alert_event where source='DATA_DEV'");
        jdbc.update("delete from ds_unified_log where resource_type='DEV_TASK' or resource_type='DEV_ARTIFACT'"
                + " or resource_type='DEV_ARTIFACT_VERSION' or resource_type='DEV_DEPENDENCY' or action like 'DEV_%'");
        // TokensDO 走 @SQLDelete 软删除，同 PK 再插入会冲突，用 JDBC 硬删后重插
        jdbc.update("delete from user_tokens");
        CtrlDomainDataService.created.clear();
        CtrlDomainDataService.createCode = KusciaAPIConstants.OK;
        CtrlDomainDataService.relativeUri = SOURCE_URI;
        CtrlDomainDataService.datatableId = SOURCE_DT;
        CtrlDomainDataService.domainId = "alice";
        CtrlDomainDataService.columns = CtrlDomainDataService.defaultColumns();
        // 权限基础数据
        jdbc.update("insert into node(node_id,name,control_node_id,type,mode) values('alice','alice-node','master','normal',0)");
        jdbc.update("insert into node(node_id,name,control_node_id,type,mode) values('carol','carol-node','master','normal',0)");
        jdbc.update("insert into project_datatable(project_id,node_id,datatable_id,table_configs,source,is_deleted) values('p1','alice','" + SOURCE_DT + "','[]','IMPORTED',0)");
        saveToken(ADMIN_TOKEN, "admin", "kuscia-system", Set.of());
        saveToken(ALICE_TOKEN, "alice", "alice", Set.of("p1"));
        saveToken(CAROL_TOKEN, "carol", "carol", Set.of("p2"));
        UserContext.setBaseUser(UserContextDTO.builder().ownerId("alice").name("alice")
                .platformType(PlatformTypeEnum.CENTER).platformNodeId("alice")
                .ownerType(UserOwnerTypeEnum.CENTER).projectIds(Set.of("p1")).build());
        JobService.State.createJobCode = KusciaAPIConstants.OK;
        JobService.State.lastCreateJobRequest = null;
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

    /** 富数据 DomainData mock：query 返回真实元数据（relativeUri + schema），create 记录调用。 */
    public static class CtrlDomainDataService extends DomainDataServiceGrpc.DomainDataServiceImplBase {
        static volatile String relativeUri = SOURCE_URI;
        static volatile String datatableId = SOURCE_DT;
        static volatile String domainId = "alice";
        static volatile int createCode = KusciaAPIConstants.OK;
        static final List<Domaindata.CreateDomainDataRequest> created = new CopyOnWriteArrayList<>();
        static volatile List<Common.DataColumn> columns = defaultColumns();

        static List<Common.DataColumn> defaultColumns() {
            return List.of(
                    column("id", "int"),
                    column("name", "str"),
                    column("phone", "str"),
                    column("id_card", "str"),
                    column("category", "str"),
                    column("amount", "float"),
                    column("score", "int"),
                    column("memo", "str"));
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

    private String createArtifact(String name, String type, String token) throws Exception {
        JsonNode created = doPost("/api/v1alpha1/data-dev/artifacts", json(Map.of(
                        "name", name, "type", type, "description", "ctrl")),
                token);
        assertEquals(0, created.path("status").path("code").asInt(), created.toString());
        return created.path("data").path("id").asText();
    }

    private JsonNode submitSql(String name, String runMode, String sql, String token) throws Exception {
        return doPost("/api/v1alpha1/data-dev/tasks/submit", json(Map.of(
                        "name", name,
                        "runMode", runMode,
                        "execType", "SQL",
                        "nodeId", "alice",
                        "datatableId", SOURCE_DT,
                        "sql", sql)),
                token);
    }

    /* ------------------------------- 制品 / 版本 ------------------------------- */

    @Test
    public void artifactCrudThroughController() throws Exception {
        String id = createArtifact("ctrl-sql-art", "SQL", ALICE_TOKEN);
        JsonNode detail = doGet("/api/v1alpha1/data-dev/artifacts/detail?id=" + id, ALICE_TOKEN);
        assertEquals("SQL", detail.path("data").path("type").asText());
        assertEquals(0, detail.path("data").path("latest_version").asInt());
        // 同名拒绝（DEV_STATE_CONFLICT）
        JsonNode dup = doPost("/api/v1alpha1/data-dev/artifacts", json(Map.of("name", "ctrl-sql-art", "type", "SQL")), ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), dup.path("status").path("code").asInt());
        assertTrue(dup.path("status").path("msg").asText().contains("已存在"), dup.toString());
        // 非法 type
        JsonNode bad = doPost("/api/v1alpha1/data-dev/artifacts", json(Map.of("name", "ctrl-bad", "type", "EXE")), ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), bad.path("status").path("code").asInt());
        assertTrue(bad.path("status").path("msg").asText().contains("type"), bad.toString());
        // 列表（type 过滤）
        JsonNode list = doGet("/api/v1alpha1/data-dev/artifacts?type=SQL", ALICE_TOKEN);
        assertEquals(1, list.path("data").size());
        // 更新（仅创建人）
        JsonNode updated = doPost("/api/v1alpha1/data-dev/artifacts/update", json(Map.of("id", id, "description", "updated")), ALICE_TOKEN);
        assertEquals(0, updated.path("status").path("code").asInt());
        // 非创建人更新 → 拒绝
        JsonNode hijack = doPost("/api/v1alpha1/data-dev/artifacts/update", json(Map.of("id", id, "description", "hijack")), CAROL_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), hijack.path("status").path("code").asInt());
        assertTrue(hijack.path("status").path("msg").asText().contains("创建人"), hijack.toString());
        // 软删
        JsonNode deleted = doPost("/api/v1alpha1/data-dev/artifacts/delete", json(Map.of("id", id)), ALICE_TOKEN);
        assertEquals(0, deleted.path("status").path("code").asInt());
        assertEquals(0, doGet("/api/v1alpha1/data-dev/artifacts?type=SQL", ALICE_TOKEN).path("data").size());
    }

    @Test
    public void createVersionAutoIncrement() throws Exception {
        String id = createArtifact("ctrl-sql-v", "SQL", ALICE_TOKEN);
        JsonNode v1 = doPost("/api/v1alpha1/data-dev/artifacts/versions", json(Map.of(
                        "artifactId", id, "contentText", "SELECT 1")),
                ALICE_TOKEN);
        assertEquals(0, v1.path("status").path("code").asInt(), v1.toString());
        assertEquals(1, v1.path("data").path("version").asInt());
        JsonNode v2 = doPost("/api/v1alpha1/data-dev/artifacts/versions", json(Map.of(
                        "artifactId", id, "contentText", "SELECT 2",
                        "paramsSchema", "[{\"name\":\"x\",\"type\":\"string\"}]",
                        "defaultParams", "{\"x\":\"1\"}")),
                ALICE_TOKEN);
        assertEquals(2, v2.path("data").path("version").asInt());
        String versionId = v2.path("data").path("id").asText();
        JsonNode versionDetail = doGet("/api/v1alpha1/data-dev/artifacts/versions/detail?versionId=" + versionId, ALICE_TOKEN);
        assertEquals("SELECT 2", versionDetail.path("data").path("content_text").asText());
        // 列表 2 个版本
        JsonNode versions = doGet("/api/v1alpha1/data-dev/artifacts/versions?artifactId=" + id, ALICE_TOKEN);
        assertEquals(2, versions.path("data").size());
        // 删版本回退 latest_version
        doPost("/api/v1alpha1/data-dev/artifacts/versions/delete", json(Map.of("id", versionId)), ALICE_TOKEN);
        JsonNode after = doGet("/api/v1alpha1/data-dev/artifacts/detail?id=" + id, ALICE_TOKEN);
        assertEquals(1, after.path("data").path("latest_version").asInt());
    }

    @Test
    public void pythonDependencyRejectedThroughController() throws Exception {
        String id = createArtifact("ctrl-py", "PYTHON", ALICE_TOKEN);
        JsonNode bad = doPost("/api/v1alpha1/data-dev/artifacts/versions", json(Map.of(
                        "artifactId", id, "contentText", "import requests\nprint(1)", "dependencyNames", List.of())),
                ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), bad.path("status").path("code").asInt());
        assertTrue(bad.path("status").path("msg").asText().contains("DEV_DEPENDENCY_REJECTED"), bad.toString());
        // 白名单内放行
        JsonNode ok = doPost("/api/v1alpha1/data-dev/artifacts/versions", json(Map.of(
                        "artifactId", id, "contentText", "import numpy as np\nprint(np.array([1]))", "dependencyNames", List.of("numpy"))),
                ALICE_TOKEN);
        assertEquals(0, ok.path("status").path("code").asInt(), ok.toString());
        assertEquals(1, ok.path("data").path("version").asInt());
    }

    /* ------------------------------- JAR 上传 ------------------------------- */

    @Test
    public void jarUploadDownloadThroughController() throws Exception {
        String id = createArtifact("ctrl-jar", "JAR", ALICE_TOKEN);
        byte[] jar = validJar();
        MockMultipartFile file = new MockMultipartFile("file", "demo.jar", "application/java-archive", jar);
        MockHttpServletResponse upload = mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1alpha1/data-dev/artifacts/versions/upload")
                        .file(file)
                        .param("artifactId", id)
                        .param("paramsSchema", "[{\"name\":\"filter\",\"type\":\"string\"}]")
                        .header("User-Token", ALICE_TOKEN))
                .andReturn().getResponse();
        assertEquals(200, upload.getStatus());
        JsonNode body = objectMapper.readTree(upload.getContentAsString());
        assertEquals(0, body.path("status").path("code").asInt(), body.toString());
        assertEquals(1, body.path("data").path("version").asInt());
        assertEquals(64, body.path("data").path("sha256").asText().length());
        String versionId = body.path("data").path("id").asText();
        // 下载回读一致
        MockHttpServletResponse download = mockMvc.perform(MockMvcRequestBuilders.get("/api/v1alpha1/data-dev/artifacts/versions/download?versionId=" + versionId)
                        .header("User-Token", ALICE_TOKEN))
                .andReturn().getResponse();
        assertEquals(200, download.getStatus());
        assertArrayEquals(jar, download.getContentAsByteArray());
    }

    @Test
    public void jarUploadRejectsNonZip() throws Exception {
        String id = createArtifact("ctrl-jar2", "JAR", ALICE_TOKEN);
        MockMultipartFile file = new MockMultipartFile("file", "bad.jar", "application/java-archive", "not a jar".getBytes(StandardCharsets.UTF_8));
        MockHttpServletResponse upload = mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1alpha1/data-dev/artifacts/versions/upload")
                        .file(file)
                        .param("artifactId", id)
                        .header("User-Token", ALICE_TOKEN))
                .andReturn().getResponse();
        assertEquals(200, upload.getStatus());
        JsonNode body = objectMapper.readTree(upload.getContentAsString());
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("魔数不符"), body.toString());
    }

    @Test
    public void jarUploadToSqlArtifactRejected() throws Exception {
        String id = createArtifact("ctrl-sql3", "SQL", ALICE_TOKEN);
        MockMultipartFile file = new MockMultipartFile("file", "demo.jar", "application/java-archive", validJar());
        MockHttpServletResponse upload = mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1alpha1/data-dev/artifacts/versions/upload")
                        .file(file)
                        .param("artifactId", id)
                        .header("User-Token", ALICE_TOKEN))
                .andReturn().getResponse();
        JsonNode body = objectMapper.readTree(upload.getContentAsString());
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("JAR"), body.toString());
    }

    /* ------------------------------- 依赖白名单 ------------------------------- */

    @Test
    public void dependencyCrudThroughController() throws Exception {
        JsonNode created = doPost("/api/v1alpha1/data-dev/dependencies", json(Map.of(
                        "name", "scipy", "versionSpec", ">=1.10", "description", "SciPy")),
                ALICE_TOKEN);
        assertEquals(0, created.path("status").path("code").asInt(), created.toString());
        String depId = created.path("data").path("id").asText();
        assertTrue(depId.startsWith("dep-"), depId);
        // 列表含预置 numpy/pandas + 新建 scipy
        JsonNode list = doGet("/api/v1alpha1/data-dev/dependencies?enabled=1", ALICE_TOKEN);
        assertTrue(list.path("data").size() >= 3, list.toString());
        // 更新 enabled 开关
        JsonNode updated = doPost("/api/v1alpha1/data-dev/dependencies/update", json(Map.of("id", depId, "enabled", false)), ALICE_TOKEN);
        assertEquals(0, updated.path("status").path("code").asInt());
        assertFalse(doGet("/api/v1alpha1/data-dev/dependencies?enabled=1", ALICE_TOKEN).path("data").toString()
                .contains("\"id\":\"" + depId + "\""));
        // 删除
        doPost("/api/v1alpha1/data-dev/dependencies/delete", json(Map.of("id", depId)), ALICE_TOKEN);
        JsonNode after = doGet("/api/v1alpha1/data-dev/dependencies?keyword=scipy", ALICE_TOKEN);
        assertEquals(0, after.path("data").size());
    }

    /* ------------------------------- 任务 ------------------------------- */

    @Test
    public void submitSqlDevSuccessPreviewAndLog() throws Exception {
        JsonNode task = submitSql("ctrl-sql-dev", "DEV",
                "SELECT category, count(*) c FROM src GROUP BY category ORDER BY category", ALICE_TOKEN);
        assertEquals(0, task.path("status").path("code").asInt(), task.toString());
        JsonNode data = task.path("data");
        assertEquals("SUCCEEDED", data.path("status").asText());
        assertEquals("DEV", data.path("run_mode").asText());
        assertTrue(data.path("result_preview").asText().contains("\"rows\""));
        // 调试日志（attempt=0 首次运行）
        JsonNode logBody = doGet("/api/v1alpha1/data-dev/tasks/log?taskId=" + data.path("id").asText() + "&attempt=0",
                ALICE_TOKEN);
        assertEquals(0, logBody.path("data").path("attempt").asInt(), logBody.toString());
        assertTrue(logBody.path("data").path("logText").asText().contains("query_only"), logBody.toString());
        // 结果预览端点
        JsonNode result = doGet("/api/v1alpha1/data-dev/tasks/results/view?taskId=" + data.path("id").asText(), ALICE_TOKEN);
        assertEquals("DEV", result.path("data").path("runMode").asText());
    }

    @Test
    public void submitSqlProdRegistersAndMount() throws Exception {
        JsonNode task = submitSql("ctrl-sql-prod", "PROD", "SELECT * FROM src WHERE score >= 60", ALICE_TOKEN);
        assertEquals(0, task.path("status").path("code").asInt(), task.toString());
        JsonNode data = task.path("data");
        assertEquals("SUCCEEDED", data.path("status").asText());
        assertNotEquals("", data.path("result_datatable_id").asText());
        assertEquals(1, CtrlDomainDataService.created.size());
        // 结果列表
        JsonNode results = doGet("/api/v1alpha1/data-dev/tasks/results?nodeId=alice", ALICE_TOKEN);
        assertEquals(1, results.path("data").size());
        // 挂载项目
        JsonNode mount = doPost("/api/v1alpha1/data-dev/tasks/mount", json(Map.of(
                        "taskId", data.path("id").asText(), "projectId", "p1")),
                ALICE_TOKEN);
        assertEquals(0, mount.path("status").path("code").asInt(), mount.toString());
        // 重复挂载 → 冲突
        JsonNode mountAgain = doPost("/api/v1alpha1/data-dev/tasks/mount", json(Map.of(
                        "taskId", data.path("id").asText(), "projectId", "p1")),
                ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), mountAgain.path("status").path("code").asInt());
        assertTrue(mountAgain.path("status").path("msg").asText().contains("已挂载"), mountAgain.toString());
    }

    @Test
    public void submitJarRunsAndCancels() throws Exception {
        String artId = createArtifact("ctrl-jar4", "JAR", ALICE_TOKEN);
        MockMultipartFile file = new MockMultipartFile("file", "demo.jar", "application/java-archive", validJar());
        MockHttpServletResponse upload = mockMvc.perform(MockMvcRequestBuilders.multipart("/api/v1alpha1/data-dev/artifacts/versions/upload")
                        .file(file)
                        .param("artifactId", artId)
                        .header("User-Token", ALICE_TOKEN))
                .andReturn().getResponse();
        JsonNode up = objectMapper.readTree(upload.getContentAsString());
        assertEquals(0, up.path("status").path("code").asInt(), up.toString());
        JsonNode task = doPost("/api/v1alpha1/data-dev/tasks/submit", json(Map.of(
                        "name", "ctrl-jar-run",
                        "runMode", "PROD",
                        "execType", "JAR",
                        "nodeId", "alice",
                        "datatableId", SOURCE_DT,
                        "artifactId", artId,
                        "version", 1)),
                ALICE_TOKEN);
        assertEquals(0, task.path("status").path("code").asInt(), task.toString());
        JsonNode data = task.path("data");
        assertEquals("RUNNING", data.path("status").asText());
        String taskId = data.path("id").asText();
        assertTrue(data.path("kuscia_job_id").asText().equals("dt-" + taskId), data.toString());
        // 取消 RUNNING → CANCELLED
        JsonNode cancel = doPost("/api/v1alpha1/data-dev/tasks/cancel", json(Map.of("id", taskId)), ALICE_TOKEN);
        assertEquals(0, cancel.path("status").path("code").asInt(), cancel.toString());
        JsonNode detail = doGet("/api/v1alpha1/data-dev/tasks/detail?id=" + taskId, ALICE_TOKEN);
        assertEquals("CANCELLED", detail.path("data").path("status").asText());
    }

    @Test
    public void submitRejectedWithoutPermission() throws Exception {
        JsonNode body = submitSql("ctrl-noperm", "DEV", "SELECT 1", CAROL_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("DEV_NO_PERMISSION"), body.toString());
        assertEquals(0L, jdbc.queryForObject("select count(1) from ds_dev_task", Long.class));
    }

    @Test
    public void submitMissingParamRejected() throws Exception {
        JsonNode body = doPost("/api/v1alpha1/data-dev/tasks/submit", json(Map.of(
                        "execType", "SQL", "datatableId", SOURCE_DT, "sql", "SELECT 1")),
                ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("nodeId"), body.toString());
    }

    @Test
    public void submitUnknownExecTypeRejected() throws Exception {
        JsonNode body = doPost("/api/v1alpha1/data-dev/tasks/submit", json(Map.of(
                        "runMode", "DEV", "execType", "FANCY", "nodeId", "alice", "datatableId", SOURCE_DT)),
                ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("execType"), body.toString());
    }

    @Test
    public void cancelSucceededTaskRejected() throws Exception {
        JsonNode task = submitSql("ctrl-cancel", "DEV", "SELECT count(*) c FROM src", ALICE_TOKEN);
        String id = task.path("data").path("id").asText();
        JsonNode body = doPost("/api/v1alpha1/data-dev/tasks/cancel", json(Map.of("id", id)), ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("不可取消"), body.toString());
    }

    @Test
    public void retryNonFailedRejected() throws Exception {
        JsonNode task = submitSql("ctrl-retry", "DEV", "SELECT count(*) c FROM src", ALICE_TOKEN);
        String id = task.path("data").path("id").asText();
        JsonNode body = doPost("/api/v1alpha1/data-dev/tasks/retry", json(Map.of("id", id)), ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("仅 FAILED"), body.toString());
    }

    /* ------------------------------- 预览 / 结果权限 ------------------------------- */

    @Test
    public void previewReturnsLimitedRows() throws Exception {
        JsonNode body = doGet("/api/v1alpha1/data-dev/tasks/preview-source?nodeId=alice&datatableId=" + SOURCE_DT + "&limit=5",
                ALICE_TOKEN);
        assertEquals(0, body.path("status").path("code").asInt(), body.toString());
        JsonNode data = body.path("data");
        assertEquals(202L, data.path("sourceRows").asLong());
        assertEquals(5, data.path("rows").size());
        assertEquals("id", data.path("header").get(0).asText());
    }

    @Test
    public void previewRejectsUnauthorized() throws Exception {
        JsonNode body = doGet("/api/v1alpha1/data-dev/tasks/preview-source?nodeId=alice&datatableId=" + SOURCE_DT + "&limit=5",
                CAROL_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("DEV_NO_PERMISSION"), body.toString());
    }

    @Test
    public void viewResultRejectedForNonCreator() throws Exception {
        JsonNode task = submitSql("ctrl-view", "DEV", "SELECT count(*) c FROM src", ALICE_TOKEN);
        String id = task.path("data").path("id").asText();
        JsonNode body = doGet("/api/v1alpha1/data-dev/tasks/results/view?taskId=" + id, CAROL_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("DEV_NO_PERMISSION"), body.toString());
    }

    @Test
    public void mountRequiresSucceededResult() throws Exception {
        JsonNode body = doPost("/api/v1alpha1/data-dev/tasks/mount", json(Map.of(
                        "taskId", "no-such-task", "projectId", "p1")),
                ALICE_TOKEN);
        assertEquals(SystemErrorCode.UNKNOWN_ERROR.getCode(), body.path("status").path("code").asInt());
        assertTrue(body.path("status").path("msg").asText().contains("DEV_NOT_FOUND"), body.toString());
    }
}
