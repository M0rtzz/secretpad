/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.service.dev;

import org.secretflow.secretpad.common.dto.UserContextDTO;
import org.secretflow.secretpad.common.enums.PlatformTypeEnum;
import org.secretflow.secretpad.common.enums.UserOwnerTypeEnum;
import org.secretflow.secretpad.common.util.UserContext;
import org.secretflow.secretpad.kuscia.v1alpha1.DynamicKusciaChannelProvider;
import org.secretflow.secretpad.kuscia.v1alpha1.constant.KusciaAPIConstants;
import org.secretflow.secretpad.kuscia.v1alpha1.constant.KusciaProtocolEnum;
import org.secretflow.secretpad.kuscia.v1alpha1.mock.MockKusciaGrpcServer;
import org.secretflow.secretpad.kuscia.v1alpha1.mock.service.HealthService;
import org.secretflow.secretpad.kuscia.v1alpha1.mock.service.JobService;
import org.secretflow.secretpad.kuscia.v1alpha1.model.KusciaGrpcConfig;
import org.secretflow.secretpad.web.SecretPadApplication;
import org.secretflow.secretpad.web.service.governance.CsvUtil;

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
import org.secretflow.v1alpha1.kusciaapi.Job;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Z-05 Stage 1 集成测试：制品/版本/依赖 CRUD + SQL 进程内 DEV/PROD + JAR/PYTHON 提交 + 权限/状态冲突。
 *
 * <p>独立 mock 端口 50055 + SQLite + 临时数据目录；mock DomainDataService 提供真实元数据
 * （relativeUri + schema）并记录 createDomainData 调用。JAR/PYTHON 提交断言 createJob payload 形状
 * （AppImage / task_input_config），运行/取回在 Stage 2 DataDevCustomIT 覆盖。@Scheduled 不运行。</p>
 */
@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
@ActiveProfiles("test")
@SpringBootTest(classes = SecretPadApplication.class)
@TestPropertySource(properties = {
        "kuscia.nodes=",
        "secretpad.data-sandbox.kuscia.enabled=true",
        "secretpad.node-id=kuscia-system",
        "spring.datasource.default.jdbc-url=jdbc:sqlite:${java.io.tmpdir}/ds-dev-it.sqlite",
        "spring.datasource.quartz.jdbc-url=jdbc:h2:${java.io.tmpdir}/ds-dev-it-quartz.mv.db;DB_CLOSE_ON_EXIT=FALSE",
        "secretpad.data.dir-path=${java.io.tmpdir}/ds-dev-data/",
        "secretpad.data-sandbox.dev.input-rows=10000",
        "secretpad.data-sandbox.dev.max-retries=3",
        "secretpad.data-sandbox.dev.sql-limit=50",
        "secretpad.data-sandbox.dev.result-preview-rows=10",
})
public class DataDevIT {

    private static final int MOCK_PORT = 50055;
    private static final String SOURCE_DT = "dt-sample";
    private static final String SOURCE_URI = "sample_full.csv";
    private static final String DATA_ROOT = System.getProperty("java.io.tmpdir") + "/ds-dev-data";

    @Resource
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbc;

    @Resource
    private DataDevService dataDev;

    @Resource
    private DynamicKusciaChannelProvider channelProvider;

    private MockKusciaGrpcServer mockServer;

    /* ------------------------------- 角色 ------------------------------- */

    private UserContextDTO alice() {
        return UserContextDTO.builder().ownerId("alice").name("alice")
                .platformType(PlatformTypeEnum.CENTER).platformNodeId("alice")
                .ownerType(UserOwnerTypeEnum.CENTER).projectIds(Set.of("p1")).build();
    }

    private UserContextDTO carol() {
        return UserContextDTO.builder().ownerId("carol").name("carol")
                .platformType(PlatformTypeEnum.CENTER).platformNodeId("carol")
                .ownerType(UserOwnerTypeEnum.CENTER).projectIds(Set.of("p2")).build();
    }

    /* ------------------------------- 生命周期 ------------------------------- */

    @BeforeAll
    public void startMock() throws Exception {
        byte[] csvBytes;
        try (InputStream in = getClass().getResourceAsStream("/gov/sample_full.csv")) {
            assertNotNull(in, "test resource gov/sample_full.csv missing");
            csvBytes = in.readAllBytes();
        }
        Files.createDirectories(Path.of(DATA_ROOT, "alice"));
        Files.write(Path.of(DATA_ROOT, "alice", SOURCE_URI), csvBytes);
        mockServer = new MockKusciaGrpcServer();
        mockServer.start(MOCK_PORT, KusciaProtocolEnum.NOTLS, List.of(
                new DevDomainDataService(), new JobService(), new HealthService()));
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
        DevDomainDataService.created.clear();
        DevDomainDataService.createCode = KusciaAPIConstants.OK;
        DevDomainDataService.relativeUri = SOURCE_URI;
        DevDomainDataService.datatableId = SOURCE_DT;
        DevDomainDataService.domainId = "alice";
        DevDomainDataService.columns = DevDomainDataService.defaultColumns();
        JobService.State.createJobCode = KusciaAPIConstants.OK;
        JobService.State.lastCreateJobRequest = null;
        // 权限基础数据：alice 节点存在 + (p1, alice, dt-sample) 授权
        jdbc.update("insert into node(node_id,name,control_node_id,type,mode) values('alice','alice-node','master','normal',0)");
        jdbc.update("insert into node(node_id,name,control_node_id,type,mode) values('carol','carol-node','master','normal',0)");
        jdbc.update("insert into project_datatable(project_id,node_id,datatable_id,table_configs,source,is_deleted) values('p1','alice','" + SOURCE_DT + "','[]','IMPORTED',0)");
        UserContext.setBaseUser(alice());
    }

    /* ------------------------------- mock ------------------------------- */

    /** 富数据 DomainData mock：query 返回真实元数据，create 记录调用。 */
    public static class DevDomainDataService extends DomainDataServiceGrpc.DomainDataServiceImplBase {
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
            return Common.DataColumn.newBuilder().setName(name).setType(type).setComment("").build();
        }

        @Override
        public void queryDomainData(Domaindata.QueryDomainDataRequest request,
                StreamObserver<Domaindata.QueryDomainDataResponse> responseObserver) {
            Domaindata.DomainData.Builder data = Domaindata.DomainData.newBuilder()
                    .setDomaindataId(datatableId)
                    .setName("sample")
                    .setType("table")
                    .setRelativeUri(relativeUri)
                    .setDomainId(domainId)
                    .setDatasourceId("default-data-source")
                    .putAttributes("DatasourceType", "LOCAL")
                    .putAttributes("DatasourceName", "default-data-source")
                    .setStatus("Available")
                    .setAuthor(domainId)
                    .setFileFormat(Common.FileFormat.CSV)
                    .setVendor("manual")
                    .addAllColumns(columns);
            responseObserver.onNext(Domaindata.QueryDomainDataResponse.newBuilder()
                    .setStatus(Common.Status.newBuilder().setCode(KusciaAPIConstants.OK).setMessage("success").build())
                    .setData(data.build()).build());
            responseObserver.onCompleted();
        }

        @Override
        public void createDomainData(Domaindata.CreateDomainDataRequest request,
                StreamObserver<Domaindata.CreateDomainDataResponse> responseObserver) {
            created.add(request);
            responseObserver.onNext(Domaindata.CreateDomainDataResponse.newBuilder()
                    .setStatus(Common.Status.newBuilder().setCode(createCode)
                            .setMessage(createCode == 0 ? "success" : "boom").build()).build());
            responseObserver.onCompleted();
        }
    }

    /* ------------------------------- helpers ------------------------------- */

    private Map<String, Object> submitSql(String name, String runMode, String sql) {
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("name", name);
        request.put("runMode", runMode);
        request.put("execType", "SQL");
        request.put("nodeId", "alice");
        request.put("datatableId", SOURCE_DT);
        request.put("sql", sql);
        return dataDev.submitTask(request);
    }

    private Map<String, Object> createArtifact(String name, String type) {
        return dataDev.createArtifact(Map.of("name", name, "type", type, "description", "it"));
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

    private long count(String sql, Object... args) {
        Long value = jdbc.queryForObject(sql, Long.class, args);
        return value == null ? 0 : value;
    }

    /* ------------------------------- 用例 ------------------------------- */

    /** 1. SQL DEV：同步 SUCCEEDED + 结果预览 + 调试日志；不注册结果表、不产生血缘。 */
    @Test
    public void sqlDevPreviewAndLogWithoutDomainData() {
        Map<String, Object> task = submitSql("it-sql-dev", "DEV",
                "SELECT category, count(*) c FROM src GROUP BY category ORDER BY category");
        String taskId = String.valueOf(task.get("id"));
        assertEquals("SUCCEEDED", String.valueOf(task.get("status")));
        assertEquals("", String.valueOf(task.get("result_datatable_id")));
        assertEquals(0, DevDomainDataService.created.size(), "DEV 不应注册结果表");
        // 结果预览：3 类各 1 行
        assertTrue(String.valueOf(task.get("result_preview")).contains("\"rows\""));
        // 调试日志（attempt=0）落库，含引擎日志
        assertEquals(1L, count("select count(1) from ds_dev_run_log where task_id=? and attempt=0", taskId));
        assertTrue(count("select count(1) from ds_dev_run_log where task_id=? and log_text like '%query_only%'", taskId) > 0);
        // 审计
        assertEquals(1L, count("select count(1) from ds_unified_log where action='DEV_TASK_DEBUG_SUCCEEDED' and resource_id=?", taskId));
    }

    /** 2. SQL PROD：注册结果表 + 血缘审计 + 挂载项目；DEV 任务不可挂载。 */
    @Test
    public void sqlProdRegistersDomainDataLineageAndMount() {
        Map<String, Object> task = submitSql("it-sql-prod", "PROD", "SELECT * FROM src WHERE score >= 60");
        String taskId = String.valueOf(task.get("id"));
        assertEquals("SUCCEEDED", String.valueOf(task.get("status")));
        String resultDt = String.valueOf(task.get("result_datatable_id"));
        assertNotEquals("", resultDt);
        assertEquals(1, DevDomainDataService.created.size());
        Domaindata.CreateDomainDataRequest req = DevDomainDataService.created.get(0);
        assertEquals(resultDt, req.getDomaindataId());
        assertEquals("alice", req.getDomainId());
        assertEquals("dev-" + taskId, req.getName());
        assertEquals("table", req.getType());
        assertEquals(Common.FileFormat.CSV, req.getFileFormat());
        // 血缘审计（source -> target）
        assertTrue(count("select count(1) from ds_unified_log where action='DEV_TASK_LINEAGE' and resource_id=? and detail like '%" + resultDt + "%'", taskId) > 0);
        // 挂载 + 重复挂载冲突
        dataDev.mountResult(Map.of("taskId", taskId, "projectId", "p1"));
        assertEquals(1L, count("select count(1) from project_datatable where project_id='p1' and datatable_id=? and source='IMPORTED' and is_deleted=0", resultDt));
        assertThrows(IllegalStateException.class, () -> dataDev.mountResult(Map.of("taskId", taskId, "projectId", "p1")));
        // DEV 任务不可挂载
        Map<String, Object> devTask = submitSql("it-sql-dev2", "DEV", "SELECT count(*) c FROM src");
        assertThrows(IllegalStateException.class, () -> dataDev.mountResult(Map.of("taskId", devTask.get("id"), "projectId", "p1")));
    }

    /** 3. 制品 CRUD + 版本自增（不可变）。 */
    @Test
    public void artifactCrudAndVersionAutoIncrement() {
        Map<String, Object> art = createArtifact("it-sql-art", "SQL");
        String artId = String.valueOf(art.get("id"));
        Map<String, Object> v1 = dataDev.createVersion(Map.of("artifactId", artId, "contentText", "SELECT 1"));
        Map<String, Object> v2 = dataDev.createVersion(Map.of("artifactId", artId, "contentText", "SELECT 2",
                "paramsSchema", "[{\"name\":\"x\",\"type\":\"string\"}]", "defaultParams", "{\"x\":\"1\"}"));
        assertEquals(1, ((Number) v1.get("version")).intValue());
        assertEquals(2, ((Number) v2.get("version")).intValue());
        Map<String, Object> detail = dataDev.artifactDetail(artId);
        assertEquals(2, ((List<?>) detail.get("versions")).size());
        assertEquals(2, ((Number) detail.get("latest_version")).intValue());
        // 同名制品冲突
        assertThrows(IllegalArgumentException.class, () -> createArtifact("it-sql-art", "SQL"));
        // 非法 type 拒绝
        assertThrows(IllegalArgumentException.class, () -> createArtifact("it-bad", "EXE"));
        // 删版本回填 latest_version
        dataDev.deleteVersion(String.valueOf(v2.get("id")));
        Map<String, Object> after = dataDev.artifactDetail(artId);
        assertEquals(1, ((Number) after.get("latest_version")).intValue());
        // 删制品（软删制品+版本）
        dataDev.deleteArtifact(artId);
        assertThrows(IllegalArgumentException.class, () -> dataDev.artifactDetail(artId));
    }

    /** 4. JAR 上传校验：合法 JAR 落盘 + sha256/size；非法文件与类型拒绝；下载回读。 */
    @Test
    public void jarUploadValidation() throws IOException {
        Map<String, Object> art = createArtifact("it-jar", "JAR");
        String artId = String.valueOf(art.get("id"));
        byte[] jar = validJar();
        Map<String, Object> v = dataDev.uploadJarVersion(artId, jar, "[]", "{}", "demo", null);
        assertEquals(1, ((Number) v.get("version")).intValue());
        assertEquals(64, String.valueOf(v.get("sha256")).length());
        assertEquals(jar.length, ((Number) v.get("size")).longValue());
        // 下载回读一致
        assertArrayEquals(jar, dataDev.downloadJar(String.valueOf(v.get("id"))));
        // 非 ZIP 拒绝
        assertThrows(IllegalArgumentException.class, () -> dataDev.uploadJarVersion(artId, "not a jar".getBytes(), "[]", "{}", "", null));
        // JAR 上传到 SQL 制品拒绝
        Map<String, Object> sqlArt = createArtifact("it-sql2", "SQL");
        assertThrows(IllegalArgumentException.class,
                () -> dataDev.uploadJarVersion(String.valueOf(sqlArt.get("id")), jar, "[]", "{}", "", null));
    }

    /** 5. PYTHON 依赖记录：createVersion 不再做白名单拦截（缺失依赖由 runner 运行时 pip 安装），dependency_names=实际 import。 */
    @Test
    public void pythonDependencyRecordedNotRejected() {
        Map<String, Object> art = createArtifact("it-py", "PYTHON");
        String artId = String.valueOf(art.get("id"));
        // 白名单外依赖不再被 createVersion 拒绝
        Map<String, Object> v1 = dataDev.createVersion(Map.of("artifactId", artId,
                "contentText", "import requests\nprint(1)"));
        assertEquals(1, ((Number) v1.get("version")).intValue());
        // dependency_names 记录实际 import 的顶层模块
        assertEquals("[\"requests\"]", String.valueOf(v1.get("dependency_names")));
        Map<String, Object> v2 = dataDev.createVersion(Map.of("artifactId", artId,
                "contentText", "import numpy as np\nimport pandas as pd\nprint(np.array([1]))"));
        assertEquals(2, ((Number) v2.get("version")).intValue());
        assertEquals("[\"numpy\",\"pandas\"]", String.valueOf(v2.get("dependency_names")));
    }

    /** 5b. 版本号手填：createVersion/uploadJarVersion 支持用户指定版本号 + 查重。 */
    @Test
    public void versionHandFillAndDedup() throws IOException {
        Map<String, Object> sqlArt = createArtifact("it-ver-sql", "SQL");
        Map<String, Object> v3 = dataDev.createVersion(Map.of("artifactId", sqlArt.get("id"),
                "contentText", "SELECT 3", "version", 3));
        assertEquals(3, ((Number) v3.get("version")).intValue());
        IllegalArgumentException dup = assertThrows(IllegalArgumentException.class,
                () -> dataDev.createVersion(Map.of("artifactId", sqlArt.get("id"), "contentText", "SELECT 3b", "version", 3)));
        assertTrue(String.valueOf(dup.getMessage()).contains(DevErrors.DEV_VERSION_EXISTS), dup.getMessage());
        Map<String, Object> auto = dataDev.createVersion(Map.of("artifactId", sqlArt.get("id"), "contentText", "SELECT 4"));
        assertEquals(4, ((Number) auto.get("version")).intValue());

        Map<String, Object> jarArt = createArtifact("it-ver-jar", "JAR");
        Map<String, Object> j1 = dataDev.uploadJarVersion(String.valueOf(jarArt.get("id")), validJar(), "[]", "{}", "", 2);
        assertEquals(2, ((Number) j1.get("version")).intValue());
        IllegalArgumentException dupJar = assertThrows(IllegalArgumentException.class,
                () -> dataDev.uploadJarVersion(String.valueOf(jarArt.get("id")), validJar(), "[]", "{}", "", 2));
        assertTrue(String.valueOf(dupJar.getMessage()).contains(DevErrors.DEV_VERSION_EXISTS), dupJar.getMessage());
    }

    /** 6. 权限拒绝：carol 访问 alice/dt-sample → DEV_NO_PERMISSION。 */
    @Test
    public void permissionDeniedForUnauthorized() {
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("runMode", "DEV");
        request.put("execType", "SQL");
        request.put("nodeId", "alice");
        request.put("datatableId", SOURCE_DT);
        request.put("sql", "SELECT 1");
        UserContext.setBaseUser(carol());
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> dataDev.submitTask(request));
        assertTrue(e.getMessage().contains(DevErrors.DEV_NO_PERMISSION), e.getMessage());
        // 预览同样被拒
        assertThrows(IllegalArgumentException.class,
                () -> dataDev.previewSource(Map.of("nodeId", "alice", "datatableId", SOURCE_DT)));
    }

    /** 7. 状态冲突：取消已成功 / 重试非失败 / 挂载无结果。 */
    @Test
    public void stateConflicts() {
        Map<String, Object> task = submitSql("it-conflict", "DEV", "SELECT count(*) c FROM src");
        String taskId = String.valueOf(task.get("id"));
        // 取消已成功
        assertThrows(IllegalStateException.class, () -> dataDev.cancelTask(taskId));
        // 重试非失败
        assertThrows(IllegalStateException.class, () -> dataDev.retryTask(taskId));
        // 非创建人操作被拒
        UserContext.setBaseUser(carol());
        assertThrows(IllegalArgumentException.class, () -> dataDev.cancelTask(taskId));
        assertThrows(IllegalArgumentException.class, () -> dataDev.viewResult(taskId));
    }

    /** 8. JAR/PYTHON 提交：RUNNING + kuscia_job_id；createJob payload 形状正确；可取消。 */
    @Test
    public void jarAndPythonSubmitWriteRunningTask() throws IOException {
        Map<String, Object> art = createArtifact("it-jar2", "JAR");
        String artId = String.valueOf(art.get("id"));
        Map<String, Object> v = dataDev.uploadJarVersion(artId, validJar(), "[]", "{}", "", null);
        Map<String, Object> jarReq = new LinkedHashMap<>();
        jarReq.put("name", "it-jar-run");
        jarReq.put("runMode", "PROD");
        jarReq.put("execType", "JAR");
        jarReq.put("nodeId", "alice");
        jarReq.put("datatableId", SOURCE_DT);
        jarReq.put("artifactId", artId);
        jarReq.put("version", ((Number) v.get("version")).intValue());
        Map<String, Object> jarTask = dataDev.submitTask(jarReq);
        String jarTaskId = String.valueOf(jarTask.get("id"));
        assertEquals("RUNNING", String.valueOf(jarTask.get("status")));
        assertEquals("dt-" + jarTaskId, String.valueOf(jarTask.get("kuscia_job_id")));
        // createJob payload：AppImage 是 jar-runner、task_input_config 含 jar_b64
        Job.CreateJobRequest createReq = JobService.State.lastCreateJobRequest;
        assertNotNull(createReq);
        assertEquals("dt-" + jarTaskId, createReq.getJobId());
        assertEquals("data-sandbox-jar-runner", createReq.getTasks(0).getAppImage());
        assertTrue(createReq.getTasks(0).getTaskInputConfig().contains("jar_b64"));
        assertTrue(createReq.getCustomFieldsOrThrow("network_policy").equals("GOVERNANCE"));
        // 取消 RUNNING → CANCELLED
        dataDev.cancelTask(jarTaskId);
        assertEquals("CANCELLED", String.valueOf(dataDev.taskDetail(jarTaskId).get("status")));

        // PYTHON DEV
        Map<String, Object> pyArt = createArtifact("it-py2", "PYTHON");
        dataDev.createVersion(Map.of("artifactId", pyArt.get("id"), "contentText", "import numpy as np\nprint('hi')"));
        Map<String, Object> pyReq = new LinkedHashMap<>();
        pyReq.put("name", "it-py-run");
        pyReq.put("runMode", "DEV");
        pyReq.put("execType", "PYTHON");
        pyReq.put("nodeId", "alice");
        pyReq.put("datatableId", SOURCE_DT);
        pyReq.put("artifactId", pyArt.get("id"));
        pyReq.put("version", 1);
        Map<String, Object> pyTask = dataDev.submitTask(pyReq);
        String pyTaskId = String.valueOf(pyTask.get("id"));
        assertEquals("RUNNING", String.valueOf(pyTask.get("status")));
        Job.CreateJobRequest pyCreate = JobService.State.lastCreateJobRequest;
        assertEquals("data-sandbox-python-runner", pyCreate.getTasks(0).getAppImage());
        assertTrue(pyCreate.getTasks(0).getTaskInputConfig().contains("\"script\""));
        assertTrue(pyCreate.getTasks(0).getTaskInputConfig().contains("\"allowed_imports\""));
        dataDev.cancelTask(pyTaskId);
    }

    /** 9. 依赖白名单 CRUD + enabled 过滤。 */
    @Test
    public void dependencyWhitelistCrud() {
        Map<String, Object> dep = dataDev.createDependency(Map.of("name", "scipy", "versionSpec", ">=1.10", "description", "SciPy"));
        String depId = String.valueOf(dep.get("id"));
        assertEquals("scipy", String.valueOf(dep.get("name")));
        assertThrows(IllegalArgumentException.class, () -> dataDev.createDependency(Map.of("name", "scipy")));
        dataDev.updateDependency(Map.of("id", depId, "enabled", false));
        assertTrue(dataDev.listDependencies("0", null).stream().anyMatch(d -> depId.equals(String.valueOf(d.get("id")))));
        dataDev.deleteDependency(depId);
        assertTrue(dataDev.listDependencies(null, "scipy").isEmpty());
    }

    /** 10. 调试日志 + 结果查看。 */
    @Test
    public void runLogAndViewResult() {
        Map<String, Object> task = submitSql("it-log", "DEV", "SELECT count(*) c FROM src");
        String taskId = String.valueOf(task.get("id"));
        Map<String, Object> log = dataDev.runLog(taskId, 0);
        assertTrue(String.valueOf(log.get("logText")).contains("query_only"));
        Map<String, Object> result = dataDev.viewResult(taskId);
        assertEquals("DEV", String.valueOf(result.get("runMode")));
        assertTrue(((Map<?, ?>) result.get("preview")).containsKey("rows"));
        // 非创建人查看被拒
        UserContext.setBaseUser(carol());
        assertThrows(IllegalArgumentException.class, () -> dataDev.viewResult(taskId));
    }

    /** 11. 失败注入 → FAILED + 告警；恢复后重试 → SUCCEEDED（retry_count=1，run_log attempt=1）。 */
    @Test
    public void retryFailedSqlSucceeds() {
        DevDomainDataService.createCode = 1;
        Map<String, Object> task = submitSql("it-retry", "PROD", "SELECT count(*) c FROM src");
        String taskId = String.valueOf(task.get("id"));
        assertEquals("FAILED", String.valueOf(task.get("status")));
        assertTrue(String.valueOf(task.get("error_message")).contains("注册结果数据集失败"));
        assertEquals(1L, count("select count(1) from ds_alert_event where source='DATA_DEV' and status='OPEN' and dedupe_key=?", "dev:" + taskId + ":failed"));
        DevDomainDataService.createCode = KusciaAPIConstants.OK;
        Map<String, Object> retried = dataDev.retryTask(taskId);
        assertEquals("SUCCEEDED", String.valueOf(retried.get("status")));
        assertEquals(1, ((Number) retried.get("retry_count")).intValue());
        assertEquals(1L, count("select count(1) from ds_dev_run_log where task_id=? and attempt=1", taskId));
        assertEquals(1L, count("select count(1) from ds_unified_log where action='DEV_TASK_RETRY' and resource_id=?", taskId));
    }
}
