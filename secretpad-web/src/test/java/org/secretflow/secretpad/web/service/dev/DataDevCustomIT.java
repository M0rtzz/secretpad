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
import org.secretflow.v1alpha1.kusciaapi.Job;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Z-05 Stage 2 集成测试：DevJobExecutor 运行组件（一次性 Kuscia Job + 容器输出取回 + 结果注册）。
 *
 * <p>共享 {@link JobService} 桩配置 Job 状态与 Cluster 端点；本地 {@link HttpServer} 模拟容器输出端口
 * （/status、/result、/log）。@Scheduled 不运行（poll-interval 拉高），手动调 {@code pollDevTasks()}。</p>
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
        "spring.datasource.default.jdbc-url=jdbc:sqlite:${java.io.tmpdir}/ds-dev-custom-it.sqlite",
        "spring.datasource.quartz.jdbc-url=jdbc:h2:${java.io.tmpdir}/ds-dev-custom-it-quartz.mv.db;DB_CLOSE_ON_EXIT=FALSE",
        "secretpad.data.dir-path=${java.io.tmpdir}/ds-dev-custom-data/",
        "secretpad.data-sandbox.dev.input-rows=10000",
        "secretpad.data-sandbox.dev.input-bytes=262144",
        "secretpad.data-sandbox.dev.max-retries=3",
        "secretpad.data-sandbox.dev.timeout-seconds=300",
        "secretpad.data-sandbox.dev.poll-interval-ms=3600000",
        "secretpad.data-sandbox.dev.result-preview-rows=10",
})
public class DataDevCustomIT {

    private static final int MOCK_PORT = 50055;
    private static final String SOURCE_DT = "dt-sample";
    private static final String SOURCE_URI = "sample_full.csv";
    private static final String DATA_ROOT = System.getProperty("java.io.tmpdir") + "/ds-dev-custom-data";
    /** 容器输出端口模拟返回的结果 CSV。 */
    private static final String RESULT_CSV = "id,name,amount\n1,alice,12.5\n2,bob,3.14\n";

    @Resource
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbc;

    @Resource
    private DataDevService dataDev;

    @Resource
    private DevJobExecutor devJobExecutor;

    @Resource
    private DynamicKusciaChannelProvider channelProvider;

    private MockKusciaGrpcServer mockServer;
    private HttpServer containerServer;
    private int containerPort;

    /* ------------------------------- 角色 ------------------------------- */

    private UserContextDTO alice() {
        return UserContextDTO.builder().ownerId("alice").name("alice")
                .platformType(PlatformTypeEnum.CENTER).platformNodeId("alice")
                .ownerType(UserOwnerTypeEnum.CENTER).projectIds(Set.of("p1")).build();
    }

    /* ------------------------------- 生命周期 ------------------------------- */

    @BeforeAll
    public void startMock() throws Exception {
        Files.createDirectories(Path.of(DATA_ROOT, "alice"));
        try (InputStream in = getClass().getResourceAsStream("/gov/sample_full.csv")) {
            assertNotNull(in, "test resource gov/sample_full.csv missing");
            Files.copy(in, Path.of(DATA_ROOT, "alice", SOURCE_URI), StandardCopyOption.REPLACE_EXISTING);
        }
        // 容器输出端口模拟
        containerServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        containerServer.createContext("/status", exchange -> {
            byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        containerServer.createContext("/result", exchange -> {
            byte[] body = RESULT_CSV.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        containerServer.createContext("/log", exchange -> {
            byte[] body = "jar/py run log".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        containerServer.start();
        containerPort = containerServer.getAddress().getPort();

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
        containerServer.stop(0);
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
        // 权限基础数据
        jdbc.update("insert into node(node_id,name,control_node_id,type,mode) values('alice','alice-node','master','normal',0)");
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

    private Map<String, Object> submitJar(String runMode) throws IOException {
        Map<String, Object> art = createArtifact("it-jar", "JAR");
        String artId = String.valueOf(art.get("id"));
        dataDev.uploadJarVersion(artId, validJar(), "[]", "{}", "", null);
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("name", "it-jar-run");
        request.put("runMode", runMode);
        request.put("execType", "JAR");
        request.put("nodeId", "alice");
        request.put("datatableId", SOURCE_DT);
        request.put("artifactId", artId);
        request.put("version", 1);
        return dataDev.submitTask(request);
    }

    private Map<String, Object> submitPython(String runMode) {
        Map<String, Object> art = createArtifact("it-py", "PYTHON");
        dataDev.createVersion(Map.of("artifactId", art.get("id"),
                "contentText", "import numpy as np\nimport pandas as pd\nprint(np.array([1]))"));
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("name", "it-py-run");
        request.put("runMode", runMode);
        request.put("execType", "PYTHON");
        request.put("nodeId", "alice");
        request.put("datatableId", SOURCE_DT);
        request.put("artifactId", art.get("id"));
        request.put("version", 1);
        return dataDev.submitTask(request);
    }

    private long count(String sql, Object... args) {
        Long value = jdbc.queryForObject(sql, Long.class, args);
        return value == null ? 0 : value;
    }

    private String findResultUri(String taskId) {
        try (var stream = Files.list(Path.of(DATA_ROOT, "alice"))) {
            return stream.filter(p -> p.getFileName().toString().startsWith(taskId + "-"))
                    .findFirst().orElseThrow().getFileName().toString();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private String readFile(String uri) {
        try {
            return Files.readString(Path.of(DATA_ROOT, "alice", uri), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /* ------------------------------- 用例 ------------------------------- */

    /** 1. JAR PROD：createJob 下发（appImage/payload/network_policy）→ 取回 → 注册结果 → 血缘 → 审计。 */
    @Test
    public void jarProdSubmitsFetchesRegistersLineage() throws IOException {
        Map<String, Object> task = submitJar("PROD");
        String taskId = String.valueOf(task.get("id"));
        assertEquals("RUNNING", String.valueOf(task.get("status")));
        assertEquals("dt-" + taskId, String.valueOf(task.get("kuscia_job_id")));

        Job.CreateJobRequest sent = JobService.State.lastCreateJobRequest;
        assertNotNull(sent);
        assertEquals("dt-" + taskId, sent.getJobId());
        assertEquals("alice", sent.getInitiator());
        assertEquals("GOVERNANCE", sent.getCustomFieldsMap().get("network_policy"));
        assertEquals(taskId, sent.getCustomFieldsMap().get("task_id"));
        assertEquals("data-sandbox-jar-runner", sent.getTasks(0).getAppImage());
        assertEquals("server", sent.getTasks(0).getParties(0).getRole());
        assertEquals("0.5", sent.getTasks(0).getParties(0).getResources().getCpu());
        assertEquals("512Mi", sent.getTasks(0).getParties(0).getResources().getMemory());
        String inputConfig = sent.getTasks(0).getTaskInputConfig();
        assertTrue(inputConfig.contains("jar_b64"), "task_input_config 应携带 JAR base64");
        assertTrue(inputConfig.contains("input_csv_b64"), "task_input_config 应携带 base64 输入子集");

        // 容器执行成功 → 端点可取回结果
        JobService.State.jobState = "Succeeded";
        JobService.State.withEndpoints = true;
        devJobExecutor.pollDevTasks();

        Map<String, Object> after = dataDev.taskDetail(taskId);
        assertEquals("SUCCEEDED", String.valueOf(after.get("status")));
        String resultDt = String.valueOf(after.get("result_datatable_id"));
        assertNotEquals("", resultDt);
        assertEquals("alice", String.valueOf(after.get("result_node_id")));
        assertEquals(2L, ((Number) after.get("result_rows")).longValue());
        // 结果 CSV 落盘
        String uri = findResultUri(taskId);
        assertEquals(RESULT_CSV, readFile(uri));
        // DomainData 注册一次
        assertEquals(1, DevDomainDataService.created.size());
        assertEquals(resultDt, DevDomainDataService.created.get(0).getDomaindataId());
        // 血缘审计（source -> target）
        assertTrue(count("select count(1) from ds_unified_log where action='DEV_TASK_LINEAGE' and resource_id=? and detail like '%" + resultDt + "%'", taskId) > 0);
        assertEquals(1L, count("select count(1) from ds_unified_log where action='DEV_TASK_SUCCEEDED' and resource_id=?", taskId));
        // 调试日志写 ds_dev_run_log（attempt=0，容器 /log）
        assertEquals(1L, count("select count(1) from ds_dev_run_log where task_id=? and attempt=0", taskId));
        assertTrue(count("select count(1) from ds_dev_run_log where task_id=? and log_text like '%run log%'", taskId) > 0);
    }

    /** 2. PYTHON DEV：只预览 + 日志，不注册结果表、不产生血缘；appImage=python-runner。 */
    @Test
    public void pythonDevOnlyPreviewNoDomainData() {
        Map<String, Object> task = submitPython("DEV");
        String taskId = String.valueOf(task.get("id"));
        assertEquals("RUNNING", String.valueOf(task.get("status")));
        Job.CreateJobRequest sent = JobService.State.lastCreateJobRequest;
        assertEquals("data-sandbox-python-runner", sent.getTasks(0).getAppImage());
        String inputConfig = sent.getTasks(0).getTaskInputConfig();
        assertTrue(inputConfig.contains("\"script\""));
        assertTrue(inputConfig.contains("\"allowed_imports\""));
        assertTrue(inputConfig.contains("numpy"), "allowed_imports 应含白名单 numpy");

        JobService.State.jobState = "Succeeded";
        JobService.State.withEndpoints = true;
        JobService.State.endpointPortName = "py";
        devJobExecutor.pollDevTasks();

        Map<String, Object> after = dataDev.taskDetail(taskId);
        assertEquals("SUCCEEDED", String.valueOf(after.get("status")));
        assertEquals("", String.valueOf(after.get("result_datatable_id")));
        assertEquals(0, DevDomainDataService.created.size(), "DEV 不应注册结果表");
        assertTrue(String.valueOf(after.get("result_preview")).contains("\"rows\""));
        assertEquals(1L, count("select count(1) from ds_dev_run_log where task_id=? and attempt=0", taskId));
        assertEquals(1L, count("select count(1) from ds_unified_log where action='DEV_TASK_DEBUG_SUCCEEDED' and resource_id=?", taskId));
        assertEquals(0L, count("select count(1) from ds_unified_log where action='DEV_TASK_SUCCEEDED' and resource_id=?", taskId));
    }

    /** 3. 容器常驻服务：Job 仍 Running 但 /result 就绪 → 提前完成（取回后终止）。 */
    @Test
    public void runningButResultReadyFinalizes() throws IOException {
        Map<String, Object> task = submitJar("PROD");
        String taskId = String.valueOf(task.get("id"));
        JobService.State.jobState = "Running";
        JobService.State.withEndpoints = true;
        devJobExecutor.pollDevTasks();
        Map<String, Object> after = dataDev.taskDetail(taskId);
        assertEquals("SUCCEEDED", String.valueOf(after.get("status")));
        assertEquals(2L, ((Number) after.get("result_rows")).longValue());
        assertEquals(RESULT_CSV, readFile(findResultUri(taskId)));
        assertEquals(1, DevDomainDataService.created.size());
    }

    /** 4. 超时：容器长时间未结束 → stopJob + FAILED + 超时告警。 */
    @Test
    public void timeoutStopsJobAndMarksFailed() throws IOException {
        Map<String, Object> task = submitJar("PROD");
        String taskId = String.valueOf(task.get("id"));
        jdbc.update("update ds_dev_task set started_at=? where id=?",
                LocalDateTime.now().minusSeconds(400).toString(), taskId);
        JobService.State.jobState = "Running";
        devJobExecutor.pollDevTasks();
        Map<String, Object> after = dataDev.taskDetail(taskId);
        assertEquals("FAILED", String.valueOf(after.get("status")));
        assertTrue(String.valueOf(after.get("error_message")).contains("超时"), after.get("error_message") + "");
        assertEquals(1L, count("select count(1) from ds_alert_event where source='DATA_DEV' and status='OPEN' and dedupe_key=?",
                "dev:" + taskId + ":timeout"));
    }

    /** 5. createJob 失败 → 任务 FAILED + 告警，不落 kuscia_job_id。 */
    @Test
    public void createJobFailureMarksFailed() throws IOException {
        JobService.State.createJobCode = 101010;
        JobService.State.createJobMessage = "app image not found";
        Map<String, Object> task = submitJar("PROD");
        assertEquals("FAILED", String.valueOf(task.get("status")));
        assertTrue(String.valueOf(task.get("error_message")).contains("创建 JAR 执行容器失败"), task.get("error_message") + "");
        assertEquals(1L, count("select count(1) from ds_alert_event where source='DATA_DEV' and status='OPEN' and dedupe_key=?",
                "dev:" + task.get("id") + ":failed"));
    }

    /** 6. Job 状态 Failed → FAILED + 告警 + deleteJob。 */
    @Test
    public void jobFailedMarksFailedAndAlerts() throws IOException {
        Map<String, Object> task = submitJar("PROD");
        String taskId = String.valueOf(task.get("id"));
        JobService.State.jobState = "Failed";
        JobService.State.jobErrMsg = "exit code 1";
        devJobExecutor.pollDevTasks();
        Map<String, Object> after = dataDev.taskDetail(taskId);
        assertEquals("FAILED", String.valueOf(after.get("status")));
        assertTrue(String.valueOf(after.get("error_message")).contains("执行容器失败"), after.get("error_message") + "");
        assertEquals(1L, count("select count(1) from ds_alert_event where source='DATA_DEV' and status='OPEN' and dedupe_key=?",
                "dev:" + taskId + ":failed"));
    }

    /** 7. 失败重试：createJob 失败 → 恢复 → retry → 成功，retry_count=1，run_log attempt=1。 */
    @Test
    public void retryAfterFailureSucceeds() throws IOException {
        JobService.State.createJobCode = 101010;
        Map<String, Object> task = submitJar("PROD");
        String taskId = String.valueOf(task.get("id"));
        assertEquals("FAILED", String.valueOf(task.get("status")));

        JobService.State.createJobCode = KusciaAPIConstants.OK;
        JobService.State.withEndpoints = true;
        JobService.State.jobState = "Succeeded";
        Map<String, Object> retried = dataDev.retryTask(taskId);
        assertEquals("RUNNING", String.valueOf(retried.get("status")));
        assertEquals("dt-" + taskId, String.valueOf(retried.get("kuscia_job_id")));

        devJobExecutor.pollDevTasks();
        Map<String, Object> after = dataDev.taskDetail(taskId);
        assertEquals("SUCCEEDED", String.valueOf(after.get("status")));
        assertEquals(1, ((Number) after.get("retry_count")).intValue());
        assertEquals(1L, count("select count(1) from ds_dev_run_log where task_id=? and attempt=1", taskId));
        assertEquals(1L, count("select count(1) from ds_unified_log where action='DEV_TASK_RETRY' and resource_id=?", taskId));
        assertFalse(DevDomainDataService.created.isEmpty(), "PROD 重试成功应注册结果表");
    }
}
