/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.service.model;

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
import org.secretflow.secretpad.web.service.dev.DataDevService;
import org.secretflow.secretpad.web.service.dev.DevJobExecutor;

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
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Z-06 Stage 1+2 集成测试：模型注册/审批/强制测试门禁/测试执行（复用一次性 Kuscia Job）/指标/摘要
 * + DevJobExecutor 同步 {@code runAndAwait}（channel='api' 不被调度轮询、结果/耗时返回、结果 CSV 落盘）。
 *
 * <p>独立 mock 端口 50055 + SQLite + 临时数据目录 + 本地 {@link HttpServer} 模拟容器输出
 * （/status、/result、/log）。测试数据为带标签列 {@code pass}/{@code score} 的 4 行 CSV，
 * 结果 CSV 与输入行级 1:1 对齐以验证指标。@Scheduled 不运行（poll-interval 拉高），手动调
 * {@code pollDevTasks()}。</p>
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
        "spring.datasource.default.jdbc-url=jdbc:sqlite:${java.io.tmpdir}/ds-model-it.sqlite",
        "spring.datasource.quartz.jdbc-url=jdbc:h2:${java.io.tmpdir}/ds-model-it-quartz.mv.db;DB_CLOSE_ON_EXIT=FALSE",
        "secretpad.data.dir-path=${java.io.tmpdir}/ds-model-data/",
        "secretpad.data-sandbox.dev.input-rows=10000",
        "secretpad.data-sandbox.dev.input-bytes=262144",
        "secretpad.data-sandbox.dev.max-retries=3",
        "secretpad.data-sandbox.dev.timeout-seconds=300",
        "secretpad.data-sandbox.dev.poll-interval-ms=3600000",
        "secretpad.data-sandbox.dev.result-preview-rows=10",
        "secretpad.data-sandbox.model.test.max-retries=3",
        "secretpad.data-sandbox.model.test.result-preview-rows=10",
})
public class DataModelIT {

    private static final int MOCK_PORT = 50055;
    private static final String LABELED_DT = "dt-labeled";
    private static final String LABELED_URI = "model_labeled.csv";
    private static final String DATA_ROOT = System.getProperty("java.io.tmpdir") + "/ds-model-data";

    /** 带真实标签的测试集（pass=score>=30 的二分类标签 + score 数值回归标签）。 */
    private static final String LABELED_CSV = "id,score,pass\n1,60,1\n2,80,1\n3,10,0\n4,20,0\n";
    /** 分类测试结果：prediction 与 pass 对齐（1 处错误 → accuracy=0.75）。 */
    private static final String CLASS_RESULT_CSV =
            "id,score,pass,prediction\n1,60,1,1\n2,80,1,1\n3,10,0,0\n4,20,0,1\n";
    /** 回归测试结果：score_pred 与 score 差异 [2,2,2,0] → mae=1.5 / rmse=√3 / r²=1-12/3275。 */
    private static final String REGRESSION_RESULT_CSV =
            "id,score,pass,score_pred\n1,60,1,62\n2,80,1,78\n3,10,0,12\n4,20,0,20\n";

    @Resource
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbc;

    @Resource
    private DataDevService dataDev;

    @Resource
    private ModelApprovalService modelApproval;

    @Resource
    private ModelTestService modelTestService;

    @Resource
    private DevJobExecutor devJobExecutor;

    @Resource
    private DynamicKusciaChannelProvider channelProvider;

    private MockKusciaGrpcServer mockServer;
    private HttpServer containerServer;
    private int containerPort;

    /** 容器 /result 返回的内容（每个用例按需覆盖）。 */
    private String resultCsv = "id,name,amount\n1,alice,12.5\n2,bob,3.14\n";

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
            byte[] body = "model test run log".getBytes(StandardCharsets.UTF_8);
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
        DevDomainDataService.created.clear();
        DevDomainDataService.createCode = KusciaAPIConstants.OK;
        DevDomainDataService.relativeUri = LABELED_URI;
        DevDomainDataService.datatableId = LABELED_DT;
        DevDomainDataService.domainId = "alice";
        DevDomainDataService.columns = DevDomainDataService.labeledColumns();
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
        resultCsv = "id,name,amount\n1,alice,12.5\n2,bob,3.14\n";
        // 权限基础数据：项目 p1 + 节点 alice + 授权表 (p1, alice, dt-labeled)
        jdbc.update("insert into project(project_id,name,owner_id,is_deleted) values('p1','P1','alice',0)");
        jdbc.update("insert into project_node(project_id,node_id,is_deleted) values('p1','alice',0)");
        jdbc.update("insert into node(node_id,name,control_node_id,type,mode) values('alice','alice-node','master','normal',0)");
        jdbc.update("insert into node(node_id,name,control_node_id,type,mode) values('carol','carol-node','master','normal',0)");
        jdbc.update("insert into project_datatable(project_id,node_id,datatable_id,table_configs,source,is_deleted)"
                + " values('p1','alice','" + LABELED_DT + "','[]','IMPORTED',0)");
        UserContext.setBaseUser(alice());
    }

    /* ------------------------------- mock ------------------------------- */

    /** 富数据 DomainData mock：query 返回真实元数据（labeled 表），create 记录调用。 */
    public static class DevDomainDataService extends DomainDataServiceGrpc.DomainDataServiceImplBase {
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
            return Common.DataColumn.newBuilder().setName(name).setType(type).setComment("").build();
        }

        @Override
        public void queryDomainData(Domaindata.QueryDomainDataRequest request,
                StreamObserver<Domaindata.QueryDomainDataResponse> responseObserver) {
            Domaindata.DomainData.Builder data = Domaindata.DomainData.newBuilder()
                    .setDomaindataId(datatableId)
                    .setName("model_labeled")
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

    /** 注册 JAR 制品 + v1 版本，返回 {artifactId, artifactVersionId}。 */
    private Map<String, Object> jarArtifact(String name) throws IOException {
        Map<String, Object> art = createArtifact(name, "JAR");
        Map<String, Object> v = dataDev.uploadJarVersion(String.valueOf(art.get("id")), validJar(), "[]", "{}", "", null);
        return Map.of("artifactId", String.valueOf(art.get("id")), "artifactVersionId", String.valueOf(v.get("id")));
    }

    /** 注册 PYTHON 制品 + v1 脚本版本，返回 {artifactId, artifactVersionId}。 */
    private Map<String, Object> pythonArtifact(String name, String script) {
        Map<String, Object> art = createArtifact(name, "PYTHON");
        Map<String, Object> v = dataDev.createVersion(Map.of("artifactId", String.valueOf(art.get("id")),
                "contentText", script, "dependencyNames", List.of()));
        return Map.of("artifactId", String.valueOf(art.get("id")), "artifactVersionId", String.valueOf(v.get("id")));
    }

    private Map<String, Object> registerModel(String name, Map<String, Object> artifact) {
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("name", name);
        request.put("projectId", "p1");
        request.put("artifactId", artifact.get("artifactId"));
        request.put("artifactVersionId", artifact.get("artifactVersionId"));
        request.put("description", "it");
        return modelApproval.registerModel(request);
    }

    private String submitAndApproveStage1(String modelId) {
        Map<String, Object> approval = modelApproval.submitApproval(modelId, "please review");
        String approvalId = String.valueOf(approval.get("id"));
        assertEquals("MODEL_REVIEW", String.valueOf(approval.get("status")));
        Map<String, Object> stage1 = modelApproval.approvalAction(approvalId, "APPROVE", "ok");
        assertEquals("RESOURCE_REVIEW", String.valueOf(stage1.get("status")));
        return approvalId;
    }

    private Map<String, Object> executeTest(String modelId, String labelColumn, String predictionColumn, String metricType) {
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("modelId", modelId);
        request.put("nodeId", "alice");
        request.put("datatableId", LABELED_DT);
        request.put("labelColumn", labelColumn);
        request.put("predictionColumn", predictionColumn);
        request.put("metricType", metricType);
        request.put("params", Map.of());
        return modelTestService.executeTest(request);
    }

    private void succeedRunningTasks() {
        JobService.State.jobState = "Succeeded";
        JobService.State.withEndpoints = true;
        devJobExecutor.pollDevTasks();
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

    private static String now() {
        return LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS).toString();
    }

    /* ------------------------------- 用例 ------------------------------- */

    /** 1. 注册校验：JAR 放行、SQL 拒绝、重复拒绝、node_id 取项目首个节点、同制品重注册版本自增。 */
    @Test
    public void registerModelRejectsSqlAndDuplicateAndAutoIncrementsVersion() throws IOException {
        Map<String, Object> jar = jarArtifact("m-jar");
        Map<String, Object> model = registerModel("m-jar-model", jar);
        String modelId = String.valueOf(model.get("id"));
        assertEquals("DRAFT", String.valueOf(model.get("status")));
        assertEquals("alice", String.valueOf(model.get("node_id")));
        assertEquals(1, ((Number) model.get("version")).intValue());

        // SQL 制品不是模型
        Map<String, Object> sqlArt = createArtifact("m-sql", "SQL");
        dataDev.createVersion(Map.of("artifactId", sqlArt.get("id"), "contentText", "SELECT 1",
                "dependencyNames", List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> registerModel("m-sql-model", Map.of("artifactId", String.valueOf(sqlArt.get("id")),
                        "artifactVersionId", dataDev.listVersions(String.valueOf(sqlArt.get("id"))).get(0).get("id"))));

        // 同项目同制品重复注册 → MODEL_ALREADY_EXISTS
        IllegalArgumentException dup = assertThrows(IllegalArgumentException.class,
                () -> registerModel("m-jar-dup", jar));
        assertTrue(dup.getMessage().contains(ModelErrors.MODEL_ALREADY_EXISTS), dup.getMessage());

        // 拒绝后重注册 → version 自增为 2（无非终结态模型）
        String approvalId = submitAndApproveStage1(modelId);
        modelApproval.approvalAction(approvalId, "REJECT", "no");
        assertEquals("REJECTED", String.valueOf(modelApproval.modelDetail(modelId).get("status")));
        Map<String, Object> again = registerModel("m-jar-again", jar);
        assertEquals(2, ((Number) again.get("version")).intValue());
        assertEquals(modelId, String.valueOf(modelApproval.modelDetail(modelId).get("id")));
    }

    /** 2. 两级审批 + 强制测试门禁：无测试 → MODEL_TEST_REQUIRED；有成功测试 → APPROVED → PUBLISHED。 */
    @Test
    public void twoStageApprovalWithTestGateAndPublish() throws IOException {
        Map<String, Object> jar = jarArtifact("m-gate");
        Map<String, Object> model = registerModel("m-gate-model", jar);
        String modelId = String.valueOf(model.get("id"));
        String approvalId = submitAndApproveStage1(modelId);
        assertEquals("RESOURCE_REVIEW", String.valueOf(modelApproval.approvalDetail(approvalId).get("status")));

        // 门禁负向：资源审批通过前无成功测试 → MODEL_TEST_REQUIRED
        IllegalArgumentException gate = assertThrows(IllegalArgumentException.class,
                () -> modelApproval.approvalAction(approvalId, "APPROVE", ""));
        assertTrue(gate.getMessage().contains(ModelErrors.MODEL_TEST_REQUIRED), gate.getMessage());
        assertEquals("RESOURCE_REVIEW", String.valueOf(modelApproval.approvalDetail(approvalId).get("status")), "审批状态应保持不变");

        // 执行分类测试 → RUNNING（task RUNNING，channel='model'）
        resultCsv = CLASS_RESULT_CSV;
        Map<String, Object> test = executeTest(modelId, "pass", "prediction", "classification");
        String testId = String.valueOf(test.get("id"));
        assertEquals("RUNNING", String.valueOf(test.get("status")));
        assertEquals("model", String.valueOf(
                jdbc.queryForMap("select channel from ds_dev_task where id=?", test.get("task_id")).get("channel")));

        // 容器成功 → 调度轮询收尾 task → 测试惰性收官 + 指标/摘要/证据
        succeedRunningTasks();
        Map<String, Object> done = modelTestService.finalizeIfNeededAndGet(testId);
        assertEquals("SUCCEEDED", String.valueOf(done.get("status")));
        @SuppressWarnings("unchecked")
        Map<String, Object> metrics = (Map<String, Object>) done.get("metrics");
        assertEquals("classification", String.valueOf(metrics.get("metricType")));
        assertEquals(0.75, ((Number) metrics.get("accuracy")).doubleValue(), 1e-6);
        assertEquals(0.833333, ((Number) metrics.get("precision")).doubleValue(), 1e-6);
        assertEquals(0.75, ((Number) metrics.get("recall")).doubleValue(), 1e-6);
        assertEquals(0.733333, ((Number) metrics.get("f1")).doubleValue(), 1e-6);
        @SuppressWarnings("unchecked")
        Map<String, Object> cm = (Map<String, Object>) metrics.get("confusionMatrix");
        assertNotNull(cm);
        assertEquals("1", String.valueOf(cm.get("positive")));
        assertEquals(2, ((Number) cm.get("tp")).intValue());
        assertEquals(1, ((Number) cm.get("fp")).intValue());
        assertEquals(0, ((Number) cm.get("fn")).intValue());
        assertEquals(1, ((Number) cm.get("tn")).intValue());
        // 输入/输出摘要
        @SuppressWarnings("unchecked")
        Map<String, Object> inputSummary = (Map<String, Object>) done.get("inputSummary");
        assertEquals(3, ((Number) inputSummary.get("columnCount")).intValue());
        assertEquals(4, ((Number) inputSummary.get("rowCount")).intValue());
        @SuppressWarnings("unchecked")
        Map<String, Object> outputSummary = (Map<String, Object>) done.get("outputSummary");
        assertEquals(4, ((Number) outputSummary.get("columnCount")).intValue());
        assertEquals(4, ((Number) outputSummary.get("rowCount")).intValue());
        // 结果预览 + 调试日志 + 结果 CSV 落盘（model 通道即使 DEV 也持久化）
        assertTrue(String.valueOf(done.get("result_preview")).contains("prediction"));
        assertEquals(1L, count("select count(1) from ds_dev_run_log where task_id=? and attempt=0", test.get("task_id")));
        assertTrue(!findResultUri(String.valueOf(test.get("task_id"))).isEmpty());
        // 审批 test_evidence
        assertEquals(1L, count("select count(1) from ds_model_approval where id=? and test_evidence<>''", approvalId));
        // 审计
        assertEquals(1L, count("select count(1) from ds_unified_log where action='MODEL_TEST_SUCCEEDED' and resource_id=?", testId));

        // 门禁正向：有成功且有指标的测试 → APPROVED
        Map<String, Object> approved = modelApproval.approvalAction(approvalId, "APPROVE", "metrics ok");
        assertEquals("APPROVED", String.valueOf(approved.get("status")));
        assertEquals("APPROVED", String.valueOf(modelApproval.modelDetail(modelId).get("status")));
        assertNotEquals("", String.valueOf(modelApproval.modelDetail(modelId).get("approved_at")));

        // PUBLISH → PUBLISHED
        Map<String, Object> published = modelApproval.approvalAction(approvalId, "PUBLISH", "");
        assertEquals("PUBLISHED", String.valueOf(published.get("status")));
        assertEquals("PUBLISHED", String.valueOf(modelApproval.modelDetail(modelId).get("status")));
        assertNotEquals("", String.valueOf(modelApproval.modelDetail(modelId).get("published_at")));
        // 终态再审批 → MODEL_STATE_CONFLICT
        assertThrows(IllegalArgumentException.class,
                () -> modelApproval.approvalAction(approvalId, "APPROVE", ""));
    }

    /** 3. 回归指标：score × score_pred → MAE/RMSE/R²。 */
    @Test
    public void regressionMetricsOnScore() throws IOException {
        Map<String, Object> jar = jarArtifact("m-reg");
        Map<String, Object> model = registerModel("m-reg-model", jar);
        String modelId = String.valueOf(model.get("id"));
        String approvalId = submitAndApproveStage1(modelId);

        resultCsv = REGRESSION_RESULT_CSV;
        Map<String, Object> test = executeTest(modelId, "score", "score_pred", "regression");
        succeedRunningTasks();
        Map<String, Object> done = modelTestService.finalizeIfNeededAndGet(String.valueOf(test.get("id")));
        assertEquals("SUCCEEDED", String.valueOf(done.get("status")));
        @SuppressWarnings("unchecked")
        Map<String, Object> metrics = (Map<String, Object>) done.get("metrics");
        assertEquals("regression", String.valueOf(metrics.get("metricType")));
        assertEquals(1.5, ((Number) metrics.get("mae")).doubleValue(), 1e-6);
        assertEquals(1.732051, ((Number) metrics.get("rmse")).doubleValue(), 1e-6);
        assertEquals(0.996336, ((Number) metrics.get("r2")).doubleValue(), 1e-6);
        assertEquals(4, ((Number) metrics.get("samples")).intValue());

        // 有成功测试 → 门禁通过
        assertEquals("APPROVED", String.valueOf(modelApproval.approvalAction(approvalId, "APPROVE", "").get("status")));
    }

    /** 4. 审批权限：非创建人/提交人不能审批；非创建人不能删除模型。 */
    @Test
    public void permissionDeniedForNonCreator() throws IOException {
        Map<String, Object> jar = jarArtifact("m-perm");
        Map<String, Object> model = registerModel("m-perm-model", jar);
        String modelId = String.valueOf(model.get("id"));
        String approvalId = submitAndApproveStage1(modelId);

        UserContext.setBaseUser(carol());
        assertThrows(IllegalArgumentException.class,
                () -> modelApproval.approvalAction(approvalId, "APPROVE", ""));
        assertThrows(IllegalArgumentException.class,
                () -> modelApproval.deleteModel(modelId));
        assertThrows(IllegalArgumentException.class,
                () -> modelTestService.executeTest(Map.of("modelId", modelId, "nodeId", "alice",
                        "datatableId", LABELED_DT, "labelColumn", "pass", "predictionColumn", "prediction",
                        "metricType", "classification")));
        UserContext.setBaseUser(alice());
        // 恢复 alice 后审批可用（门禁仍拦截，因为 carol 无法测试）
        assertThrows(IllegalArgumentException.class,
                () -> modelApproval.approvalAction(approvalId, "APPROVE", ""));
    }

    /** 5. REJECT → RESUBMIT（版本+1）→ 回到 MODEL_REVIEW。 */
    @Test
    public void rejectAndResubmitWithVersionBump() throws IOException {
        Map<String, Object> jar = jarArtifact("m-resub");
        Map<String, Object> model = registerModel("m-resub-model", jar);
        String modelId = String.valueOf(model.get("id"));
        String approvalId = submitAndApproveStage1(modelId);
        Map<String, Object> rejected = modelApproval.approvalAction(approvalId, "REJECT", "need more tests");
        assertEquals("REJECTED", String.valueOf(rejected.get("status")));
        assertEquals("REJECTED", String.valueOf(modelApproval.modelDetail(modelId).get("status")));

        Map<String, Object> resubmitted = modelApproval.approvalAction(approvalId, "RESUBMIT", "retry");
        assertEquals("MODEL_REVIEW", String.valueOf(resubmitted.get("status")));
        assertEquals(2, ((Number) resubmitted.get("version")).intValue());
        assertEquals("APPROVING", String.valueOf(modelApproval.modelDetail(modelId).get("status")));
        // 提交中不可再次提交
        assertThrows(IllegalArgumentException.class, () -> modelApproval.submitApproval(modelId, ""));
    }

    /** 6. PYTHON 模型：白名单校验 + python-runner 提交 + 指标；非白名单 import 直接拒绝。 */
    @Test
    public void pythonModelRunsAndDependencyRejected() throws IOException {
        Map<String, Object> py = pythonArtifact("m-py", "import numpy as np\nimport pandas as pd\nprint('x')");
        Map<String, Object> model = registerModel("m-py-model", py);
        String modelId = String.valueOf(model.get("id"));
        String approvalId = submitAndApproveStage1(modelId);

        resultCsv = CLASS_RESULT_CSV;
        Map<String, Object> test = executeTest(modelId, "pass", "prediction", "classification");
        assertEquals("RUNNING", String.valueOf(test.get("status")));
        Job.CreateJobRequest sent = JobService.State.lastCreateJobRequest;
        assertNotNull(sent);
        assertEquals("data-sandbox-python-runner", sent.getTasks(0).getAppImage());
        assertTrue(sent.getTasks(0).getTaskInputConfig().contains("numpy"), "allowed_imports 应含白名单 numpy");
        JobService.State.endpointPortName = "py";
        succeedRunningTasks();
        Map<String, Object> done = modelTestService.finalizeIfNeededAndGet(String.valueOf(test.get("id")));
        assertEquals("SUCCEEDED", String.valueOf(done.get("status")));
        assertEquals(0.75, ((Number) ((Map<?, ?>) done.get("metrics")).get("accuracy")).doubleValue(), 1e-6);

        // 非白名单 import 拒绝（绕过 createVersion 直接插版本模拟历史/异常数据）
        Map<String, Object> bad = createArtifact("m-py-bad", "PYTHON");
        String badArtId = String.valueOf(bad.get("id"));
        String badVersionId = "dav-bad-" + System.currentTimeMillis();
        jdbc.update("insert into ds_dev_artifact_version(id,artifact_id,version,content_text,file_path,sha256,size,"
                        + "params_schema,default_params,dependency_names,description,created_by,created_at,deleted)"
                        + " values(?,?,?,?,'','',0,'[]','{}','[]','it-bad','it',?,0)",
                badVersionId, badArtId, 1, "import requests\nprint('x')", now());
        Map<String, Object> badModel = registerModel("m-py-bad-model",
                Map.of("artifactId", badArtId, "artifactVersionId", badVersionId));
        String badModelId = String.valueOf(badModel.get("id"));
        String badApprovalId = submitAndApproveStage1(badModelId);
        assertTrue(!badApprovalId.isEmpty());
        IllegalArgumentException dep = assertThrows(IllegalArgumentException.class,
                () -> executeTest(badModelId, "pass", "prediction", "classification"));
        assertTrue(dep.getMessage().contains(ModelErrors.MODEL_DEPENDENCY_REJECTED), dep.getMessage());
    }

    /** 7. 取消 RUNNING 测试：task + test 均 CANCELLED，Job 被 stop+delete。 */
    @Test
    public void cancelRunningTest() throws IOException {
        Map<String, Object> jar = jarArtifact("m-cancel");
        Map<String, Object> model = registerModel("m-cancel-model", jar);
        String modelId = String.valueOf(model.get("id"));
        submitAndApproveStage1(modelId);

        Map<String, Object> test = executeTest(modelId, "pass", "prediction", "classification");
        String testId = String.valueOf(test.get("id"));
        String taskId = String.valueOf(test.get("task_id"));
        assertEquals("RUNNING", String.valueOf(test.get("status")));
        Map<String, Object> cancelled = modelTestService.cancelTest(testId);
        assertEquals("CANCELLED", String.valueOf(cancelled.get("status")));
        assertEquals("CANCELLED", String.valueOf(jdbc.queryForMap("select status from ds_dev_task where id=?", taskId).get("status")));
        // 已取消不可再取消/重试
        assertThrows(IllegalArgumentException.class, () -> modelTestService.cancelTest(testId));
        assertThrows(IllegalArgumentException.class, () -> modelTestService.retryTest(testId));
    }

    /** 8. 同步 runAndAwait（channel='api'）：SUCCEEDED 返回全量行 + 耗时；结果 CSV 落盘。 */
    @Test
    public void runAndAwaitReturnsResultAndPersistsCsv() {
        String taskId = "dt-api-" + Long.toHexString(System.currentTimeMillis());
        String now = now();
        jdbc.update("insert into ds_dev_task(id,name,description,artifact_id,version,run_mode,exec_type,source_node_id,"
                        + "source_datatable_id,source_relative_uri,params,content_snapshot,dependency_names,channel,status,result_node_id,"
                        + "result_datatable_id,result_preview,result_uri,source_rows,result_rows,error_message,kuscia_job_id,retry_count,"
                        + "created_by,created_at,updated_at,started_at,finished_at,deleted)"
                        + " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,'RUNNING','','','','',0,0,'',?,0,'alice',?,?,?,'',0)",
                taskId, "api-invoke", "", "", 0, "DEV", "JAR", "alice", LABELED_DT, LABELED_URI,
                "{}", "", "[]", "api", "dt-" + taskId, now, now, now);
        JobService.State.jobState = "Succeeded";
        JobService.State.withEndpoints = true;
        resultCsv = "id,name,amount\n1,alice,12.5\n2,bob,3.14\n";
        Map<String, Object> result = devJobExecutor.runAndAwait(taskId);
        assertEquals("SUCCEEDED", String.valueOf(result.get("status")));
        assertEquals(List.of("id", "name", "amount"), result.get("header"));
        assertEquals(2, ((List<?>) result.get("rows")).size());
        assertTrue(((Number) result.get("elapsedMs")).longValue() >= 0);
        assertEquals("", String.valueOf(result.get("errorMessage")));
        // DEV + api 通道仍落盘结果 CSV（供调用取数）
        assertEquals(resultCsv, readFile(findResultUri(taskId)));
        // task 终态
        assertEquals("SUCCEEDED", String.valueOf(jdbc.queryForMap("select status from ds_dev_task where id=?", taskId).get("status")));
    }

    /** 9. 通道隔离：pollDevTasks 绝不含 channel='api'；dev/model 在轮询集内。 */
    @Test
    public void pollSkipsApiChannelTasks() {
        String apiTaskId = "dt-api-poll-" + Long.toHexString(System.currentTimeMillis());
        String modelTaskId = "dt-model-poll-" + Long.toHexString(System.currentTimeMillis());
        String now = now();
        insertTask(apiTaskId, "api", now);
        insertTask(modelTaskId, "model", now);
        JobService.State.jobState = "Failed";
        JobService.State.jobErrMsg = "boom";
        devJobExecutor.pollDevTasks();
        // api 通道不被轮询 → 保持 RUNNING
        assertEquals("RUNNING", String.valueOf(jdbc.queryForMap("select status from ds_dev_task where id=?", apiTaskId).get("status")));
        // model 通道被轮询 → FAILED
        assertEquals("FAILED", String.valueOf(jdbc.queryForMap("select status from ds_dev_task where id=?", modelTaskId).get("status")));
    }

    private void insertTask(String taskId, String channel, String now) {
        jdbc.update("insert into ds_dev_task(id,name,description,artifact_id,version,run_mode,exec_type,source_node_id,"
                        + "source_datatable_id,source_relative_uri,params,content_snapshot,dependency_names,channel,status,result_node_id,"
                        + "result_datatable_id,result_preview,result_uri,source_rows,result_rows,error_message,kuscia_job_id,retry_count,"
                        + "created_by,created_at,updated_at,started_at,finished_at,deleted)"
                        + " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,'RUNNING','','','','',0,0,'',?,0,'alice',?,?,?,'',0)",
                taskId, "it-task", "", "", 0, "DEV", "JAR", "alice", LABELED_DT, LABELED_URI,
                "{}", "", "[]", channel, "dt-" + taskId, now, now, now);
    }
}
