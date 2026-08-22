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
import org.secretflow.secretpad.web.service.storage.NodeDatasetStore;
import org.secretflow.secretpad.web.service.storage.SandboxDbService;
import org.secretflow.secretpad.web.service.util.TestSchemaMigrator;

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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Z-05 Stage 1 沙箱函数开发（UDF）集成测试：FUNCTION 全链路。
 *
 * <p>覆盖：FUNCTION inline 提交（createJob payload 注入 {@code sandbox_db_b64} 整库快照 +
 * python-runner 包装器脚本 + {@code allowed_imports}）、FUNCTION 制品版本（函数三列落库）与
 * 制品引用提交、入参/依赖白名单校验、沙箱重试（FUNCTION 由新列重生成包装器重派发）。
 * 独立 mock 端口 50056 + SQLite + 临时数据目录；@Scheduled 不运行。</p>
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
        "spring.datasource.default.jdbc-url=jdbc:sqlite:${java.io.tmpdir}/ds-dev-fn-it.sqlite",
        "spring.datasource.quartz.jdbc-url=jdbc:h2:${java.io.tmpdir}/ds-dev-fn-it-quartz.mv.db;DB_CLOSE_ON_EXIT=FALSE",
        "secretpad.data.dir-path=${java.io.tmpdir}/ds-dev-fn-data/",
        "secretpad.data-sandbox.dev.input-rows=10000",
        "secretpad.data-sandbox.dev.max-retries=3",
        "secretpad.data-sandbox.dev.sql-limit=50",
        "secretpad.data-sandbox.dev.result-preview-rows=10",
})
public class DataDevSandboxFunctionIT {

    private static final String SBX = "sbx-fn";
    private static final String PROC = "ast-fn-proc";
    private static final String DATA_ROOT = System.getProperty("java.io.tmpdir") + "/ds-dev-fn-data";

    /** 源表列对齐资产样例 asset_ac03e81da460（脱敏表：金额/交易字段齐全，供 UDF 真计算）。 */
    private static final List<String> HEADER = List.of("id", "account_no", "name", "branch", "card_type",
            "category", "balance", "trans_amount", "trans_date", "trans_type", "memo");
    private static final List<List<String>> ROWS = List.of(
            List.of("1", "acct-001", "张三", "武广支行", "白金", "A", "12000.00", "4500.00", "2026-03-05", "POS消费", ""),
            List.of("2", "acct-002", "李四", "中南支行", "金卡", "B", "68000.00", "1200.00", "2026-03-08", "转账", ""),
            List.of("3", "acct-003", "王五", "武广支行", "普通", "A", "7500.00", "6200.00", "2026-04-12", "大额", ""),
            List.of("4", "acct-004", "赵六", "光谷支行", "白金", "C", "32000.00", "2800.00", "2026-04-20", "POS消费", ""),
            List.of("5", "acct-005", "孙七", "中南支行", "金卡", "B", "9800.00", "5200.00", "2026-05-01", "取现", ""));

    /** 样例函数：资金风险评分 UDF（含 numpy 依赖，验证白名单放行 + allowed_imports）。 */
    private static final String FN_SOURCE =
            "import numpy as np\n"
                    + "\n"
                    + "def risk_score(balance, trans_amount):\n"
                    + "    \"\"\"资金风险评分：余额越低、单笔交易越大 → 风险越高（3=高/2=中/1=低）\"\"\"\n"
                    + "    score = 1\n"
                    + "    if balance < 20000:\n"
                    + "        score += 1\n"
                    + "    if trans_amount > 3000:\n"
                    + "        score += 1\n"
                    + "    if trans_amount > 5000 or balance < 8000:\n"
                    + "        score += 1\n"
                    + "    return min(score, 3)\n";

    private static final String FN_SQL =
            "SELECT account_no, branch, category, card_type, balance, trans_amount,\n"
                    + "       risk_score(balance, trans_amount) AS risk_level,\n"
                    + "       CASE WHEN trans_amount > 3000 THEN 'Y' ELSE 'N' END AS big_txn\n"
                    + "FROM " + NodeDatasetStore.assetTableName(PROC) + "\n"
                    + "ORDER BY risk_level DESC, trans_amount DESC";

    @Resource
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbc;

    @Resource
    private DataDevService dataDev;

    @Resource
    private SandboxDbService sandboxDb;

    @Resource
    private NodeDatasetStore nodeStore;

    @Resource
    private DynamicKusciaChannelProvider channelProvider;

    private MockKusciaGrpcServer mockServer;

    @DynamicPropertySource
    static void schemaProps(DynamicPropertyRegistry registry) {
        registry.add("flyway.default.locations", () -> TestSchemaMigrator.dedupedLocation("center"));
    }

    /* ------------------------------- 角色 ------------------------------- */

    private UserContextDTO alice() {
        return UserContextDTO.builder().ownerId("alice").name("alice")
                .platformType(PlatformTypeEnum.CENTER).platformNodeId("alice")
                .ownerType(UserOwnerTypeEnum.CENTER).projectIds(Set.of("p1")).build();
    }

    /* ------------------------------- 生命周期 ------------------------------- */

    @BeforeAll
    public void startMock() throws Exception {
        mockServer = new MockKusciaGrpcServer();
        mockServer.start(0, KusciaProtocolEnum.NOTLS, List.of(
                new DevDomainDataService(), new JobService(), new HealthService()));
        KusciaGrpcConfig config = mockServer.buildKusciaGrpcConfig("kuscia-system");
        config.setPort(mockServer.getPort());
        channelProvider.registerKuscia(config);
    }

    @AfterAll
    public void stopMock() {
        mockServer.stop();
    }

    @BeforeEach
    public void reset() throws IOException {
        for (String t : new String[]{"ds_sandbox_data_dir", "ds_sandbox_db", "ds_sandbox_dataset_mount",
                "ds_dev_run_log", "ds_dev_task", "ds_dev_artifact_version", "ds_dev_artifact",
                "ds_node_dataset"}) {
            jdbc.update("delete from " + t);
        }
        jdbc.update("delete from ds_data_asset where id like 'ast-%'");
        jdbc.update("delete from project_datatable where project_id in ('p1','p2')");
        jdbc.update("delete from ds_alert_event");
        jdbc.update("delete from ds_unified_log where resource_type='DEV_TASK' or action like 'DEV_%'");
        jdbc.update("delete from ds_sandbox");
        jdbc.update("delete from ds_sandbox_snapshot");
        jdbc.update("delete from node where node_id in ('alice','carol')");
        jdbc.update("delete from project_node where project_id in ('p1','p2')");
        jdbc.update("delete from project where project_id in ('p1','p2')");
        deleteRecursively(Path.of(DATA_ROOT));
        DevDomainDataService.created.clear();
        DevDomainDataService.createCode = KusciaAPIConstants.OK;
        JobService.State.createJobCode = KusciaAPIConstants.OK;
        JobService.State.lastCreateJobRequest = null;
        UserContext.setBaseUser(alice());
        insertBase();
    }

    private void insertBase() {
        String now = LocalDateTime.now().toString();
        jdbc.update("insert into project(project_id,name,owner_id,is_deleted) values('p1','IT Project','alice',0)");
        jdbc.update("insert into project_node(project_id,node_id,is_deleted) values('p1','alice',0)");
        jdbc.update("insert into node(node_id,name,control_node_id,type,mode) values('alice','alice-node','master','normal',0)");
        jdbc.update("insert into ds_sandbox(id,name,owner_id,project_id,image_id,status,expires_at,network_policy,"
                        + "cpu_cores,memory_gb,gpu_count,storage_gb,kuscia_job_id,endpoint,last_error,created_by,"
                        + "created_at,updated_at,deleted) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)",
                SBX, "IT 函数沙箱", "alice", "p1", "img-secretflow", "RUNNING", "2099-01-01T00:00:00",
                "INTERNAL_ONLY", 1, 2, 0, 10, "", "", "", "alice", now, now);
        jdbc.update("insert into ds_data_asset(id,name,provider_node_id,processor_node_id,ingestion_type,"
                        + "modality,data_stage,datatable_id,storage_uri,metadata_json,created_by,created_at,"
                        + "updated_at,version,status,deleted) values(?,?,?,?,?,?,?,?,?,?,?,?,?,1,'ACTIVE',0)",
                PROC, "脱敏样本", "alice", "alice", "IMPORTED", "TABULAR", "PROCESSED", PROC,
                "node-data://x", "{}", "alice", now, now);
        nodeStore.materializeExternal(PROC, "alice", "src-local", HEADER, ROWS, "sha-local");
        jdbc.update("insert into ds_sandbox_dataset_mount(id,sandbox_id,asset_id,asset_version,provider_node_id,"
                        + "staging_uri,mount_path,checksum,status,expires_at,created_at,updated_at,deleted)"
                        + " values(?,?,?,?,?,?,?,?,?,?,?,?,0)",
                "m1", SBX, PROC, 1, "alice",
                "node-data://" + NodeDatasetStore.assetTableName(PROC),
                "/data/assets/" + PROC, "", "READY", "", now, now);
    }

    /* ------------------------------- 工具 ------------------------------- */

    private String procTable() {
        return NodeDatasetStore.assetTableName(PROC);
    }

    private long count(String sql, Object... args) {
        Long value = jdbc.queryForObject(sql, Long.class, args);
        return value == null ? 0 : value;
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var walk = Files.walk(root)) {
            for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(p);
            }
        }
    }

    /** inline FUNCTION 提交请求。 */
    private Map<String, Object> functionRequest(String runMode, String fnSource, String sql) {
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("sandboxId", SBX);
        request.put("name", "it-fn-" + runMode.toLowerCase());
        request.put("runMode", runMode);
        request.put("execType", "FUNCTION");
        request.put("functionName", "risk_score");
        request.put("functionNargs", 2);
        request.put("functionSource", fnSource);
        request.put("sql", sql);
        request.put("sourceTable", procTable());
        request.put("outputTable", "out_fn");
        return request;
    }

    private void assertDbSnapshotInConfig(String config) {
        assertTrue(config.contains("\"sandbox_db_b64\""), "应注入 sandbox_db_b64，实际: " + config);
        // 解码整库快照必须是合法 SQLite 文件
        int begin = config.indexOf("\"sandbox_db_b64\":\"") + "\"sandbox_db_b64\":\"".length();
        int end = config.indexOf('"', begin);
        byte[] db = Base64.getDecoder().decode(config.substring(begin, end));
        assertTrue(db.length >= 16, "解码字节数应 ≥ SQLite 魔数长度，实际 " + db.length);
        String magic = new String(db, 0, 15, java.nio.charset.StandardCharsets.US_ASCII);
        assertEquals("SQLite format 3", magic, "解码应为合法 SQLite 文件");
    }

    private void assertWrapperInConfig(String config, String fnName, String expectedOutputTable) {
        assertTrue(config.contains("\"script\""), "FUNCTION 走 python-runner，应含 script，实际: " + config);
        assertTrue(config.contains("conn.create_function('" + fnName + "', 2, " + fnName + ")"),
                "包装器应含 create_function 注册: " + config);
        assertTrue(config.contains("PRAGMA query_only=ON"), "包装器应含只读强制: " + config);
        assertTrue(config.contains("?mode=ro"), "包装器应只读打开 DB: " + config);
        assertTrue(config.contains("\"allowed_imports\":[\"numpy\"]"), "函数体 import numpy → allowed_imports，实际: " + config);
        assertTrue(config.contains("\"jdbc_url\":\"jdbc:sqlite:/workspace/sandbox_data.db\""), config);
        assertTrue(config.contains("\"input_table\":\"" + procTable() + "\""), config);
        if (expectedOutputTable != null) {
            assertTrue(config.contains("\"output_table\":\"" + expectedOutputTable + "\""), config);
        }
    }

    /* ------------------------------- mock ------------------------------- */

    /** DomainData mock：create 记录调用并返回 OK（沙箱 PROD 结果注册预留）。 */
    public static class DevDomainDataService extends DomainDataServiceGrpc.DomainDataServiceImplBase {
        static volatile int createCode = KusciaAPIConstants.OK;
        static final List<Domaindata.CreateDomainDataRequest> created = new CopyOnWriteArrayList<>();

        @Override
        public void createDomainData(Domaindata.CreateDomainDataRequest request,
                StreamObserver<Domaindata.CreateDomainDataResponse> responseObserver) {
            created.add(request);
            responseObserver.onNext(Domaindata.CreateDomainDataResponse.newBuilder()
                    .setStatus(Common.Status.newBuilder().setCode(createCode).setMessage("success").build()).build());
            responseObserver.onCompleted();
        }
    }

    /* ------------------------------- 用例 ------------------------------- */

    /** 1. FUNCTION inline 提交：RUNNING + DB 快照 + 包装器 + 白名单；函数列持久化。 */
    @Test
    public void inlineFunctionSubmitCarriesDbSnapshotAndWrapper() {
        sandboxDb.rebuild(SBX);
        Map<String, Object> task = dataDev.submitSandboxTask(functionRequest("PROD", FN_SOURCE, FN_SQL));
        String taskId = String.valueOf(task.get("id"));
        assertEquals("RUNNING", String.valueOf(task.get("status")));
        assertEquals("dt-", taskId.substring(0, 3));
        Job.CreateJobRequest createReq = JobService.State.lastCreateJobRequest;
        assertNotNull(createReq, "FUNCTION 应触发 createJob");
        assertEquals("data-sandbox-python-runner", createReq.getTasks(0).getAppImage());
        String config = createReq.getTasks(0).getTaskInputConfig();
        assertDbSnapshotInConfig(config);
        assertWrapperInConfig(config, "risk_score", "out_fn");
        // 函数列持久化到 ds_dev_task 备查/重试
        assertEquals("risk_score", String.valueOf(jdbc.queryForMap(
                "select function_name from ds_dev_task where id=?", taskId).get("function_name")));
        assertEquals(2, ((Number) jdbc.queryForMap(
                "select function_nargs from ds_dev_task where id=?", taskId).get("function_nargs")).intValue());
        assertTrue(String.valueOf(jdbc.queryForMap(
                "select function_source from ds_dev_task where id=?", taskId).get("function_source")).contains("def risk_score"));
        assertTrue(String.valueOf(jdbc.queryForMap(
                "select sql_template from ds_dev_task where id=?", taskId).get("sql_template")).contains("risk_score(balance"));
        assertEquals(procTable(), String.valueOf(jdbc.queryForMap(
                "select source_table_name from ds_dev_task where id=?", taskId).get("source_table_name")));
        dataDev.cancelTask(taskId);
    }

    /** 2. FUNCTION 制品版本（函数三列）落库 + 制品引用提交。 */
    @Test
    public void artifactFunctionVersionPersistsAndSubmitByRef() {
        sandboxDb.rebuild(SBX);
        Map<String, Object> art = dataDev.createArtifact(Map.of("name", "it-udf", "type", "FUNCTION", "description", "it"));
        String artId = String.valueOf(art.get("id"));
        Map<String, Object> v = dataDev.createVersion(Map.of(
                "artifactId", artId,
                "contentText", FN_SOURCE,
                "functionName", "risk_score",
                "functionNargs", 2,
                "sqlTemplate", FN_SQL,
                "dependencyNames", List.of("numpy")));
        assertEquals(1, ((Number) v.get("version")).intValue());
        // 函数三列落库
        assertEquals("risk_score", String.valueOf(jdbc.queryForMap(
                "select function_name from ds_dev_artifact_version where artifact_id=?", artId).get("function_name")));
        assertEquals(2, ((Number) jdbc.queryForMap(
                "select function_nargs from ds_dev_artifact_version where artifact_id=?", artId).get("function_nargs")).intValue());
        assertTrue(String.valueOf(jdbc.queryForMap(
                "select sql_template from ds_dev_artifact_version where artifact_id=?", artId).get("sql_template")).contains("risk_score"));
        // 制品引用提交：resolveFunctionSpec 从版本读函数定义
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("sandboxId", SBX);
        request.put("name", "it-fn-byref");
        request.put("runMode", "DEV");
        request.put("execType", "FUNCTION");
        request.put("artifactId", artId);
        request.put("version", 1);
        request.put("sourceTable", procTable());
        Map<String, Object> task = dataDev.submitSandboxTask(request);
        assertEquals("RUNNING", String.valueOf(task.get("status")));
        String config = JobService.State.lastCreateJobRequest.getTasks(0).getTaskInputConfig();
        assertWrapperInConfig(config, "risk_score", null);
        assertTrue(String.valueOf(jdbc.queryForMap(
                "select function_source from ds_dev_task where id=?", task.get("id")).get("function_source")).contains("def risk_score"));
        dataDev.cancelTask(String.valueOf(task.get("id")));
    }

    /** 3. 校验：inline 缺字段 / 非白名单依赖 / 非法制品类型 / 非法函数名。 */
    @Test
    public void functionValidationRejectsBadInput() {
        sandboxDb.rebuild(SBX);
        // 缺 sql
        Map<String, Object> noSql = functionRequest("DEV", FN_SOURCE, null);
        assertThrows(IllegalArgumentException.class, () -> dataDev.submitSandboxTask(noSql));
        // 非白名单依赖（requests 不在白名单）
        Map<String, Object> badDep = functionRequest("DEV",
                "import requests\n\ndef risk_score(a, b):\n    return 1", FN_SQL);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> dataDev.submitSandboxTask(badDep));
        assertTrue(e.getMessage().contains(DevErrors.DEV_DEPENDENCY_REJECTED), e.getMessage());
        // 非法函数名（Python 标识符校验在包装器生成前触发）
        Map<String, Object> badName = functionRequest("DEV", FN_SOURCE, FN_SQL);
        badName.put("functionName", "1risk");
        assertThrows(IllegalArgumentException.class, () -> dataDev.submitSandboxTask(badName));
        // FUNCTION 制品版本缺函数列
        Map<String, Object> art = dataDev.createArtifact(Map.of("name", "it-udf-bad", "type", "FUNCTION"));
        assertThrows(IllegalArgumentException.class, () -> dataDev.createVersion(Map.of(
                "artifactId", String.valueOf(art.get("id")), "contentText", FN_SOURCE)));
    }

    /** 4. 沙箱 FUNCTION 重试：FAILED → retryTask 重读函数列重生成包装器重派发。 */
    @Test
    public void sandboxFunctionRetryRegeneratesWrapper() {
        sandboxDb.rebuild(SBX);
        String now = LocalDateTime.now().toString();
        String taskId = "dt-fn-retry";
        jdbc.update("insert into ds_dev_task(id,name,run_mode,exec_type,source_node_id,source_datatable_id,"
                        + "source_relative_uri,params,content_snapshot,dependency_names,status,sandbox_id,"
                        + "source_table_name,output_table_name,function_name,function_nargs,function_source,"
                        + "sql_template,retry_count,created_by,created_at,updated_at,deleted)"
                        + " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?,?,?,0)",
                taskId, "it-fn-retry", "PROD", "FUNCTION", "alice", procTable(),
                "sandbox-db://" + procTable(), "{}", "", "[]", "FAILED", SBX,
                procTable(), "out_retry", "risk_score", 2, FN_SOURCE, FN_SQL, "alice", now, now);
        Map<String, Object> retried = dataDev.retryTask(taskId);
        assertEquals("RUNNING", String.valueOf(retried.get("status")));
        assertEquals(1, ((Number) retried.get("retry_count")).intValue());
        assertEquals("", String.valueOf(retried.get("error_message")));
        Job.CreateJobRequest createReq = JobService.State.lastCreateJobRequest;
        assertNotNull(createReq, "重试应重发 createJob");
        assertWrapperInConfig(createReq.getTasks(0).getTaskInputConfig(), "risk_score", "out_retry");
    }
}
