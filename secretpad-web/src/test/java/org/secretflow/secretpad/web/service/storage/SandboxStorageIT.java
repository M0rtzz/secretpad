/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.service.storage;

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
import org.secretflow.secretpad.web.service.dev.DevErrors;
import org.secretflow.secretpad.web.service.dev.DevSqlEngine;
import org.secretflow.secretpad.web.service.governance.CsvUtil;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stage 6 集成测试：沙箱权威库 {@code sandbox_data.db}（SandboxDbService）构建与沙箱 SQLite 计算契约。
 *
 * <p>覆盖：rebuild PROCESSED-only（RAW 禁入沙箱）、幂等重建且保留 result_* 表、executeOnDb 只读强制
 * （INSERT/DROP/PRAGMA 拒绝）、SQL DEV 仅预览 / PROD 结果回填 + 结果数据集注册 + 一键挂载、
 * JAR/PYTHON 提交 createJob payload 注入 {@code jdbc_url/input_table/output_table}、非创建人拒绝。
 * 独立 mock 端口 50057 + SQLite + 临时数据目录；@Scheduled 不运行。</p>
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
        "spring.datasource.default.jdbc-url=jdbc:sqlite:${java.io.tmpdir}/ds-sandbox-storage-it.sqlite",
        "spring.datasource.quartz.jdbc-url=jdbc:h2:${java.io.tmpdir}/ds-sandbox-storage-it-quartz.mv.db;DB_CLOSE_ON_EXIT=FALSE",
        "secretpad.data.dir-path=${java.io.tmpdir}/ds-sandbox-storage-data/",
        "secretpad.data-sandbox.dev.input-rows=10000",
        "secretpad.data-sandbox.dev.sql-limit=50",
        "secretpad.data-sandbox.dev.sql-timeout-seconds=30",
})
public class SandboxStorageIT {

    private static final String SBX = "sbx-it";
    private static final String PROC_LOCAL = "ast-proc-local";
    private static final String PROC_SYNCED = "ast-proc-synced";
    private static final String RAW_REMOTE = "ast-raw-remote";
    private static final String DATA_ROOT = System.getProperty("java.io.tmpdir") + "/ds-sandbox-storage-data";
    private static final List<String> HEADER = List.of("id", "name", "score");
    private static final List<List<String>> ROWS = List.of(
            List.of("1", "alice", "90"),
            List.of("2", "bob", "55"),
            List.of("3", "carol", "70"));

    @Resource
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbc;

    @Resource
    private SandboxDbService sandboxDb;

    @Resource
    private NodeDatasetStore nodeStore;

    @Resource
    private DataDevService dataDev;

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

    private UserContextDTO carol() {
        return UserContextDTO.builder().ownerId("carol").name("carol")
                .platformType(PlatformTypeEnum.CENTER).platformNodeId("carol")
                .ownerType(UserOwnerTypeEnum.CENTER).projectIds(Set.of("p1")).build();
    }

    /* ------------------------------- 生命周期 ------------------------------- */

    @BeforeAll
    public void startMock() throws Exception {
        mockServer = new MockKusciaGrpcServer();
        // 端口 0 = 随机可用端口，避免与宿主环境既有服务冲突
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
        jdbc.update("insert into project_node(project_id,node_id,is_deleted) values('p1','carol',0)");
        jdbc.update("insert into node(node_id,name,control_node_id,type,mode) values('alice','alice-node','master','normal',0)");
        jdbc.update("insert into node(node_id,name,control_node_id,type,mode) values('carol','carol-node','master','normal',0)");
        jdbc.update("insert into ds_sandbox(id,name,owner_id,project_id,image_id,status,expires_at,network_policy,"
                        + "cpu_cores,memory_gb,gpu_count,storage_gb,kuscia_job_id,endpoint,last_error,created_by,"
                        + "created_at,updated_at,deleted) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)",
                SBX, "IT 沙箱", "alice", "p1", "img-secretflow", "RUNNING", "2099-01-01T00:00:00",
                "INTERNAL_ONLY", 1, 2, 0, 10, "", "", "", "alice", now, now);
        insertAsset(PROC_LOCAL, "本地脱敏样本", "alice", "PROCESSED", now);
        insertAsset(PROC_SYNCED, "跨节点脱敏样本", "carol", "PROCESSED", now);
        insertAsset(RAW_REMOTE, "对方原始数据", "carol", "RAW", now);
        // 节点级权威库物化（TABULAR 真实行，本地 + 跨节点同步各一；RAW 不物化）
        nodeStore.materializeExternal(PROC_LOCAL, "alice", "src-local", HEADER, ROWS, "sha-local");
        nodeStore.materializeExternal(PROC_SYNCED, "carol", "src-synced", HEADER, ROWS, "sha-synced");
        insertMount("m1", PROC_LOCAL, "alice", now);
        insertMount("m2", PROC_SYNCED, "carol", now);
        insertMount("m3", RAW_REMOTE, "carol", now);
    }

    private void insertAsset(String id, String name, String provider, String stage, String now) {
        jdbc.update("insert into ds_data_asset(id,name,provider_node_id,processor_node_id,ingestion_type,"
                        + "modality,data_stage,datatable_id,storage_uri,metadata_json,created_by,created_at,"
                        + "updated_at,version,status,deleted) values(?,?,?,?,?,?,?,?,?,?,?,?,?,1,'ACTIVE',0)",
                id, name, provider, provider, "IMPORTED", "TABULAR", stage, id, "node-data://x", "{}",
                "alice", now, now);
    }

    private void insertMount(String id, String assetId, String provider, String now) {
        jdbc.update("insert into ds_sandbox_dataset_mount(id,sandbox_id,asset_id,asset_version,provider_node_id,"
                        + "staging_uri,mount_path,checksum,status,expires_at,created_at,updated_at,deleted)"
                        + " values(?,?,?,?,?,?,?,?,?,?,?,?,0)",
                id, SBX, assetId, 1, provider,
                "node-data://" + NodeDatasetStore.assetTableName(assetId),
                "/data/assets/" + assetId, "", "READY", "", now, now);
    }

    /* ------------------------------- 工具 ------------------------------- */

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

    private String procTable() {
        return NodeDatasetStore.assetTableName(PROC_LOCAL);
    }

    /* ------------------------------- mock ------------------------------- */

    /** DomainData mock：create 记录调用并返回 OK（沙箱 SQL PROD 结果注册）。 */
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

    /** 1. rebuild：PROCESSED 物化进沙箱库，RAW 源数据禁入；目录/库记录/预览/下载齐备。 */
    @Test
    public void rebuildFiltersRawAndMaterializesProcessed() throws Exception {
        sandboxDb.rebuild(SBX);
        Path db = sandboxDb.sandboxDbPath(SBX);
        assertTrue(Files.exists(db), "sandbox_data.db 应被创建");
        assertTrue(SqliteTableLoader.tableExists(db, NodeDatasetStore.assetTableName(PROC_LOCAL)));
        assertTrue(SqliteTableLoader.tableExists(db, NodeDatasetStore.assetTableName(PROC_SYNCED)));
        assertFalse(SqliteTableLoader.tableExists(db, NodeDatasetStore.assetTableName(RAW_REMOTE)),
                "RAW 源数据禁入沙箱库");

        Map<String, Object> dir = sandboxDb.directory(SBX);
        assertEquals(SBX, String.valueOf(dir.get("sandboxId")));
        assertEquals(2, ((List<?>) dir.get("items")).size(), "目录应恰有 2 个 PROCESSED 挂载表");
        assertEquals(2L, count("select count(1) from ds_sandbox_data_dir where sandbox_id=? and kind='MOUNT'", SBX));
        Map<String, Object> rec = jdbc.queryForMap("select table_count,status from ds_sandbox_db where sandbox_id=?", SBX);
        assertEquals(2, ((Number) rec.get("table_count")).intValue());
        assertEquals("READY", String.valueOf(rec.get("status")));

        // 预览：schema 3 列 + 总行数 3
        Map<String, Object> prev = sandboxDb.previewTable(SBX, procTable(), 20);
        assertEquals(procTable(), String.valueOf(prev.get("tableName")));
        assertEquals(3, ((List<?>) prev.get("schema")).size());
        assertEquals(3L, ((Number) prev.get("totalRows")).longValue());
        assertEquals(3, ((List<?>) prev.get("rows")).size());

        // 全量读取（计算源）
        Map<String, Object> read = sandboxDb.readTable(SBX, procTable());
        assertEquals(HEADER, read.get("header"));
        assertEquals(3, ((List<?>) read.get("rows")).size());

        // 清单维度校验
        assertTrue(sandboxDb.hasTable(SBX, procTable()));
        assertTrue(sandboxDb.hasTable(SBX, NodeDatasetStore.assetTableName(PROC_SYNCED)));
        assertFalse(sandboxDb.hasTable(SBX, NodeDatasetStore.assetTableName(RAW_REMOTE)));

        // 下载返回合法 SQLite 文件
        byte[] dl = sandboxDb.downloadBytes(SBX);
        assertTrue(new String(dl, StandardCharsets.ISO_8859_1).startsWith("SQLite format 3"));
    }

    /** 2. rebuild 幂等：重建保留 result_* 结果表与清单；挂载变更后重跑不丢开发产出。 */
    @Test
    public void rebuildIdempotentAndPreservesResultTables() {
        sandboxDb.rebuild(SBX);
        Map<String, Object> backfilled = sandboxDb.backfillResultTable(SBX, "t1", "任务一",
                List.of("k", "c"), List.of(List.of("A", "2")));
        String resultTable = String.valueOf(backfilled.get("tableName"));
        assertEquals("result_t1", resultTable);

        sandboxDb.rebuild(SBX);
        assertTrue(SqliteTableLoader.tableExists(sandboxDb.sandboxDbPath(SBX), resultTable),
                "重建后 result_* 结果表应保留");
        assertTrue(sandboxDb.hasTable(SBX, resultTable));
        assertEquals(2L, count("select count(1) from ds_sandbox_data_dir where sandbox_id=? and kind='MOUNT'", SBX));
        assertEquals(1L, count("select count(1) from ds_sandbox_data_dir where sandbox_id=? and kind='RESULT'", SBX));
    }

    /** 3. executeOnDb 只读强制：INSERT/DROP/PRAGMA 全拒；读查询正常。 */
    @Test
    public void executeOnDbEnforcesReadOnly() {
        sandboxDb.rebuild(SBX);
        Path db = sandboxDb.sandboxDbPath(SBX);
        String table = procTable();
        assertThrows(Exception.class, () -> DevSqlEngine.executeOnDb(db,
                "INSERT INTO " + table + "(id,name,score) VALUES('9','x','1')", Map.of(), 50, 30));
        assertThrows(Exception.class, () -> DevSqlEngine.executeOnDb(db,
                "DROP TABLE " + table, Map.of(), 50, 30));
        assertThrows(Exception.class, () -> DevSqlEngine.executeOnDb(db,
                "PRAGMA journal_mode=WAL", Map.of(), 50, 30));
        DevSqlEngine.SqlResult ok = DevSqlEngine.executeOnDb(db,
                "SELECT count(*) c FROM " + table, Map.of(), 50, 30);
        assertEquals("3", ok.rows().get(0).get(0));
    }

    /** 4. 沙箱 SQL DEV：仅预览+日志，不注册结果、不回填沙箱库。 */
    @Test
    public void sandboxSqlDevPreviewOnly() {
        sandboxDb.rebuild(SBX);
        Map<String, Object> task = submitSql("DEV", "SELECT name, score FROM " + procTable() + " WHERE score >= 60");
        assertEquals("SUCCEEDED", String.valueOf(task.get("status")));
        assertEquals("", String.valueOf(task.get("result_table_name")));
        assertEquals("", String.valueOf(task.get("result_datatable_id")));
        assertEquals(2, ((Number) task.get("result_rows")).intValue());
        assertEquals(0, DevDomainDataService.created.size(), "DEV 不注册结果数据集");
        assertEquals(0L, count("select count(1) from ds_sandbox_data_dir where sandbox_id=? and kind='RESULT'", SBX));
    }

    /** 5. 沙箱 SQL PROD：结果表回填沙箱库 + 结果数据集注册 + 目录 RESULT 行；结果不可挂载、不可作计算源，仅预览/导出。 */
    @Test
    public void sandboxSqlProdBackfillsResultAndBlocksMount() {
        sandboxDb.rebuild(SBX);
        Map<String, Object> task = submitSql("PROD", "SELECT name, score FROM " + procTable() + " WHERE score >= 60");
        String taskId = String.valueOf(task.get("id"));
        assertEquals("SUCCEEDED", String.valueOf(task.get("status")));
        String resultTable = String.valueOf(task.get("result_table_name"));
        assertTrue(resultTable.startsWith("result_"), "结果表名应形如 result_<taskId>");
        assertTrue(SqliteTableLoader.tableExists(sandboxDb.sandboxDbPath(SBX), resultTable),
                "结果表应回填 sandbox_data.db");
        assertEquals(1, DevDomainDataService.created.size(), "PROD 应注册结果数据集");
        assertEquals(1L, count("select count(1) from ds_sandbox_data_dir where sandbox_id=? and kind='RESULT'", SBX));

        // 沙箱结果不可挂载到项目
        String resultDt = String.valueOf(task.get("result_datatable_id"));
        assertTrue(!resultDt.isEmpty());
        IllegalStateException mountErr = assertThrows(IllegalStateException.class,
                () -> dataDev.mountResult(Map.of("taskId", taskId, "projectId", "p1")));
        assertTrue(String.valueOf(mountErr.getMessage()).contains(DevErrors.DEV_RESULT_NOT_MOUNTABLE),
                mountErr.getMessage());

        // 结果表不可作为沙箱计算源
        assertTrue(sandboxDb.isResultTable(SBX, resultTable), "result_ 表应判定为结果表");
        assertFalse(sandboxDb.isResultTable(SBX, procTable()), "挂载表不应判定为结果表");
        Map<String, Object> consume = new LinkedHashMap<>();
        consume.put("sandboxId", SBX);
        consume.put("name", "consume-result");
        consume.put("runMode", "DEV");
        consume.put("execType", "SQL");
        consume.put("sql", "SELECT * FROM " + resultTable);
        consume.put("sourceTable", resultTable);
        IllegalArgumentException consumeErr = assertThrows(IllegalArgumentException.class,
                () -> dataDev.submitSandboxTask(consume));
        assertTrue(String.valueOf(consumeErr.getMessage()).contains(DevErrors.DEV_RESULT_NOT_CONSUMABLE),
                consumeErr.getMessage());

        // SQL 内深引用 result_ 表也拒绝
        IllegalArgumentException sqlDeepErr = assertThrows(IllegalArgumentException.class,
                () -> DevSqlEngine.executeOnDb(sandboxDb.sandboxDbPath(SBX),
                        "SELECT * FROM src WHERE x IN (SELECT y FROM " + resultTable + ")", Map.of(), 50, 30));
        assertTrue(String.valueOf(sqlDeepErr.getMessage()).contains(DevErrors.DEV_RESULT_NOT_CONSUMABLE),
                sqlDeepErr.getMessage());

        // 结果表仍可预览 + 单表 CSV 导出
        assertTrue(((List<?>) sandboxDb.previewTable(SBX, resultTable, 20).get("rows")).size() > 0, "结果表可预览");
        String csv = new String(sandboxDb.readTableCsv(SBX, resultTable), StandardCharsets.UTF_8);
        assertTrue(csv.startsWith("name,score"), "导出 CSV 应以表头开头，实际: " + csv);
    }

    /** 5b. 单表 CSV 导出：挂载表 readTableCsv 往返与挂载表/结果表均可导出。 */
    @Test
    public void tableExportRoundTrips() {
        sandboxDb.rebuild(SBX);
        String csv = new String(sandboxDb.readTableCsv(SBX, procTable()), StandardCharsets.UTF_8);
        List<List<String>> parsed = CsvUtil.parse(csv);
        assertEquals(4, parsed.size(), "表头 + 3 行");
        assertEquals(HEADER, parsed.get(0));
        assertEquals(ROWS, parsed.subList(1, parsed.size()));
        // 未知表拒绝
        assertThrows(IllegalArgumentException.class, () -> sandboxDb.readTableCsv(SBX, "nope_table"));
    }

    private Map<String, Object> submitSql(String runMode, String sql) {
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("sandboxId", SBX);
        request.put("name", "it-sbx-" + runMode.toLowerCase());
        request.put("runMode", runMode);
        request.put("execType", "SQL");
        request.put("sql", sql);
        request.put("sourceTable", procTable());
        return dataDev.submitSandboxTask(request);
    }

    /** 6. 沙箱 JAR 提交：createJob payload 注入 jdbc_url/input_table/output_table。 */
    @Test
    public void sandboxJarSubmitCarriesJdbcContract() throws IOException {
        sandboxDb.rebuild(SBX);
        Map<String, Object> art = dataDev.createArtifact(Map.of("name", "it-jar-sbx", "type", "JAR"));
        String artId = String.valueOf(art.get("id"));
        Map<String, Object> v = dataDev.uploadJarVersion(artId, validJar(), "[]", "{}", "", null);
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("sandboxId", SBX);
        request.put("name", "it-jar-sbx-run");
        request.put("runMode", "PROD");
        request.put("execType", "JAR");
        request.put("artifactId", artId);
        request.put("version", ((Number) v.get("version")).intValue());
        request.put("sourceTable", procTable());
        request.put("outputTable", "out_jar");
        Map<String, Object> task = dataDev.submitSandboxTask(request);
        String taskId = String.valueOf(task.get("id"));
        assertEquals("RUNNING", String.valueOf(task.get("status")));
        Job.CreateJobRequest createReq = JobService.State.lastCreateJobRequest;
        assertNotNull(createReq);
        String config = createReq.getTasks(0).getTaskInputConfig();
        assertTrue(config.contains("\"jdbc_url\"") && config.contains("jdbc:sqlite:/workspace/sandbox_data.db"),
                "应注入 jdbc_url 契约，实际: " + config);
        assertTrue(config.contains("\"input_table\"") && config.contains(procTable()),
                "应注入 input_table，实际: " + config);
        assertTrue(config.contains("\"output_table\"") && config.contains("out_jar"),
                "应注入 output_table，实际: " + config);
        dataDev.cancelTask(taskId);
    }

    /** 7. 沙箱 PYTHON 提交：同样注入 jdbc 契约；源表导出 CSV base64 通道。 */
    @Test
    public void sandboxPythonSubmitCarriesJdbcContract() {
        sandboxDb.rebuild(SBX);
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("sandboxId", SBX);
        request.put("name", "it-py-sbx-run");
        request.put("runMode", "PROD");
        request.put("execType", "PYTHON");
        request.put("script", "import numpy as np\nprint(np.array([1]))");
        request.put("sourceTable", procTable());
        request.put("outputTable", "out_py");
        Map<String, Object> task = dataDev.submitSandboxTask(request);
        String taskId = String.valueOf(task.get("id"));
        assertEquals("RUNNING", String.valueOf(task.get("status")));
        Job.CreateJobRequest createReq = JobService.State.lastCreateJobRequest;
        assertNotNull(createReq);
        String config = createReq.getTasks(0).getTaskInputConfig();
        assertTrue(config.contains("\"jdbc_url\"") && config.contains("jdbc:sqlite:/workspace/sandbox_data.db"),
                "应注入 jdbc_url 契约，实际: " + config);
        assertTrue(config.contains("\"input_table\"") && config.contains(procTable()), "应注入 input_table");
        assertTrue(config.contains("input_csv_b64"), "应保留 CSV base64 输入通道");
        dataDev.cancelTask(taskId);
    }

    /** 8. 权限：非创建人提交/预览被拒；未知表被拒。 */
    @Test
    public void nonCreatorRejectedAndUnknownTableRejected() {
        sandboxDb.rebuild(SBX);
        // 未知表
        Map<String, Object> bad = new LinkedHashMap<>();
        bad.put("sandboxId", SBX);
        bad.put("name", "bad");
        bad.put("runMode", "DEV");
        bad.put("execType", "SQL");
        bad.put("sql", "SELECT 1");
        bad.put("sourceTable", "nope_table");
        IllegalArgumentException e1 = assertThrows(IllegalArgumentException.class,
                () -> dataDev.submitSandboxTask(bad));
        assertTrue(String.valueOf(e1.getMessage()).contains("沙箱内无此表"));

        // 非创建人
        UserContext.setBaseUser(carol());
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("sandboxId", SBX);
        req.put("name", "carol-run");
        req.put("runMode", "DEV");
        req.put("execType", "SQL");
        req.put("sql", "SELECT 1");
        req.put("sourceTable", procTable());
        assertThrows(IllegalArgumentException.class, () -> dataDev.submitSandboxTask(req));
        assertThrows(IllegalArgumentException.class, () -> dataDev.previewSandboxTable(SBX, procTable(), 10));
    }

    /** 9. RAW 挂载数据不可作为计算源（rebuild 已禁入，目录与任务提交双保险）。 */
    @Test
    public void rawMountNeverReachesCompute() {
        sandboxDb.rebuild(SBX);
        assertFalse(sandboxDb.hasTable(SBX, NodeDatasetStore.assetTableName(RAW_REMOTE)));
        Map<String, Object> req = new LinkedHashMap<>();
        req.put("sandboxId", SBX);
        req.put("name", "raw-run");
        req.put("runMode", "DEV");
        req.put("execType", "SQL");
        req.put("sql", "SELECT 1");
        req.put("sourceTable", NodeDatasetStore.assetTableName(RAW_REMOTE));
        assertThrows(IllegalArgumentException.class, () -> dataDev.submitSandboxTask(req));
    }

    /** 10. CSV 往返：readTable → CsvUtil 可重新解析（计算源契约）。 */
    @Test
    public void readTableRoundTripsThroughCsv() {
        sandboxDb.rebuild(SBX);
        Map<String, Object> read = sandboxDb.readTable(SBX, procTable());
        @SuppressWarnings("unchecked")
        List<String> header = new ArrayList<>((List<String>) read.get("header"));
        @SuppressWarnings("unchecked")
        List<List<String>> rows = new ArrayList<>((List<List<String>>) read.get("rows"));
        String csv = CsvUtil.toCsv(header, rows);
        List<List<String>> parsed = CsvUtil.parse(csv);
        assertEquals(4, parsed.size(), "表头 + 3 行");
        assertEquals(ROWS, parsed.subList(1, parsed.size()));
    }
}
