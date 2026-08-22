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
import org.secretflow.secretpad.web.SecretPadApplication;
import org.secretflow.secretpad.web.service.storage.NodeDatasetStore;
import org.secretflow.secretpad.web.service.storage.SandboxDbService;
import org.secretflow.secretpad.web.service.util.TestSchemaMigrator;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Z-05 沙箱 DB 快照大小上限集成测试：整库 base64 送 pod 前超限 → {@code DEV_INPUT_TOO_LARGE}。
 *
 * <p>{@code sandbox_db_b64} 快照上限 {@code secretpad.data-sandbox.dev.sandbox-db-bytes}=2048 字节，
 * 重建后的 sandbox_data.db 必然超限 → FUNCTION/JAR/PYTHON 沙箱提交在 createJob 之前即被拦截，
 * 任务标记 FAILED 且 error_message 含 DEV_INPUT_TOO_LARGE（校验先于 kuscia 门禁，无需 mock 服务）。</p>
 */
@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
@ActiveProfiles("test")
@SpringBootTest(classes = SecretPadApplication.class)
@TestPropertySource(properties = {
        "kuscia.nodes=",
        "secretpad.data-sandbox.kuscia.enabled=false",
        "secretpad.node-id=kuscia-system",
        "spring.datasource.default.jdbc-url=jdbc:sqlite:${java.io.tmpdir}/ds-dev-db-limit-it.sqlite",
        "spring.datasource.quartz.jdbc-url=jdbc:h2:${java.io.tmpdir}/ds-dev-db-limit-it-quartz.mv.db;DB_CLOSE_ON_EXIT=FALSE",
        "secretpad.data.dir-path=${java.io.tmpdir}/ds-dev-db-limit-data/",
        "secretpad.data-sandbox.dev.input-rows=10000",
        "secretpad.data-sandbox.dev.sql-limit=50",
        "secretpad.data-sandbox.dev.sandbox-db-bytes=2048",
})
public class DataDevSandboxDbLimitIT {

    private static final String SBX = "sbx-limit";
    private static final String PROC = "ast-limit-proc";
    private static final String DATA_ROOT = System.getProperty("java.io.tmpdir") + "/ds-dev-db-limit-data";

    @Resource
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbc;

    @Resource
    private DataDevService dataDev;

    @Resource
    private SandboxDbService sandboxDb;

    @Resource
    private NodeDatasetStore nodeStore;

    @DynamicPropertySource
    static void schemaProps(DynamicPropertyRegistry registry) {
        registry.add("flyway.default.locations", () -> TestSchemaMigrator.dedupedLocation("center"));
    }

    @BeforeEach
    public void reset() throws IOException {
        for (String t : new String[]{"ds_sandbox_data_dir", "ds_sandbox_db", "ds_sandbox_dataset_mount",
                "ds_dev_task", "ds_node_dataset"}) {
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
        UserContext.setBaseUser(alice());
        String now = LocalDateTime.now().toString();
        jdbc.update("insert into project(project_id,name,owner_id,is_deleted) values('p1','IT Project','alice',0)");
        jdbc.update("insert into project_node(project_id,node_id,is_deleted) values('p1','alice',0)");
        jdbc.update("insert into node(node_id,name,control_node_id,type,mode) values('alice','alice-node','master','normal',0)");
        jdbc.update("insert into ds_sandbox(id,name,owner_id,project_id,image_id,status,expires_at,network_policy,"
                        + "cpu_cores,memory_gb,gpu_count,storage_gb,kuscia_job_id,endpoint,last_error,created_by,"
                        + "created_at,updated_at,deleted) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)",
                SBX, "IT 上限沙箱", "alice", "p1", "img-secretflow", "RUNNING", "2099-01-01T00:00:00",
                "INTERNAL_ONLY", 1, 2, 0, 10, "", "", "", "alice", now, now);
        jdbc.update("insert into ds_data_asset(id,name,provider_node_id,processor_node_id,ingestion_type,"
                        + "modality,data_stage,datatable_id,storage_uri,metadata_json,created_by,created_at,"
                        + "updated_at,version,status,deleted) values(?,?,?,?,?,?,?,?,?,?,?,?,?,1,'ACTIVE',0)",
                PROC, "脱敏样本", "alice", "alice", "IMPORTED", "TABULAR", "PROCESSED", PROC,
                "node-data://x", "{}", "alice", now, now);
        nodeStore.materializeExternal(PROC, "alice", "src-local",
                List.of("id", "name", "amount"), List.of(
                        List.of("1", "alice", "12.5"),
                        List.of("2", "bob", "3.14"),
                        List.of("3", "carol", "7.0")), "sha-local");
        jdbc.update("insert into ds_sandbox_dataset_mount(id,sandbox_id,asset_id,asset_version,provider_node_id,"
                        + "staging_uri,mount_path,checksum,status,expires_at,created_at,updated_at,deleted)"
                        + " values(?,?,?,?,?,?,?,?,?,?,?,?,0)",
                "m1", SBX, PROC, 1, "alice",
                "node-data://" + NodeDatasetStore.assetTableName(PROC),
                "/data/assets/" + PROC, "", "READY", "", now, now);
    }

    private UserContextDTO alice() {
        return UserContextDTO.builder().ownerId("alice").name("alice")
                .platformType(PlatformTypeEnum.CENTER).platformNodeId("alice")
                .ownerType(UserOwnerTypeEnum.CENTER).projectIds(Set.of("p1")).build();
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

    @Test
    public void sandboxDbSnapshotOverLimitFailsWithDevInputTooLarge() throws IOException {
        sandboxDb.rebuild(SBX);
        String table = NodeDatasetStore.assetTableName(PROC);
        assertTrue(Files.size(sandboxDb.sandboxDbPath(SBX)) > 2048,
                "重建后的沙箱库应超过 2048 字节上限，实际 "
                        + Files.size(sandboxDb.sandboxDbPath(SBX)) + " 字节");
        // FUNCTION 提交：快照超限 → 任务 FAILED + error_message 含 DEV_INPUT_TOO_LARGE
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("sandboxId", SBX);
        request.put("name", "it-limit-fn");
        request.put("runMode", "PROD");
        request.put("execType", "FUNCTION");
        request.put("functionName", "risk_score");
        request.put("functionNargs", 2);
        request.put("functionSource", "def risk_score(a, b):\n    return min(a + b, 3)\n");
        request.put("sql", "SELECT risk_score(1, 2) AS s FROM " + table);
        request.put("sourceTable", table);
        Map<String, Object> task = dataDev.submitSandboxTask(request);
        assertEquals("FAILED", String.valueOf(task.get("status")));
        String error = String.valueOf(task.get("error_message"));
        assertTrue(error.contains(DevErrors.DEV_INPUT_TOO_LARGE),
                "error_message 应含 DEV_INPUT_TOO_LARGE，实际: " + error);
        // JAR/PYTHON 同样在 createJob 前被拦截
        Map<String, Object> py = new LinkedHashMap<>();
        py.put("sandboxId", SBX);
        py.put("name", "it-limit-py");
        py.put("runMode", "PROD");
        py.put("execType", "PYTHON");
        py.put("script", "import numpy as np\nprint(np.array([1]))");
        py.put("sourceTable", table);
        Map<String, Object> pyTask = dataDev.submitSandboxTask(py);
        assertEquals("FAILED", String.valueOf(pyTask.get("status")));
        assertTrue(String.valueOf(pyTask.get("error_message")).contains(DevErrors.DEV_INPUT_TOO_LARGE));
    }
}
