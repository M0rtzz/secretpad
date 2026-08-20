/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.service;

import org.secretflow.secretpad.common.dto.UserContextDTO;
import org.secretflow.secretpad.common.enums.PlatformTypeEnum;
import org.secretflow.secretpad.common.enums.UserOwnerTypeEnum;
import org.secretflow.secretpad.common.util.UserContext;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.secretflow.secretpad.web.SecretPadApplication;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Z-01 regression guard: with the Kuscia runtime disabled, START must produce a clear
 * ERROR (never a fake RUNNING), while STOP/DESTROY keep working locally.
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
        "secretpad.data-sandbox.snapshot-root=${java.io.tmpdir}/ds-sandbox-snapshots",
        "secretpad.data-sandbox.backup-root=${java.io.tmpdir}/ds-sandbox-backups",
        "spring.datasource.default.jdbc-url=jdbc:sqlite:${java.io.tmpdir}/ds-sandbox-disabled-it.sqlite",
        "spring.datasource.quartz.jdbc-url=jdbc:h2:${java.io.tmpdir}/ds-sandbox-disabled-it-quartz.mv.db;DB_CLOSE_ON_EXIT=FALSE",
})
public class DataSandboxKusciaDisabledIT {

    @Resource
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbc;

    @Resource
    private DataSandboxMvpService service;

    @BeforeEach
    public void reset() {
        jdbc.update("delete from ds_sandbox");
        jdbc.update("delete from ds_sandbox_snapshot");
        UserContext.setBaseUser(UserContextDTO.builder().ownerId("alice").name("alice")
                .platformType(PlatformTypeEnum.CENTER).platformNodeId("alice")
                .ownerType(UserOwnerTypeEnum.CENTER).projectIds(Set.of("p1")).build());
    }

    private String createSandbox() {
        Map<String, Object> created = service.createSandbox(Map.of(
                "name", "disabled-it", "ownerId", "alice", "imageId", "img-jupyter-scipy",
                "networkPolicy", "INTERNAL_ONLY", "cpuCores", 1, "memoryGb", 2, "gpuCount", 0,
                "storageGb", 10, "validDays", 7));
        return String.valueOf(created.get("id"));
    }

    private Map<String, Object> statusOf(String id) {
        return jdbc.queryForMap("select status,intent,last_error from ds_sandbox where id=?", id);
    }

    @Test
    public void startIsRejectedWithClearMessageWhenKusciaDisabled() {
        String id = createSandbox();
        service.sandboxAction(Map.of("id", id, "action", "START"));

        Map<String, Object> after = statusOf(id);
        assertEquals("ERROR", after.get("status"), "kuscia 未启用时禁止假 RUNNING");
        assertNotEquals("RUNNING", after.get("status"));
        assertTrue(String.valueOf(after.get("last_error")).contains("未启用"),
                "错误文案应说明运行时未启用: " + after.get("last_error"));
        assertEquals("", after.get("intent"));
    }

    @Test
    public void stopCompletesLocallyWhenNoRealJobExists() {
        String id = createSandbox();
        // ERROR 沙箱（例如上次启动失败）允许停止，且无真实 job：本地直接完成
        jdbc.update("update ds_sandbox set status='ERROR',intent='',last_error='injected' where id=?", id);
        service.sandboxAction(Map.of("id", id, "action", "STOP"));

        Map<String, Object> after = statusOf(id);
        assertEquals("STOPPED", after.get("status"), "无真实 job 时停止应本地完成，不卡 STOPPING");
        assertEquals("", after.get("intent"));
    }

    @Test
    public void destroyCompletesLocallyWhenKusciaDisabled() {
        String id = createSandbox();
        jdbc.update("update ds_sandbox set status='RUNNING',intent='' where id=?", id);
        service.sandboxAction(Map.of("id", id, "action", "DESTROY"));

        Map<String, Object> row = jdbc.queryForMap("select status,deleted from ds_sandbox where id=?", id);
        assertEquals("DESTROYED", row.get("status"));
        assertEquals(1, ((Number) row.get("deleted")).intValue());
    }

    @Test
    public void syncIsSafeNoOpWhenKusciaDisabled() {
        createSandbox();
        // disabled 时同步必须直接跳过，不抛异常
        service.syncKusciaStatuses();
    }
}
