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
import org.secretflow.secretpad.kuscia.v1alpha1.DynamicKusciaChannelProvider;
import org.secretflow.secretpad.kuscia.v1alpha1.constant.KusciaAPIConstants;
import org.secretflow.secretpad.kuscia.v1alpha1.mock.MockKusciaGrpcServer;
import org.secretflow.secretpad.kuscia.v1alpha1.mock.service.JobService;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the Z-01 real sandbox runtime: sandbox actions must drive a real
 * Kuscia Job through the mock gRPC server, local status must never be "fake RUNNING",
 * and the status synchronizer must honour the local intent (never blindly overwrite).
 */
@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD) // surefire parallel=all 会并行执行类内方法，@BeforeEach 清理会互相删除数据，必须串行
@ActiveProfiles("test")
@SpringBootTest(classes = SecretPadApplication.class)
@TestPropertySource(properties = {
        "kuscia.nodes=",
        "secretpad.data-sandbox.kuscia.enabled=true",
        "secretpad.node-id=kuscia-system",
        "secretpad.data-sandbox.snapshot-root=${java.io.tmpdir}/ds-sandbox-snapshots",
        "secretpad.data-sandbox.backup-root=${java.io.tmpdir}/ds-sandbox-backups",
        // 测试工作目录（模块目录）没有 db/ 目录，SQLite 不自动建目录：数据源指到 /tmp 下的全新库
        "spring.datasource.default.jdbc-url=jdbc:sqlite:${java.io.tmpdir}/ds-sandbox-it.sqlite",
        "spring.datasource.quartz.jdbc-url=jdbc:h2:${java.io.tmpdir}/ds-sandbox-it-quartz.mv.db;DB_CLOSE_ON_EXIT=FALSE",
})
public class DataSandboxKusciaIT {

    private static final String IMAGE_ID = "img-secretflow";

    @Resource
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbc;

    @Resource
    private DataSandboxMvpService service;

    @Resource
    private DynamicKusciaChannelProvider channelProvider;

    private MockKusciaGrpcServer mockServer;

    @BeforeAll
    public void startMock() throws Exception {
        mockServer = new MockKusciaGrpcServer();
        mockServer.start();
        channelProvider.registerKuscia(mockServer.buildKusciaGrpcConfig("kuscia-system"));
    }

    @AfterAll
    public void stopMock() {
        mockServer.stop();
    }

    @BeforeEach
    public void reset() {
        // 每个用例前清理沙箱数据、重置 mock 状态、设置会话用户
        jdbc.update("delete from ds_sandbox");
        jdbc.update("delete from ds_sandbox_snapshot");
        // Z-02：资源分配与 GPU 台账一并复位，避免跨用例残留占用
        jdbc.update("delete from ds_resource_allocation");
        jdbc.update("update ds_gpu_ledger set status='AVAILABLE',owner_id='',allocated_at=''");
        JobService.State.createJobCode = KusciaAPIConstants.OK;
        JobService.State.createJobMessage = "success";
        JobService.State.jobQueryCode = KusciaAPIConstants.OK;
        JobService.State.jobState = "RUNNING";
        JobService.State.taskState = "";
        JobService.State.partyState = "";
        JobService.State.jobErrMsg = "";
        JobService.State.withEndpoints = true;
        JobService.State.endpointPortName = "web";
        JobService.State.endpointScope = "Cluster";
        JobService.State.endpointAddress = "10.0.0.1:31234";
        JobService.State.stopJobCode = KusciaAPIConstants.OK;
        JobService.State.stopJobMessage = "success";
        JobService.State.deleteJobCode = KusciaAPIConstants.OK;
        JobService.State.deleteJobMessage = "success";
        UserContext.setBaseUser(UserContextDTO.builder().ownerId("alice").name("alice")
                .platformType(PlatformTypeEnum.CENTER).platformNodeId("alice")
                .ownerType(UserOwnerTypeEnum.CENTER).projectIds(Set.of("p1")).build());
    }

    /** 创建沙箱并返回真实 id（service 会忽略请求里的 id，自行生成）。 */
    private String createSandbox() {
        Map<String, Object> created = service.createSandbox(Map.of(
                "name", "it-sandbox", "ownerId", "alice", "imageId", IMAGE_ID,
                "networkPolicy", "INTERNAL_ONLY", "cpuCores", 1, "memoryGb", 2, "gpuCount", 0,
                "storageGb", 10, "validDays", 7));
        return String.valueOf(created.get("id"));
    }

    private Map<String, Object> statusOf(String id) {
        return jdbc.queryForMap("select status,intent,last_error,endpoint from ds_sandbox where id=?", id);
    }

    @Test
    public void startSuccessFlowAdvancesToRunningAndWritesEndpoint() {
        String id = createSandbox();
        service.sandboxAction(Map.of("id", id, "action", "START"));

        // createJob 成功：先落 STARTING + intent=START，不直接置 RUNNING
        Map<String, Object> afterStart = statusOf(id);
        assertEquals("STARTING", afterStart.get("status"));
        assertEquals("START", afterStart.get("intent"));
        assertNotEquals("", jdbc.queryForMap("select kuscia_job_id from ds_sandbox where id=?", id).get("kuscia_job_id"));

        // mock Kuscia Job 已 RUNNING：同步推进为 RUNNING 并写入端点、清空意图
        JobService.State.jobState = "RUNNING";
        service.syncKusciaStatuses();
        Map<String, Object> afterSync = statusOf(id);
        assertEquals("RUNNING", afterSync.get("status"));
        assertEquals("", afterSync.get("intent"));
        assertEquals("10.0.0.1:31234", afterSync.get("endpoint"));
    }

    @Test
    public void startFailureMarksErrorNeverRunning() {
        String id = createSandbox();
        JobService.State.createJobCode = 101010;
        JobService.State.createJobMessage = "app image not found";
        service.sandboxAction(Map.of("id", id, "action", "START"));

        Map<String, Object> after = statusOf(id);
        assertEquals("ERROR", after.get("status"));
        assertTrue(String.valueOf(after.get("last_error")).contains("app image not found"));
        assertNotEquals("RUNNING", after.get("status"));
    }

    @Test
    public void runningReturnsToStartingWhenRuntimeBecomesPending() {
        String id = createSandbox();
        // 直接构造 RUNNING 本地状态
        jdbc.update("update ds_sandbox set status='RUNNING',intent='',endpoint='old:1234',kuscia_job_id='ds-" + id + "' where id=?", id);
        JobService.State.jobState = "PENDING";

        service.syncKusciaStatuses();
        Map<String, Object> after = statusOf(id);
        assertEquals("STARTING", after.get("status"));
        assertEquals("", after.get("intent"));
        assertEquals("", after.get("endpoint"));
    }

    @Test
    public void runningJobWithPendingTaskDoesNotExposeEndpoint() {
        String id = createSandbox();
        service.sandboxAction(Map.of("id", id, "action", "START"));
        JobService.State.jobState = "RUNNING";
        JobService.State.taskState = "PENDING";
        JobService.State.partyState = "PENDING";

        service.syncKusciaStatuses();
        Map<String, Object> after = statusOf(id);
        assertEquals("STARTING", after.get("status"));
        assertEquals("START", after.get("intent"));
        assertEquals("", after.get("endpoint"));
    }

    @Test
    public void startingFailsWhenKusciaJobFails() {
        String id = createSandbox();
        service.sandboxAction(Map.of("id", id, "action", "START"));
        JobService.State.jobState = "FAILED";
        JobService.State.jobErrMsg = "runtime error";

        service.syncKusciaStatuses();
        Map<String, Object> after = statusOf(id);
        assertEquals("ERROR", after.get("status"));
        assertTrue(String.valueOf(after.get("last_error")).contains("FAILED"));
        assertEquals("", after.get("intent"));
    }

    @Test
    public void stopFlowAdvancesToStoppedOnlyAfterKusciaTerminal() {
        String id = createSandbox();
        // 置为运行中
        jdbc.update("update ds_sandbox set status='RUNNING',intent='',kuscia_job_id='ds-" + id + "' where id=?", id);
        JobService.State.jobState = "RUNNING";

        service.sandboxAction(Map.of("id", id, "action", "STOP"));
        Map<String, Object> afterStop = statusOf(id);
        assertEquals("STOPPING", afterStop.get("status"));
        assertEquals("STOP", afterStop.get("intent"));

        // Kuscia Job 仍 RUNNING：同步不得提前置 STOPPED
        service.syncKusciaStatuses();
        assertEquals("STOPPING", statusOf(id).get("status"));

        // Kuscia Job 终态：推进为 STOPPED 并清空意图
        JobService.State.jobState = "SUCCEEDED";
        service.syncKusciaStatuses();
        Map<String, Object> after = statusOf(id);
        assertEquals("STOPPED", after.get("status"));
        assertEquals("", after.get("intent"));
    }

    @Test
    public void stopFailureMarksErrorNeverStopped() {
        String id = createSandbox();
        jdbc.update("update ds_sandbox set status='RUNNING',intent='',kuscia_job_id='ds-" + id + "' where id=?", id);
        JobService.State.stopJobCode = 500;
        JobService.State.stopJobMessage = "stop failed";

        service.sandboxAction(Map.of("id", id, "action", "STOP"));
        Map<String, Object> after = statusOf(id);
        assertEquals("ERROR", after.get("status"), "stopJob 失败不得假 STOPPED");
        assertTrue(String.valueOf(after.get("last_error")).contains("stop failed"));
        assertEquals("", after.get("intent"));
    }

    @Test
    public void expireFlowMarksExpiredAfterSuccessfulStop() {
        String id = createSandbox();
        // 置为运行中且已到期
        jdbc.update("update ds_sandbox set status='RUNNING',intent='',kuscia_job_id='ds-" + id + "',expires_at=? where id=?",
                LocalDateTime.now().minusMinutes(1).toString(), id);
        JobService.State.jobState = "RUNNING";

        service.expireSandboxesAndCheckAlerts();
        assertEquals("EXPIRED", statusOf(id).get("status"));
    }

    @Test
    public void expireFailureMarksErrorInsteadOfExpired() {
        String id = createSandbox();
        jdbc.update("update ds_sandbox set status='RUNNING',intent='',kuscia_job_id='ds-" + id + "',expires_at=? where id=?",
                LocalDateTime.now().minusMinutes(1).toString(), id);
        JobService.State.stopJobCode = 500;
        JobService.State.stopJobMessage = "stop failed on expire";

        service.expireSandboxesAndCheckAlerts();
        Map<String, Object> after = statusOf(id);
        assertEquals("ERROR", after.get("status"), "到期停止失败不得假 EXPIRED");
        assertTrue(String.valueOf(after.get("last_error")).contains("stop failed on expire"));
    }

    @Test
    public void destroyFlowMarksDeleted() {
        String id = createSandbox();
        jdbc.update("update ds_sandbox set status='RUNNING',intent='',kuscia_job_id='ds-" + id + "' where id=?", id);
        service.sandboxAction(Map.of("id", id, "action", "DESTROY"));
        Map<String, Object> row = jdbc.queryForMap("select status,deleted from ds_sandbox where id=?", id);
        assertEquals("DESTROYED", row.get("status"));
        assertEquals(1, ((Number) row.get("deleted")).intValue());
    }

    @Test
    public void destroyFailureKeepsRecordAsError() {
        String id = createSandbox();
        jdbc.update("update ds_sandbox set status='RUNNING',intent='',kuscia_job_id='ds-" + id + "' where id=?", id);
        JobService.State.deleteJobCode = 500;
        JobService.State.deleteJobMessage = "delete failed";

        service.sandboxAction(Map.of("id", id, "action", "DESTROY"));
        Map<String, Object> row = jdbc.queryForMap("select status,deleted,last_error from ds_sandbox where id=?", id);
        assertEquals("ERROR", row.get("status"));
        assertEquals(0, ((Number) row.get("deleted")).intValue());
        assertTrue(String.valueOf(row.get("last_error")).contains("delete failed"));
    }

    @Test
    public void snapshotRejectedUnlessRunningOrStopped() {
        String id = createSandbox();
        // STOPPED 允许快照
        service.sandboxAction(Map.of("id", id, "action", "SNAPSHOT"));
        assertTrue(jdbc.queryForObject("select count(1) from ds_sandbox_snapshot where sandbox_id=?", Integer.class, id) > 0);
        // STARTING 拒绝快照
        jdbc.update("update ds_sandbox set status='STARTING',intent='START' where id=?", id);
        boolean rejected = false;
        try {
            service.sandboxAction(Map.of("id", id, "action", "SNAPSHOT"));
        } catch (IllegalStateException e) {
            rejected = true;
        }
        assertTrue(rejected, "STARTING 状态应拒绝快照");
    }
}
