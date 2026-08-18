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
import org.secretflow.secretpad.kuscia.v1alpha1.constant.KusciaProtocolEnum;
import org.secretflow.secretpad.kuscia.v1alpha1.mock.MockKusciaGrpcServer;
import org.secretflow.secretpad.kuscia.v1alpha1.mock.service.JobService;
import org.secretflow.secretpad.kuscia.v1alpha1.model.KusciaGrpcConfig;

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
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the Z-02 resource lifecycle: 预占(RESERVED) → 绑定(BOUND) → 释放
 * (RELEASED)，GPU 台账绑定/归还，生命周期感知的用量统计与异常回收。与 DataSandboxKusciaIT
 * 使用独立的 mock 端口与 SQLite 文件，避免 surefire parallel 下互相干扰。
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
        "secretpad.data-sandbox.snapshot-root=${java.io.tmpdir}/ds-sandbox-res-snapshots",
        "secretpad.data-sandbox.backup-root=${java.io.tmpdir}/ds-sandbox-res-backups",
        "spring.datasource.default.jdbc-url=jdbc:sqlite:${java.io.tmpdir}/ds-sandbox-resource-it.sqlite",
        "spring.datasource.quartz.jdbc-url=jdbc:h2:${java.io.tmpdir}/ds-sandbox-resource-it-quartz.mv.db;DB_CLOSE_ON_EXIT=FALSE",
})
public class DataSandboxResourceIT {

    private static final String IMAGE_ID = "img-secretflow";
    private static final int MOCK_PORT = 50052;

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
        mockServer.start(MOCK_PORT, KusciaProtocolEnum.NOTLS, null);
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
        jdbc.update("delete from ds_sandbox");
        jdbc.update("delete from ds_sandbox_snapshot");
        jdbc.update("delete from ds_resource_allocation");
        // Z-02 告警：reclaim 每分钟 @Scheduled 会跨用例残留告警，逐用例清空
        jdbc.update("delete from ds_alert_event");
        jdbc.update("update ds_gpu_ledger set status='AVAILABLE',owner_id='',allocated_at=''");
        // 配额默认 gpu_count=0，测试用 GPU 需先给 alice 配额度
        jdbc.update("update ds_resource_quota set cpu_cores=16,memory_gb=64,gpu_count=4,storage_gb=1024 where owner_id='alice'");
        JobService.State.createJobCode = KusciaAPIConstants.OK;
        JobService.State.createJobMessage = "success";
        JobService.State.jobQueryCode = KusciaAPIConstants.OK;
        JobService.State.jobState = "RUNNING";
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

    private String createSandbox(int cpu, int memoryGb, int gpu, int storageGb) {
        Map<String, Object> created = service.createSandbox(Map.of(
                "name", "res-it-sandbox", "ownerId", "alice", "imageId", IMAGE_ID,
                "networkPolicy", "INTERNAL_ONLY", "cpuCores", cpu, "memoryGb", memoryGb,
                "gpuCount", gpu, "storageGb", storageGb, "validDays", 7));
        return String.valueOf(created.get("id"));
    }

    private void run() {
        // 模拟 Kuscia Job 已 RUNNING 并同步推进本地状态
        JobService.State.jobState = "RUNNING";
        service.syncKusciaStatuses();
    }

    private void stopAndSync() {
        JobService.State.jobState = "SUCCEEDED";
        service.syncKusciaStatuses();
    }

    private List<Map<String, Object>> allocations(String sandboxId) {
        return jdbc.queryForList("select * from ds_resource_allocation where sandbox_id=? order by resource_type", sandboxId);
    }

    @Test
    public void createReservesCapacityLifecycleAware() {
        String id = createSandbox(2, 4, 0, 10);

        Map<String, Object> row = jdbc.queryForMap("select alloc_state,runtime_meta from ds_sandbox where id=?", id);
        assertEquals("RESERVED", row.get("alloc_state"));
        List<Map<String, Object>> allocs = allocations(id);
        assertEquals(3, allocs.size(), "GPU=0 不应生成 GPU 分配行");
        Map<String, Object> cpuRow = allocationsAsMap(id, "CPU");
        assertEquals(2d, cpuRow.get("amount"));
        assertEquals("RESERVED", cpuRow.get("state"));
        // runtime_meta 记录 spec 与 alloc_state
        assertTrue(String.valueOf(row.get("runtime_meta")).contains("\"spec\""));
        assertTrue(String.valueOf(row.get("runtime_meta")).contains("RESERVED"));
        // 生命周期感知用量：创建即占额
        assertEquals(2d, ownerUsageCpu("alice"));
    }

    @Test
    public void runningBindsAllocationsAndGpuLedger() {
        String id = createSandbox(1, 2, 1, 10);
        service.sandboxAction(Map.of("id", id, "action", "START"));
        run();

        assertEquals("BOUND", jdbc.queryForMap("select alloc_state from ds_sandbox where id=?", id).get("alloc_state"));
        Map<String, Object> cpuRow = allocationsAsMap(id, "CPU");
        assertEquals("BOUND", cpuRow.get("state"));
        assertTrue(String.valueOf(cpuRow.get("bound_at")).length() > 0, "BOUND 应写入 bound_at");
        // GPU 台账绑定给 owner
        assertEquals(1L, jdbc.queryForObject("select count(1) from ds_gpu_ledger where status='ALLOCATED' and owner_id='alice'", Long.class));
        // runtime_meta 追加 job_id / resources / endpoint
        String meta = String.valueOf(jdbc.queryForMap("select runtime_meta from ds_sandbox where id=?", id).get("runtime_meta"));
        assertTrue(meta.contains("job_id"));
        assertTrue(meta.contains("\"resources\""));
        assertTrue(meta.contains("10.0.0.1:31234"));
    }

    @Test
    public void stopReleasesAndFreesCapacity() {
        String id = createSandbox(3, 4, 0, 10);
        service.sandboxAction(Map.of("id", id, "action", "START"));
        run();
        assertEquals(3d, ownerUsageCpu("alice"));

        service.sandboxAction(Map.of("id", id, "action", "STOP"));
        stopAndSync();

        assertEquals("STOPPED", jdbc.queryForMap("select status from ds_sandbox where id=?", id).get("status"));
        assertEquals("RELEASED", jdbc.queryForMap("select alloc_state from ds_sandbox where id=?", id).get("alloc_state"));
        Map<String, Object> cpuRow = allocationsAsMap(id, "CPU");
        assertEquals("RELEASED", cpuRow.get("state"));
        assertEquals("MANUAL", cpuRow.get("released_by"));
        // 释放后配额立即回落（生命周期感知）
        assertEquals(0d, ownerUsageCpu("alice"));
    }

    @Test
    public void expireReleasesWithByExpire() {
        String id = createSandbox(1, 2, 0, 10);
        jdbc.update("update ds_sandbox set status='RUNNING',intent='',kuscia_job_id='ds-" + id + "',expires_at=? where id=?",
                LocalDateTime.now().minusMinutes(1).toString(), id);

        service.expireSandboxesAndCheckAlerts();

        assertEquals("EXPIRED", jdbc.queryForMap("select status from ds_sandbox where id=?", id).get("status"));
        assertEquals("EXPIRE", allocationsAsMap(id, "CPU").get("released_by"));
    }

    @Test
    public void destroyReleasesWithByDestroy() {
        String id = createSandbox(1, 2, 0, 10);
        jdbc.update("update ds_sandbox set status='RUNNING',intent='',kuscia_job_id='ds-" + id + "' where id=?", id);

        service.sandboxAction(Map.of("id", id, "action", "DESTROY"));

        assertEquals("DESTROYED", jdbc.queryForMap("select status from ds_sandbox where id=?", id).get("status"));
        assertEquals("DESTROY", allocationsAsMap(id, "CPU").get("released_by"));
    }

    @Test
    public void restartReReservesAfterStop() {
        String id = createSandbox(1, 2, 0, 10);
        service.sandboxAction(Map.of("id", id, "action", "START"));
        run();
        service.sandboxAction(Map.of("id", id, "action", "STOP"));
        stopAndSync();
        assertEquals(0d, ownerUsageCpu("alice"));

        service.sandboxAction(Map.of("id", id, "action", "START"));
        run();

        assertEquals("BOUND", jdbc.queryForMap("select alloc_state from ds_sandbox where id=?", id).get("alloc_state"));
        assertEquals(1d, ownerUsageCpu("alice"));
    }

    @Test
    public void gpuLedgerReleasedOnStop() {
        String id = createSandbox(1, 2, 2, 10);
        service.sandboxAction(Map.of("id", id, "action", "START"));
        run();
        assertEquals(2L, jdbc.queryForObject("select count(1) from ds_gpu_ledger where status='ALLOCATED' and owner_id='alice'", Long.class));

        service.sandboxAction(Map.of("id", id, "action", "STOP"));
        stopAndSync();
        assertEquals(0L, jdbc.queryForObject("select count(1) from ds_gpu_ledger where status='ALLOCATED'", Long.class));
    }

    @Test
    public void reclaimStaleReservedAllocation() {
        String id = createSandbox(1, 2, 0, 10);
        // 构造卡死场景：分配超 10 分钟且沙箱处于 ERROR
        jdbc.update("update ds_resource_allocation set created_at=datetime('now','-11 minutes') where sandbox_id=?", id);
        jdbc.update("update ds_sandbox set status='ERROR',intent='' where id=?", id);

        service.reclaimAbnormalAllocations();

        assertEquals("RELEASED", allocationsAsMap(id, "CPU").get("state"));
        assertEquals("RECLAIM", allocationsAsMap(id, "CPU").get("released_by"));
        assertEquals("RELEASED", jdbc.queryForMap("select alloc_state from ds_sandbox where id=?", id).get("alloc_state"));
        // 异常回收告警已生成（>=1：reclaim 为每分钟 @Scheduled，可能与显式调用并发，去重幂等由 AlertsIT 覆盖）
        assertTrue(jdbc.queryForObject("select count(1) from ds_alert_event where status='OPEN' and source='SANDBOX' and title='资源异常回收'", Long.class) >= 1L);
    }

    @Test
    public void limitVerifyReturnsExpectedAndInstructions() {
        String id = createSandbox(2, 4, 1, 10);
        Map<String, Object> result = service.limitVerify(id);
        assertEquals(id, result.get("sandboxId"));
        Map<?, ?> expected = (Map<?, ?>) result.get("expected");
        assertEquals(2d, ((Number) expected.get("cpu")).doubleValue());
        assertEquals(4d, ((Number) expected.get("memory_gb")).doubleValue());
        assertTrue(String.valueOf(result.get("instructions")).contains("verify-limits.sh"));
    }

    private double ownerUsageCpu(String ownerId) {
        Object usage = service.resourceOverview(ownerId).get("ownerUsage");
        return ((Number) ((Map<?, ?>) usage).get("CPU")).doubleValue();
    }

    private Map<String, Object> allocationsAsMap(String sandboxId, String resourceType) {
        return jdbc.queryForMap("select resource_type,amount,state,bound_at,released_by from ds_resource_allocation where sandbox_id=? and resource_type=?",
                sandboxId, resourceType);
    }
}
