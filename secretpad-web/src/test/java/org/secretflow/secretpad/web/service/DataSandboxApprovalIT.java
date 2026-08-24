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
import org.secretflow.secretpad.common.errorcode.DataErrorCode;
import org.secretflow.secretpad.common.exception.SecretpadException;
import org.secretflow.secretpad.common.enums.PlatformTypeEnum;
import org.secretflow.secretpad.common.enums.UserOwnerTypeEnum;
import org.secretflow.secretpad.common.util.UserContext;
import org.secretflow.secretpad.kuscia.v1alpha1.DynamicKusciaChannelProvider;
import org.secretflow.secretpad.kuscia.v1alpha1.constant.KusciaAPIConstants;
import org.secretflow.secretpad.kuscia.v1alpha1.constant.KusciaProtocolEnum;
import org.secretflow.secretpad.kuscia.v1alpha1.mock.MockKusciaGrpcServer;
import org.secretflow.secretpad.kuscia.v1alpha1.mock.service.JobService;
import org.secretflow.secretpad.kuscia.v1alpha1.model.KusciaGrpcConfig;
import org.secretflow.secretpad.web.SecretPadApplication;
import org.secretflow.secretpad.web.service.DataAssetService;
import org.secretflow.secretpad.web.service.sandbox.SandboxApprovalService;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Z-03 集成测试：沙箱资源申请与审批全链路。覆盖两级审批（供数方→运营方）、驳回复审、并发审批、
 * 失败自动重试/FAILED、提交幂等、四类型执行流（CREATE/RENEW/SPEC_CHANGE/RECYCLE）与卡死兜底。
 *
 * <p>独立 mock 端口 50053 与 SQLite 文件，与其它 DataSandbox IT 互不干扰；
 * @Scheduled 在 test profile 不运行，测试手动调 {@link SandboxApprovalService#executeApprovals()}。</p>
 */
@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
@ActiveProfiles("test")
@SpringBootTest(classes = SecretPadApplication.class)
@TestPropertySource(properties = {
        "kuscia.nodes=",
        "secretpad.data-sandbox.kuscia.enabled=true",
        "secretpad.data-sandbox.approval.required=true",
        "secretpad.node-id=kuscia-system",
        "secretpad.data-sandbox.snapshot-root=${java.io.tmpdir}/ds-sandbox-apr-snapshots",
        "secretpad.data-sandbox.backup-root=${java.io.tmpdir}/ds-sandbox-apr-backups",
        "spring.datasource.default.jdbc-url=jdbc:sqlite:${java.io.tmpdir}/ds-sandbox-approval-it.sqlite",
        "spring.datasource.quartz.jdbc-url=jdbc:h2:${java.io.tmpdir}/ds-sandbox-approval-it-quartz.mv.db;DB_CLOSE_ON_EXIT=FALSE",
})
public class DataSandboxApprovalIT {

    private static final String IMAGE_ID = "img-secretflow";
    private static final int MOCK_PORT = 50053;
    private static final int FAIL_CODE = 1;

    @Resource
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbc;

    @Resource
    private DataSandboxMvpService service;

    @Resource
    private SandboxApprovalService approvalService;

    @Resource
    private DataAssetService dataAssetService;

    @Resource
    private DynamicKusciaChannelProvider channelProvider;

    private MockKusciaGrpcServer mockServer;

    /* ------------------------------- 角色（UserContext 按线程设置） ------------------------------- */

    private UserContextDTO user(String name, String ownerId) {
        return UserContextDTO.builder().ownerId(ownerId).name(name)
                .platformType(PlatformTypeEnum.CENTER).platformNodeId(ownerId)
                .ownerType(UserOwnerTypeEnum.CENTER).projectIds(Set.of("p1")).build();
    }

    private UserContextDTO alice() {
        return user("alice", "alice");
    }

    private UserContextDTO carol() {
        return user("carol", "carol");
    }

    private UserContextDTO admin() {
        return UserContextDTO.builder().ownerId("kuscia-system").name("admin")
                .platformType(PlatformTypeEnum.CENTER).platformNodeId("kuscia-system")
                .ownerType(UserOwnerTypeEnum.CENTER).build();
    }

    /** alice 节点的运维账号（可作阶段2运营方，用于并发审批）。 */
    private UserContextDTO opsAlice() {
        return user("ops-alice", "alice");
    }

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
        jdbc.update("delete from ds_sandbox_approval_vote");
        jdbc.update("delete from ds_sandbox_approval_history");
        jdbc.update("delete from ds_sandbox_approval");
        jdbc.update("delete from ds_project_asset");
        jdbc.update("delete from ds_data_asset where id like 'asset-delete-it-%'");
        jdbc.update("delete from project_datatable where project_id='p1'");
        jdbc.update("delete from ds_sandbox");
        jdbc.update("delete from ds_sandbox_snapshot");
        jdbc.update("delete from ds_resource_allocation");
        jdbc.update("delete from ds_alert_event");
        jdbc.update("delete from project_node where project_id in ('p1','p2')");
        jdbc.update("delete from project where project_id in ('p1','p2')");
        jdbc.update("insert into project(project_id,name,owner_id,status,is_deleted) values('p1','Approval IT','alice',1,0)");
        jdbc.update("insert into project_node(project_id,node_id,is_deleted) values('p1','alice',0)");
        jdbc.update("insert into project_node(project_id,node_id,is_deleted) values('p1','carol',0)");
        jdbc.update("insert or replace into ds_sandbox_image(id,name,image_ref,kuscia_app_image,description,enabled,created_by,created_at,updated_at) values(?,?,?,'','','1','test',?,?)",
                IMAGE_ID, "SecretFlow", "secretflow:test", LocalDateTime.now().toString(), LocalDateTime.now().toString());
        jdbc.update("insert or ignore into ds_resource_quota(owner_id,cpu_cores,memory_gb,gpu_count,storage_gb,updated_by,updated_at) values('alice',16,64,4,1024,'test',?)",
                LocalDateTime.now().toString());
        jdbc.update("update ds_gpu_ledger set status='AVAILABLE',owner_id='',allocated_at=''");
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
        UserContext.setBaseUser(alice());
    }

    /* ------------------------------- helpers ------------------------------- */

    private Map<String, Object> createPayload(String type, String sandboxId) {
        Map<String, Object> payload = new java.util.LinkedHashMap<>();
        payload.put("approvalType", type);
        if (sandboxId != null) {
            payload.put("sandboxId", sandboxId);
        }
        payload.put("reason", "IT");
        return payload;
    }

    /** alice 提交 CREATE 申请单（含完整规格与镜像）。 */
    private String submitCreate() {
        Map<String, Object> payload = createPayload("CREATE", null);
        payload.put("ownerId", "alice");
        payload.put("projectId", "p1");
        payload.put("name", "apr-sandbox");
        payload.put("imageId", IMAGE_ID);
        payload.put("networkPolicy", "INTERNAL_ONLY");
        payload.put("cpuCores", 1);
        payload.put("memoryGb", 2);
        payload.put("gpuCount", 0);
        payload.put("storageGb", 10);
        payload.put("validDays", 7);
        return String.valueOf(approvalService.submit(payload).get("id"));
    }

    /** carol 阶段1 通过（供数方）。 */
    private void approveStage1(String id) {
        UserContext.setBaseUser(carol());
        approvalService.approvalAction(Map.of("id", id, "action", "APPROVE", "comment", "供数方确认"));
    }

    /** admin 阶段2 通过（运营方）。 */
    private void approveStage2(String id) {
        UserContext.setBaseUser(admin());
        approvalService.approvalAction(Map.of("id", id, "action", "APPROVE", "comment", "运营方确认"));
    }

    /** 完整提交+两级审批 → APPROVED。 */
    private String submitAndApprove() {
        String id = submitCreate();
        approveStage1(id);
        approveStage2(id);
        return id;
    }

    private String approvalStatus(String id) {
        return String.valueOf(jdbc.queryForMap("select status from ds_sandbox_approval where id=?", id).get("status"));
    }

    private String createSandbox() {
        Map<String, Object> created = service.createSandbox(Map.of(
                "name", "apr-sbx", "ownerId", "alice", "projectId", "p1", "imageId", IMAGE_ID,
                "networkPolicy", "INTERNAL_ONLY", "cpuCores", 1, "memoryGb", 2, "gpuCount", 0,
                "storageGb", 10, "validDays", 7));
        return String.valueOf(created.get("id"));
    }

    private long count(String sql, Object... args) {
        Long value = jdbc.queryForObject(sql, Long.class, args);
        return value == null ? 0 : value;
    }

    /* ------------------------------- 用例 ------------------------------- */

    /** 已归档项目不可创建沙箱或挂载数据，审批执行前再次校验项目状态。 */
    @Test
    public void archivedProjectRejectsSandboxCreationAndDataMounts() {
        String sandboxId = createSandbox();
        jdbc.update("update project set status=2 where project_id='p1'");

        assertThrows(IllegalStateException.class, this::submitCreate);
        assertThrows(IllegalStateException.class, () -> service.createSandbox(Map.of(
                "name", "archived-project-sbx", "ownerId", "alice", "projectId", "p1",
                "imageId", IMAGE_ID)));
        assertThrows(IllegalStateException.class, () -> dataAssetService.attachProjectAssets(
                Map.of("projectId", "p1", "assetIds", List.of())));

        Map<String, Object> dataChange = createPayload("DATA_CHANGE", sandboxId);
        dataChange.put("datasetAssetIds", List.of());
        assertThrows(IllegalStateException.class, () -> approvalService.submit(dataChange));
    }

    /** 项目在申请批准后归档时，执行器不得继续创建沙箱。 */
    @Test
    public void archivedProjectBlocksApprovedCreateExecution() {
        String id = submitAndApprove();
        jdbc.update("update project set status=2 where project_id='p1'");

        approvalService.executeApprovals();

        assertEquals("APPROVED", approvalStatus(id), "归档后的创建申请应保留为可诊断的失败重试状态");
        assertEquals(0L, count("select count(1) from ds_sandbox where deleted=0"));
        assertTrue(String.valueOf(jdbc.queryForMap(
                "select last_error from ds_sandbox_approval where id=?", id).get("last_error"))
                .contains("项目已归档"));
    }

    /** 1. CREATE 全链路：提交→两级审批→执行引擎自动建沙箱并拉起→同步 RUNNING/BOUND。 */
    @Test
    public void createFullLifecycleAutoExecutes() {
        String id = submitAndApprove();
        assertEquals("APPROVED", approvalStatus(id));

        JobService.State.lastCreateJobRequest = null;
        approvalService.executeApprovals();

        assertEquals("COMPLETED", approvalStatus(id));
        assertNotNull(JobService.State.lastCreateJobRequest, "执行引擎应真实调用 createJob");
        String sandboxId = String.valueOf(jdbc.queryForMap("select sandbox_id from ds_sandbox_approval where id=?", id).get("sandbox_id"));
        assertTrue(sandboxId.startsWith("sbx-"), "执行应回填沙箱 id");
        // 建出后自动 START：沙箱处于 STARTING（createJob 已发）
        assertEquals("STARTING", String.valueOf(jdbc.queryForMap("select status from ds_sandbox where id=?", sandboxId).get("status")));

        // 同步推进 RUNNING → 分配绑定 BOUND
        UserContext.setBaseUser(alice());
        service.syncKusciaStatuses();
        Map<String, Object> sbx = jdbc.queryForMap("select status,alloc_state,endpoint from ds_sandbox where id=?", sandboxId);
        assertEquals("RUNNING", String.valueOf(sbx.get("status")));
        assertEquals("BOUND", String.valueOf(sbx.get("alloc_state")));
        assertTrue(String.valueOf(sbx.get("endpoint")).contains("10.0.0.1"));
        // 审批历史完整：SUBMIT / APPROVE / APPROVE / EXECUTE / COMPLETE
        assertEquals(5L, count("select count(1) from ds_sandbox_approval_history where approval_id=?", id));
    }

    /** 2. 驳回与复审：阶段1 REJECT → REJECTED；申请人 RESUBMIT → version=2 → 复审通过。 */
    @Test
    public void rejectThenResubmitIncrementsVersion() {
        String id = submitCreate();
        UserContext.setBaseUser(carol());
        approvalService.approvalAction(Map.of("id", id, "action", "REJECT", "comment", "规格不符"));
        assertEquals("REJECTED", approvalStatus(id));

        UserContext.setBaseUser(alice());
        approvalService.approvalAction(Map.of("id", id, "action", "RESUBMIT", "comment", "已修正规格"));
        assertEquals("DATA_PROVIDER_REVIEW", approvalStatus(id));
        Map<String, Object> row = jdbc.queryForMap("select version,review_comment from ds_sandbox_approval where id=?", id);
        assertEquals(2, ((Number) row.get("version")).intValue());
        assertEquals("", String.valueOf(row.get("review_comment")), "复审应清空上一轮审核意见");

        approveStage1(id);
        approveStage2(id);
        assertEquals("APPROVED", approvalStatus(id));
    }

    /** 3. 并发审批：阶段2 两审核人同时 APPROVE，恰一个成功，另一个收到冲突。 */
    @Test
    public void concurrentStageTwoApproveOnlyOneWins() throws Exception {
        String id = submitCreate();
        approveStage1(id);
        assertEquals("OPERATOR_REVIEW", approvalStatus(id));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger success = new AtomicInteger();
        AtomicInteger conflict = new AtomicInteger();
        Runnable adminApprove = () -> {
            UserContext.setBaseUser(admin());
            try {
                latch.await();
                approvalService.approvalAction(Map.of("id", id, "action", "APPROVE", "comment", "admin"));
                success.incrementAndGet();
            } catch (Exception e) {
                conflict.incrementAndGet();
            }
        };
        Runnable opsApprove = () -> {
            UserContext.setBaseUser(opsAlice());
            try {
                latch.await();
                approvalService.approvalAction(Map.of("id", id, "action", "APPROVE", "comment", "ops"));
                success.incrementAndGet();
            } catch (Exception e) {
                conflict.incrementAndGet();
            }
        };
        Thread t1 = new Thread(adminApprove);
        Thread t2 = new Thread(opsApprove);
        t1.start();
        t2.start();
        latch.countDown();
        t1.join(5000);
        t2.join(5000);
        assertEquals(1, success.get(), "恰有一个审核人胜出");
        assertEquals(1, conflict.get(), "另一审核人应收到并发冲突");
        assertEquals("APPROVED", approvalStatus(id));
    }

    /** 4. 失败自动重试与 FAILED：createJob 先失败回退 APPROVED，恢复后 COMPLETED；持续失败达上限 FAILED+告警。 */
    @Test
    public void createJobFailureRetriesThenFailsAfterMax() {
        String id = submitAndApprove();

        // 第一次失败 → retry_count=1 → 回退 APPROVED（未达上限自动重试）
        JobService.State.createJobCode = FAIL_CODE;
        JobService.State.createJobMessage = "boom";
        approvalService.executeApprovals();
        assertEquals("APPROVED", approvalStatus(id));
        assertEquals(1, ((Number) jdbc.queryForMap("select retry_count from ds_sandbox_approval where id=?", id).get("retry_count")).intValue());
        assertTrue(String.valueOf(jdbc.queryForMap("select last_error from ds_sandbox_approval where id=?", id).get("last_error")).contains("boom"));

        // 恢复 createJob → 自动重试成功 → COMPLETED
        JobService.State.createJobCode = KusciaAPIConstants.OK;
        approvalService.executeApprovals();
        assertEquals("COMPLETED", approvalStatus(id));

        // 另起一单：连续失败 3 次 → FAILED + 告警
        String failedId = submitAndApprove();
        JobService.State.createJobCode = FAIL_CODE;
        for (int i = 0; i < 3; i++) {
            approvalService.executeApprovals();
        }
        assertEquals("FAILED", approvalStatus(failedId));
        assertEquals(3, ((Number) jdbc.queryForMap("select retry_count from ds_sandbox_approval where id=?", failedId).get("retry_count")).intValue());
        assertTrue(count("select count(1) from ds_alert_event where source='SANDBOX' and title='沙箱申请执行失败'") >= 1L);

        // FAILED 后可人工 RETRY：恢复 createJob → 同步执行成功
        JobService.State.createJobCode = KusciaAPIConstants.OK;
        UserContext.setBaseUser(admin());
        approvalService.approvalAction(Map.of("id", failedId, "action", "RETRY", "comment", "环境已恢复"));
        assertEquals("COMPLETED", approvalStatus(failedId));
    }

    /** 5. 提交幂等：同 owner 存在进行中 CREATE 时重复提交被拒；变更类按沙箱同理。 */
    @Test
    public void submitIdempotentRejectsDuplicateOpen() {
        submitCreate();
        Map<String, Object> dup = createPayload("CREATE", null);
        dup.put("ownerId", "alice");
        dup.put("name", "dup");
        dup.put("imageId", IMAGE_ID);
        dup.put("networkPolicy", "INTERNAL_ONLY");
        dup.put("cpuCores", 1);
        dup.put("memoryGb", 2);
        dup.put("gpuCount", 0);
        dup.put("storageGb", 10);
        dup.put("validDays", 7);
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> approvalService.submit(dup));
        assertTrue(e.getMessage().contains("已有同类型申请单处理中"), e.getMessage());

        // 变更类幂等：同一沙箱存在进行中 RENEW 时重复提交被拒
        String sandboxId = createSandbox();
        Map<String, Object> renew = createPayload("RENEW", sandboxId);
        renew.put("expiresAt", LocalDateTime.now().plusDays(7).toString());
        approvalService.submit(renew);
        Map<String, Object> renewDup = createPayload("RENEW", sandboxId);
        renewDup.put("expiresAt", LocalDateTime.now().plusDays(14).toString());
        IllegalStateException e2 = assertThrows(IllegalStateException.class, () -> approvalService.submit(renewDup));
        assertTrue(e2.getMessage().contains("已有同类型申请单处理中"), e2.getMessage());
    }

    /** 6. RENEW：expires_at 前移，无可续（已回收）视为完成。 */
    @Test
    public void renewMovesExpiresAt() {
        String sandboxId = createSandbox();
        String before = String.valueOf(jdbc.queryForMap("select expires_at from ds_sandbox where id=?", sandboxId).get("expires_at"));

        Map<String, Object> payload = createPayload("RENEW", sandboxId);
        String requestedExpiresAt = LocalDateTime.now().plusDays(15).withNano(0).toString();
        payload.put("expiresAt", requestedExpiresAt);
        String id = String.valueOf(approvalService.submit(payload).get("id"));
        approveStage1(id);
        approveStage2(id);
        approvalService.executeApprovals();

        assertEquals("COMPLETED", approvalStatus(id));
        String after = String.valueOf(jdbc.queryForMap("select expires_at from ds_sandbox where id=?", sandboxId).get("expires_at"));
        assertNotEquals(before, after, "续期应前移 expires_at");
        assertTrue(LocalDateTime.parse(after).isAfter(LocalDateTime.parse(before)));
        assertEquals(requestedExpiresAt, after);
        // 已回收沙箱无可续 → 视为完成
        Map<String, Object> recyclePayload = createPayload("RECYCLE", sandboxId);
        recyclePayload.put("days", 7);
        String recycled = String.valueOf(approvalService.submit(recyclePayload).get("id"));
        approveStage1(recycled);
        approveStage2(recycled);
        approvalService.executeApprovals();
        String done = String.valueOf(jdbc.queryForMap("select status from ds_sandbox where id=?", sandboxId).get("status"));
        assertEquals("DESTROYED", done);
    }

    /** 7. SPEC_CHANGE：停删旧 job → 落新规格 → 释放旧分配 → 按新规格重预留 → 新 job 拉起。 */
    @Test
    public void specChangeAppliesNewSpecsAndNewJob() {
        String sandboxId = createSandbox();
        jdbc.update("update ds_sandbox set kuscia_job_id='ds-old-job-xxx' where id=?", sandboxId);

        Map<String, Object> payload = createPayload("SPEC_CHANGE", sandboxId);
        payload.put("cpuCores", 4);
        payload.put("memoryGb", 8);
        payload.put("gpuCount", 1);
        payload.put("storageGb", 20);
        String id = String.valueOf(approvalService.submit(payload).get("id"));
        approveStage1(id);
        approveStage2(id);
        // DeleteJob is asynchronous in Kuscia; the mock reports the old job as gone so the
        // executor must wait for deletion before creating the replacement job.
        JobService.State.jobQueryCode = FAIL_CODE;
        approvalService.executeApprovals();
        JobService.State.jobQueryCode = KusciaAPIConstants.OK;

        assertEquals("COMPLETED", approvalStatus(id));
        Map<String, Object> sbx = jdbc.queryForMap("select cpu_cores,memory_gb,gpu_count,storage_gb,kuscia_job_id,alloc_state from ds_sandbox where id=?", sandboxId);
        assertEquals(4d, ((Number) sbx.get("cpu_cores")).doubleValue());
        assertEquals(8d, ((Number) sbx.get("memory_gb")).doubleValue());
        assertEquals(20d, ((Number) sbx.get("storage_gb")).doubleValue());
        assertNotEquals("ds-old-job-xxx", String.valueOf(sbx.get("kuscia_job_id")));
        // 旧分配全部释放（SPEC_CHANGE，CPU/MEMORY/STORAGE 三条），新规格分配预留
        assertEquals(3L, count("select count(1) from ds_resource_allocation where sandbox_id=? and released_by='SPEC_CHANGE'", sandboxId));
        assertEquals("RESERVED", String.valueOf(sbx.get("alloc_state")));
        Map<String, Object> cpuRow = jdbc.queryForMap("select amount from ds_resource_allocation where sandbox_id=? and resource_type='CPU' and state in ('RESERVED','BOUND')", sandboxId);
        assertEquals(4d, ((Number) cpuRow.get("amount")).doubleValue());
        // 新规格运行同步 → BOUND
        UserContext.setBaseUser(alice());
        service.syncKusciaStatuses();
        assertEquals("BOUND", String.valueOf(jdbc.queryForMap("select alloc_state from ds_sandbox where id=?", sandboxId).get("alloc_state")));
    }

    /** 8. RECYCLE：停删 job → 软删沙箱 → 分配按 DESTROY 释放。 */
    @Test
    public void recycleSoftDeletesAndReleases() {
        String sandboxId = createSandbox();
        jdbc.update("update ds_sandbox set kuscia_job_id='ds-rc-xxx' where id=?", sandboxId);

        Map<String, Object> payload = createPayload("RECYCLE", sandboxId);
        String id = String.valueOf(approvalService.submit(payload).get("id"));
        approveStage1(id);
        approveStage2(id);
        approvalService.executeApprovals();

        assertEquals("COMPLETED", approvalStatus(id));
        Map<String, Object> sbx = jdbc.queryForMap("select status,deleted from ds_sandbox where id=?", sandboxId);
        assertEquals("DESTROYED", String.valueOf(sbx.get("status")));
        assertEquals(1, ((Number) sbx.get("deleted")).intValue());
        // 全部分配行按 DESTROY 释放（CPU/MEMORY/STORAGE 三条）
        assertEquals(3L, count("select count(1) from ds_resource_allocation where sandbox_id=? and released_by='DESTROY'", sandboxId));
    }

    /** 历史沙箱没有项目关联时，仅允许创建人在所属节点直接提交回收申请。 */
    @Test
    public void recycleLegacySandboxWithoutProject() {
        String sandboxId = createSandbox();
        jdbc.update("update ds_sandbox set project_id='' where id=?", sandboxId);

        String id = String.valueOf(approvalService.submit(createPayload("RECYCLE", sandboxId)).get("id"));
        assertEquals("APPROVED", approvalStatus(id));
        assertEquals("", String.valueOf(jdbc.queryForMap(
                "select project_snapshot_at from ds_sandbox_approval where id=?", id).get("project_snapshot_at")));

        approvalService.executeApprovals();

        assertEquals("COMPLETED", approvalStatus(id));
        Map<String, Object> sandbox = jdbc.queryForMap("select status,deleted from ds_sandbox where id=?", sandboxId);
        assertEquals("DESTROYED", String.valueOf(sandbox.get("status")));
        assertEquals(1, ((Number) sandbox.get("deleted")).intValue());

        String otherSandboxId = createSandbox();
        jdbc.update("update ds_sandbox set project_id='' where id=?", otherSandboxId);
        Map<String, Object> renew = createPayload("RENEW", otherSandboxId);
        renew.put("expiresAt", LocalDateTime.now().plusDays(7).toString());
        assertThrows(IllegalArgumentException.class, () -> approvalService.submit(renew));
    }

    /** 9. 卡死兜底：EXECUTING 超 10 分钟未更新 → 回退 APPROVED（自动重试），已达上限 → FAILED。 */
    @Test
    public void stuckExecutingReclaimed() {
        String id = submitAndApprove();
        // 第一次：卡死未达上限 → 回退 APPROVED（复用同一申请单，避免撞提交幂等）
        jdbc.update("update ds_sandbox_approval set status='EXECUTING',current_stage='EXECUTING',updated_at=? where id=?",
                LocalDateTime.now().minusMinutes(11).toString(), id);
        approvalService.reclaimStuckExecuting();
        assertEquals("APPROVED", approvalStatus(id), "未达重试上限的卡死单应回退 APPROVED 自动重试");

        // 第二次：同一单卡死且已达上限 → FAILED
        jdbc.update("update ds_sandbox_approval set status='EXECUTING',current_stage='EXECUTING',retry_count=3,updated_at=? where id=?",
                LocalDateTime.now().minusMinutes(11).toString(), id);
        approvalService.reclaimStuckExecuting();
        assertEquals("FAILED", approvalStatus(id), "已达上限的卡死单应置 FAILED");
    }

    /** 已挂载数据须经项目其余节点一致同意，审批执行后同时移除目录、项目引用和物理对象。 */
    @Test
    public void mountedAssetDeletionRequiresUnanimousProjectApproval() {
        String assetId = "asset-delete-it-mounted";
        jdbc.update("insert into ds_data_asset(id,name,provider_node_id,processor_node_id,ingestion_type,modality,data_stage,source_asset_id,datatable_id,storage_uri,metadata_json,created_by,created_at,updated_at,version,status,deleted) values(?,?,?,?,?,'TABULAR','RAW','',?,'','{}',?,?,?,1,'ACTIVE',0)",
                assetId, "待删除数据", "alice", "alice", "FILE", assetId, "alice", LocalDateTime.now().toString(), LocalDateTime.now().toString());
        jdbc.update("insert into ds_project_asset(project_id,asset_id,provider_node_id,asset_json,attached_by,attached_at,expires_at,deleted,is_deleted,gmt_create,gmt_modified) values('p1',?,?,'{}','alice','','',0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
                assetId, "alice");
        jdbc.update("insert into project_datatable(project_id,node_id,datatable_id,table_configs,source,is_deleted) values('p1','alice',?,'[]','IMPORTED',0)", assetId);

        Map<String, Object> result = dataAssetService.delete(assetId);
        assertEquals("PENDING_APPROVAL", result.get("status"));
        assertEquals(0L, count("select deleted from ds_data_asset where id=?", assetId));
        String approvalId = String.valueOf(((List<?>) result.get("approvalIds")).get(0));
        assertEquals("DATA_PROVIDER_REVIEW", approvalStatus(approvalId));

        UserContext.setBaseUser(carol());
        approvalService.approvalAction(Map.of("id", approvalId, "action", "APPROVE", "comment", "同意删除"));
        assertEquals("APPROVED", approvalStatus(approvalId));
        jdbc.update("update ds_sandbox_approval set status='EXECUTING',current_stage='EXECUTING' where id=?", approvalId);
        UserContext.setBaseUser(alice());
        approvalService.executeOne(approvalId);

        assertEquals("COMPLETED", approvalStatus(approvalId));
        assertEquals(1L, count("select deleted from ds_data_asset where id=?", assetId));
        assertEquals(1L, count("select is_deleted from ds_project_asset where project_id='p1' and asset_id=?", assetId));
        assertEquals(1L, count("select is_deleted from project_datatable where project_id='p1' and datatable_id=?", assetId));
    }

    /** 两个节点使用同名管理员时，合作方仍须看到审批动作，且不能冒充申请人撤回。 */
    @Test
    public void sameUsernameOnDifferentNodesRemainsIncomingReviewer() {
        UserContext.setBaseUser(user("devadmin", "alice"));
        String approvalId = submitCreate();

        UserContext.setBaseUser(user("devadmin", "carol"));
        Map<String, Object> incoming = approvalService.listApprovals("", "", "").stream()
                .filter(row -> approvalId.equals(row.get("id")))
                .findFirst().orElseThrow();
        assertEquals("INCOMING", incoming.get("direction"));
        assertThrows(SecretpadException.class,
                () -> approvalService.approvalAction(Map.of("id", approvalId, "action", "CANCEL")));

        approvalService.approvalAction(Map.of("id", approvalId, "action", "APPROVE", "comment", "同意"));
        assertEquals("APPROVED", approvalStatus(approvalId));
    }

    /** 归档项目遗留的资产关系不应触发删除审批。 */
    @Test
    public void archivedProjectAssetReferenceDoesNotRequireApproval() {
        String assetId = "asset-delete-it-archived";
        jdbc.update("insert into ds_data_asset(id,name,provider_node_id,processor_node_id,ingestion_type,modality,data_stage,source_asset_id,datatable_id,storage_uri,metadata_json,created_by,created_at,updated_at,version,status,deleted) values(?,?,?,?,?,'TABULAR','PROCESSED','',?,'','{}',?,?,?,1,'ACTIVE',0)",
                assetId, "归档项目数据", "alice", "alice", "GOVERNANCE", assetId, "alice",
                LocalDateTime.now().toString(), LocalDateTime.now().toString());
        jdbc.update("insert into ds_project_asset(project_id,asset_id,provider_node_id,asset_json,attached_by,attached_at,expires_at,deleted,is_deleted,gmt_create,gmt_modified) values('p1',?,?,'{}','alice','','',0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
                assetId, "alice");
        jdbc.update("update project set status=2 where project_id='p1'");
        jdbc.update("update project_node set is_deleted=1 where project_id='p1'");

        Map<String, Object> result = dataAssetService.delete(assetId);

        assertEquals("DELETED", result.get("status"));
        assertEquals(1L, count("select deleted from ds_data_asset where id=?", assetId));
        assertEquals(0L, count("select count(1) from ds_sandbox_approval where sandbox_id=?", assetId));
    }

    /** 源数据存在活动衍生资产时返回明确的数据业务错误。 */
    @Test
    public void derivedAssetReferenceReturnsBusinessError() {
        String sourceId = "asset-delete-it-source";
        String childId = "asset-delete-it-child";
        String now = LocalDateTime.now().toString();
        jdbc.update("insert into ds_data_asset(id,name,provider_node_id,processor_node_id,ingestion_type,modality,data_stage,source_asset_id,datatable_id,storage_uri,metadata_json,created_by,created_at,updated_at,version,status,deleted) values(?,?,?,?,?,'TABULAR','RAW','',?,'','{}',?,?,?,1,'ACTIVE',0)",
                sourceId, "源数据", "alice", "alice", "FILE", sourceId, "alice", now, now);
        jdbc.update("insert into ds_data_asset(id,name,provider_node_id,processor_node_id,ingestion_type,modality,data_stage,source_asset_id,datatable_id,storage_uri,metadata_json,created_by,created_at,updated_at,version,status,deleted) values(?,?,?,?,?,'TABULAR','PROCESSED',?,?,'','{}',?,?,?,1,'ACTIVE',0)",
                childId, "衍生数据", "alice", "alice", "GOVERNANCE", sourceId, childId, "alice", now, now);

        SecretpadException error = assertThrows(SecretpadException.class,
                () -> dataAssetService.delete(sourceId));

        assertEquals(DataErrorCode.DATA_ASSET_HAS_DERIVED_ASSET, error.getErrorCode());
    }

    /** 多项目删除审批中任一项目拒绝后，其余审批不得被误标记为完成。 */
    @Test
    public void rejectedProjectPreventsAssetDeletionGroupCompletion() {
        String assetId = "asset-delete-it-rejected-group";
        String now = LocalDateTime.now().toString();
        jdbc.update("insert into project(project_id,name,owner_id,status,is_deleted) values('p2','Approval IT 2','alice',1,0)");
        jdbc.update("insert into project_node(project_id,node_id,is_deleted) values('p2','alice',0)");
        jdbc.update("insert into project_node(project_id,node_id,is_deleted) values('p2','carol',0)");
        jdbc.update("insert into ds_data_asset(id,name,provider_node_id,processor_node_id,ingestion_type,modality,data_stage,source_asset_id,datatable_id,storage_uri,metadata_json,created_by,created_at,updated_at,version,status,deleted) values(?,?,?,?,?,'TABULAR','RAW','',?,'','{}',?,?,?,1,'ACTIVE',0)",
                assetId, "多项目审批数据", "alice", "alice", "FILE", assetId, "alice", now, now);
        jdbc.update("insert into ds_project_asset(project_id,asset_id,provider_node_id,asset_json,attached_by,attached_at,expires_at,deleted,is_deleted,gmt_create,gmt_modified) values('p1',?,?,'{}','alice','','',0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", assetId, "alice");
        jdbc.update("insert into ds_project_asset(project_id,asset_id,provider_node_id,asset_json,attached_by,attached_at,expires_at,deleted,is_deleted,gmt_create,gmt_modified) values('p2',?,?,'{}','alice','','',0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", assetId, "alice");

        Map<String, Object> result = dataAssetService.delete(assetId);
        List<Map<String, Object>> approvals = jdbc.queryForList("select id,project_id from ds_sandbox_approval where sandbox_id=? order by project_id", assetId);
        assertEquals(2, approvals.size());
        String rejectedId = String.valueOf(approvals.get(0).get("id"));
        String approvedId = String.valueOf(approvals.get(1).get("id"));

        UserContext.setBaseUser(carol());
        approvalService.approvalAction(Map.of("id", rejectedId, "action", "REJECT", "comment", "拒绝删除"));
        approvalService.approvalAction(Map.of("id", approvedId, "action", "APPROVE", "comment", "同意删除"));
        jdbc.update("update ds_sandbox_approval set status='EXECUTING',current_stage='EXECUTING' where id=?", approvedId);
        UserContext.setBaseUser(alice());
        approvalService.executeOne(approvedId);

        assertEquals("REJECTED", approvalStatus(rejectedId));
        assertEquals("REJECTED", approvalStatus(approvedId));
        assertEquals(0L, count("select deleted from ds_data_asset where id=?", assetId));
        assertEquals(2L, count("select count(1) from ds_project_asset where asset_id=? and is_deleted=0", assetId));
        assertEquals("PENDING_APPROVAL", result.get("status"));
    }

    /** 审批提交后新增项目引用时，旧审批快照不得覆盖并删除新增引用。 */
    @Test
    public void projectReferenceAddedAfterApprovalCausesConflict() {
        String assetId = "asset-delete-it-reference-race";
        String now = LocalDateTime.now().toString();
        jdbc.update("insert into ds_data_asset(id,name,provider_node_id,processor_node_id,ingestion_type,modality,data_stage,source_asset_id,datatable_id,storage_uri,metadata_json,created_by,created_at,updated_at,version,status,deleted) values(?,?,?,?,?,'TABULAR','RAW','',?,'','{}',?,?,?,1,'ACTIVE',0)",
                assetId, "引用变化数据", "alice", "alice", "FILE", assetId, "alice", now, now);
        jdbc.update("insert into ds_project_asset(project_id,asset_id,provider_node_id,asset_json,attached_by,attached_at,expires_at,deleted,is_deleted,gmt_create,gmt_modified) values('p1',?,?,'{}','alice','','',0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", assetId, "alice");
        Map<String, Object> result = dataAssetService.delete(assetId);
        String approvalId = String.valueOf(((List<?>) result.get("approvalIds")).get(0));
        UserContext.setBaseUser(carol());
        approvalService.approvalAction(Map.of("id", approvalId, "action", "APPROVE", "comment", "同意删除"));

        jdbc.update("insert into project(project_id,name,owner_id,status,is_deleted) values('p2','Approval IT 2','alice',1,0)");
        jdbc.update("insert into project_node(project_id,node_id,is_deleted) values('p2','alice',0)");
        jdbc.update("insert into project_node(project_id,node_id,is_deleted) values('p2','carol',0)");
        jdbc.update("insert into ds_project_asset(project_id,asset_id,provider_node_id,asset_json,attached_by,attached_at,expires_at,deleted,is_deleted,gmt_create,gmt_modified) values('p2',?,?,'{}','alice','','',0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", assetId, "alice");
        jdbc.update("update ds_sandbox_approval set status='EXECUTING',current_stage='EXECUTING' where id=?", approvalId);
        UserContext.setBaseUser(alice());
        approvalService.executeOne(approvalId);

        assertEquals("APPROVED", approvalStatus(approvalId));
        assertEquals(1L, count("select retry_count from ds_sandbox_approval where id=?", approvalId));
        assertEquals(0L, count("select deleted from ds_data_asset where id=?", assetId));
        assertEquals(1L, count("select count(1) from ds_project_asset where project_id='p2' and asset_id=? and is_deleted=0", assetId));
    }

    /** 数据目录应展示挂载项目；其他项目节点查看时应标记为项目共享数据。 */
    @Test
    public void catalogShowsMountedProjectsAndSharedStatus() {
        String assetId = "asset-delete-it-catalog";
        jdbc.update("insert into ds_data_asset(id,name,provider_node_id,processor_node_id,ingestion_type,modality,data_stage,source_asset_id,datatable_id,storage_uri,metadata_json,created_by,created_at,updated_at,version,status,deleted) values(?,?,?,?,?,'TABULAR','RAW','',?,'','{}',?,?,?,1,'ACTIVE',0)",
                assetId, "共享目录数据", "alice", "alice", "FILE", assetId, "alice",
                LocalDateTime.now().toString(), LocalDateTime.now().toString());
        jdbc.update("insert into ds_project_asset(project_id,asset_id,provider_node_id,asset_json,attached_by,attached_at,expires_at,deleted,is_deleted,gmt_create,gmt_modified) values('p1',?,?,'{}','alice','','',0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
                assetId, "alice");

        Map<String, Object> owned = dataAssetService.catalog("").stream()
                .filter(asset -> assetId.equals(asset.get("id"))).findFirst().orElseThrow();
        assertEquals(false, owned.get("project_shared"));
        assertEquals(1, owned.get("mounted_project_count"));
        assertEquals("Approval IT", ((List<?>) owned.get("mounted_projects")).stream()
                .map(project -> ((Map<?, ?>) project).get("name")).findFirst().orElseThrow());

        UserContext.setBaseUser(carol());
        List<Map<String, Object>> sharedRows = dataAssetService.catalog("").stream()
                .filter(asset -> assetId.equals(asset.get("id"))).toList();
        assertEquals(1, sharedRows.size(), "同一共享数据在目录中只能出现一次");
        assertEquals(true, sharedRows.get(0).get("project_shared"));
        assertEquals(1, sharedRows.get(0).get("mounted_project_count"));
    }
}
