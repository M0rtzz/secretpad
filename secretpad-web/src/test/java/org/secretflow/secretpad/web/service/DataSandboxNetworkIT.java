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
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Z-02 网络隔离集成测试：NO_NETWORK 使用 -nonet AppImage 变体（无 scope=Cluster 端点）且
 * dev-token/proxy 一律拒绝；INTERNAL_ONLY 保持原样；ALLOW_LIST 白名单 CRUD 登记 + 开发端点
 * 正常。与其它 IT 使用独立 mock 端口与 SQLite 文件。
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
        "secretpad.data-sandbox.snapshot-root=${java.io.tmpdir}/ds-sandbox-net-snapshots",
        "secretpad.data-sandbox.backup-root=${java.io.tmpdir}/ds-sandbox-net-backups",
        "spring.datasource.default.jdbc-url=jdbc:sqlite:${java.io.tmpdir}/ds-sandbox-network-it.sqlite",
        "spring.datasource.quartz.jdbc-url=jdbc:h2:${java.io.tmpdir}/ds-sandbox-network-it-quartz.mv.db;DB_CLOSE_ON_EXIT=FALSE",
})
public class DataSandboxNetworkIT {

    private static final String IMAGE_ID = "img-secretflow";
    private static final int MOCK_PORT = 50053;

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
        jdbc.update("delete from ds_network_allowlist");
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
        JobService.State.lastCreateJobRequest = null;
        UserContext.setBaseUser(UserContextDTO.builder().ownerId("alice").name("alice")
                .platformType(PlatformTypeEnum.CENTER).platformNodeId("alice")
                .ownerType(UserOwnerTypeEnum.CENTER).projectIds(Set.of("p1")).build());
    }

    private String createSandbox(String networkPolicy) {
        Map<String, Object> created = service.createSandbox(Map.of(
                "name", "net-it-sandbox", "ownerId", "alice", "imageId", IMAGE_ID,
                "networkPolicy", networkPolicy, "cpuCores", 1, "memoryGb", 2, "gpuCount", 0,
                "storageGb", 10, "validDays", 7));
        return String.valueOf(created.get("id"));
    }

    private void run() {
        JobService.State.jobState = "RUNNING";
        service.syncKusciaStatuses();
    }

    private String appImageOfLastCreateJob() {
        org.secretflow.v1alpha1.kusciaapi.Job.CreateJobRequest request = JobService.State.lastCreateJobRequest;
        assertTrue(request != null, "应捕获到 createJob 请求");
        return request.getTasks(0).getAppImage();
    }

    @Test
    public void noNetworkUsesNonetAppImageAndRejectsDevToken() {
        String id = createSandbox("NO_NETWORK");
        service.sandboxAction(Map.of("id", id, "action", "START"));
        run();
        // -nonet 变体已下发到 Kuscia
        assertEquals("data-sandbox-secretflow-nonet", appImageOfLastCreateJob());
        // 运行中但 NO_NETWORK 一律拒绝开发端点
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> service.generateDevToken(id));
        assertTrue(e.getMessage().contains("NO_NETWORK"), e.getMessage());
        // 纵深防御：即使 token 校验与转发目标也拒绝
        assertThrows(RuntimeException.class, () -> service.proxyTarget(id));
    }

    @Test
    public void internalOnlyUsesBaseAppImageAndKeepsDevEndpoint() {
        String id = createSandbox("INTERNAL_ONLY");
        service.sandboxAction(Map.of("id", id, "action", "START"));
        run();
        // INTERNAL_ONLY 行为不变：基础 AppImage（无 -nonet 后缀）+ 正常开发端点
        assertFalse(appImageOfLastCreateJob().endsWith("-nonet"));
        assertEquals("data-sandbox-secretflow", appImageOfLastCreateJob());
        Map<String, Object> issued = service.generateDevToken(id);
        assertTrue(String.valueOf(issued.get("url")).startsWith("/api/v1alpha1/data-sandbox/proxy/"));
    }

    @Test
    public void allowListKeepsDevEndpoint() {
        String id = createSandbox("ALLOW_LIST");
        service.sandboxAction(Map.of("id", id, "action", "START"));
        run();
        // ALLOW_LIST 策略仍提供集群内开发端点（egress 过滤留待集群环境，见隔离验证报告）
        Map<String, Object> issued = service.generateDevToken(id);
        assertTrue(String.valueOf(issued.get("url")).startsWith("/api/v1alpha1/data-sandbox/proxy/"));
        String endpoint = jdbc.queryForObject("select endpoint from ds_sandbox where id=?", String.class, id);
        assertEquals("10.0.0.1:31234", endpoint);
    }

    @Test
    public void proxyTargetReturnsRawEndpoint() {
        String id = createSandbox("INTERNAL_ONLY");
        service.sandboxAction(Map.of("id", id, "action", "START"));
        run();
        // 路由决策在 SandboxProxyController（.svc → secretpad.gateway 按 Host 头转发，
        // host:port → 直连）；service 层只返回 DB endpoint 原值，防 SSRF
        assertEquals("10.0.0.1:31234", service.proxyTarget(id));
    }

    @Test
    public void requireOwnerAllowsNodeOperatorByPlatformNodeId() {
        String id = createSandbox("INTERNAL_ONLY");
        service.sandboxAction(Map.of("id", id, "action", "START"));
        run();
        // 运维账号：ownerId 与沙箱 owner 不同，但 platformNodeId 与沙箱 owner 一致 → 允许进入
        UserContext.setBaseUser(UserContextDTO.builder().ownerId("ops-001").name("ops")
                .platformType(PlatformTypeEnum.CENTER).platformNodeId("alice")
                .ownerType(UserOwnerTypeEnum.CENTER).projectIds(Set.of("p1")).build());
        Map<String, Object> issued = service.generateDevToken(id);
        assertTrue(String.valueOf(issued.get("url")).startsWith("/api/v1alpha1/data-sandbox/proxy/"));
    }

    @Test
    public void allowlistCrudLifecycle() {
        String id = createSandbox("ALLOW_LIST");
        Map<String, Object> added = service.addNetworkAllowlist(Map.of(
                "sandboxId", id, "host", "db.internal.example.com", "port", 5432, "proto", "tcp", "remark", "训练库"));
        String entryId = String.valueOf(added.get("id"));
        assertTrue(String.valueOf(added.get("host")).equals("db.internal.example.com"));

        List<Map<String, Object>> list = service.listNetworkAllowlist(id);
        assertEquals(1, list.size());

        Map<String, Object> added2 = service.addNetworkAllowlist(Map.of(
                "sandboxId", id, "host", "192.168.1.10", "port", 6379, "proto", "udp"));
        assertEquals(2, service.listNetworkAllowlist(id).size());

        service.deleteNetworkAllowlist(entryId);
        List<Map<String, Object>> after = service.listNetworkAllowlist(id);
        assertEquals(1, after.size());
        assertEquals("192.168.1.10", after.get(0).get("host"));

        // 非法输入：port 越界 / proto 不支持 / 空 host
        assertThrows(IllegalArgumentException.class, () -> service.addNetworkAllowlist(Map.of(
                "sandboxId", id, "host", "x", "port", 70000)));
        assertThrows(IllegalArgumentException.class, () -> service.addNetworkAllowlist(Map.of(
                "sandboxId", id, "host", "x", "port", 80, "proto", "icmp")));
        assertThrows(IllegalArgumentException.class, () -> service.addNetworkAllowlist(Map.of(
                "sandboxId", id, "port", 80)));
        // 删除不存在的记录
        assertThrows(IllegalArgumentException.class, () -> service.deleteNetworkAllowlist("al-missing"));
        // 空 sandboxId 列出全部
        assertEquals(1, service.listNetworkAllowlist("").size());
        String _unused = String.valueOf(added2.get("id"));
    }
}
