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

import com.sun.net.httpserver.HttpServer;
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

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Z-02 告警与通知集成测试：raiseAlert 按 (source, dedupe_key) 去重、NODE_METRIC 真实指标阈值
 * （WARNING/CRITICAL）、单用户配额使用率告警、沙箱异常告警（进入 ERROR），以及 alert.created
 * webhook HMAC 送达。
 */
@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
@ActiveProfiles("test")
@SpringBootTest(classes = SecretPadApplication.class)
@TestPropertySource(properties = {
        "kuscia.nodes=",
        // 本类不走真实 Kuscia Job：sandboxAction START 因运行时未启用直接 ERROR，用于沙箱异常告警
        "secretpad.data-sandbox.kuscia.enabled=false",
        "secretpad.data-sandbox.alerts.quota-warning-percent=50",
        "secretpad.node-id=kuscia-system",
        "secretpad.data-sandbox.snapshot-root=${java.io.tmpdir}/ds-sandbox-alerts-snapshots",
        "secretpad.data-sandbox.backup-root=${java.io.tmpdir}/ds-sandbox-alerts-backups",
        "spring.datasource.default.jdbc-url=jdbc:sqlite:${java.io.tmpdir}/ds-sandbox-alerts-it.sqlite",
        "spring.datasource.quartz.jdbc-url=jdbc:h2:${java.io.tmpdir}/ds-sandbox-alerts-it-quartz.mv.db;DB_CLOSE_ON_EXIT=FALSE",
})
public class DataSandboxAlertsIT {

    @Resource
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbc;

    @Resource
    private DataSandboxMvpService service;

    @BeforeEach
    public void reset() {
        jdbc.update("delete from ds_sandbox");
        jdbc.update("delete from ds_sandbox_snapshot");
        jdbc.update("delete from ds_resource_allocation");
        jdbc.update("delete from ds_node_metric");
        jdbc.update("delete from ds_alert_event");
        jdbc.update("delete from ds_webhook_delivery");
        jdbc.update("delete from ds_webhook");
        jdbc.update("update ds_gpu_ledger set status='AVAILABLE',owner_id='',allocated_at=''");
        jdbc.update("update ds_resource_quota set cpu_cores=4,memory_gb=16,gpu_count=2,storage_gb=256 where owner_id='alice'");
        UserContext.setBaseUser(UserContextDTO.builder().ownerId("alice").name("alice")
                .platformType(PlatformTypeEnum.CENTER).platformNodeId("alice")
                .ownerType(UserOwnerTypeEnum.CENTER).projectIds(Set.of("p1")).build());
    }

    private void seedNodeMetric(double cpu, double mem, double storage) {
        // 每轮覆盖写入（与 ResourceCollector 语义一致）：先清旧行再插入，避免 PK 冲突
        jdbc.update("delete from ds_node_metric");
        jdbc.update("insert into ds_node_metric(id,node_id,cpu_cores,cpu_usage_percent,memory_total_gb,memory_available_gb,"
                        + "memory_usage_percent,storage_total_gb,storage_available_gb,storage_usage_percent,gpu_utilization_percent,"
                        + "source,status,raw_json,created_at) values(?,?,?,?,?,?,?,?,?,?,?,?,'FRESH','',?)",
                "node-metric-test", "test-node", 16d, cpu, 64d, 32d, mem, 100d, 50d, storage, -1d, "test", now());
    }

    private long openAlerts(String source, String title) {
        return jdbc.queryForObject("select count(1) from ds_alert_event where status='OPEN' and source=? and title=?",
                Long.class, source, title);
    }

    private String createSandbox(int cpu, int memGb) {
        Map<String, Object> created = service.createSandbox(Map.of(
                "name", "alert-it-sandbox", "ownerId", "alice", "imageId", "img-secretflow",
                "networkPolicy", "INTERNAL_ONLY", "cpuCores", cpu, "memoryGb", memGb, "gpuCount", 0,
                "storageGb", 10, "validDays", 7));
        return String.valueOf(created.get("id"));
    }

    private static String now() {
        return LocalDateTime.now().truncatedTo(java.time.temporal.ChronoUnit.SECONDS).toString();
    }

    private static String sha256(String value) throws Exception {
        return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(value.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void nodeMetricCriticalAlertAndDedup() {
        seedNodeMetric(95, 70, 50); // CPU 95% ≥ critical(90)
        service.checkNodeMetricsAlerts();
        assertEquals(1L, openAlerts("NODE_METRIC", "CPU 节点使用率危险"));
        assertEquals(1L, jdbc.queryForObject("select count(1) from ds_alert_event where status='OPEN' and source='NODE_METRIC' and severity='CRITICAL'", Long.class));
        // 再次触发不去重失败：同 dedupe_key 只保留一条 OPEN
        service.checkNodeMetricsAlerts();
        assertEquals(1L, openAlerts("NODE_METRIC", "CPU 节点使用率危险"));
    }

    @Test
    public void nodeMetricWarningLevelAndResolveRetriggers() {
        seedNodeMetric(10, 10, 88); // STORAGE 88% ≥ warning(85) 且 < critical(90)
        service.checkNodeMetricsAlerts();
        assertEquals(1L, openAlerts("NODE_METRIC", "STORAGE 节点使用率告警"));
        assertEquals(1L, jdbc.queryForObject("select count(1) from ds_alert_event where status='OPEN' and source='NODE_METRIC' and severity='WARNING'", Long.class));
        // 低于阈值不产生告警
        seedNodeMetric(10, 10, 10);
        jdbc.update("update ds_alert_event set status='RESOLVED',resolved_at=? where source='NODE_METRIC'", now());
        service.checkNodeMetricsAlerts();
        assertEquals(0L, openAlerts("NODE_METRIC", "CPU 节点使用率告警"));
    }

    @Test
    public void quotaUsageAlertAndDedup() {
        createSandbox(2, 2); // CPU 2/4 = 50% ≥ 阈值 50
        service.expireSandboxesAndCheckAlerts();
        assertEquals(1L, openAlerts("RESOURCE", "alice 配额使用率告警"));
        assertTrue(jdbc.queryForObject("select detail from ds_alert_event where status='OPEN' and source='RESOURCE' and title='alice 配额使用率告警'", String.class).contains("CPU"));
        // 去重：再次调用不新增
        service.expireSandboxesAndCheckAlerts();
        assertEquals(1L, openAlerts("RESOURCE", "alice 配额使用率告警"));
    }

    @Test
    public void sandboxErrorAlertOnStartFailureAndRetriggerAfterResolve() {
        String id = createSandbox(1, 2);
        // kuscia 运行时未启用 → 启动失败 → 状态 ERROR + SANDBOX 告警
        service.sandboxAction(Map.of("id", id, "action", "START"));
        assertEquals("ERROR", jdbc.queryForObject("select status from ds_sandbox where id=?", String.class, id));
        assertEquals(1L, openAlerts("SANDBOX", "沙箱异常"));
        // 去重：再次触发不新增
        service.sandboxAction(Map.of("id", id, "action", "START"));
        assertEquals(1L, openAlerts("SANDBOX", "沙箱异常"));
        // RESOLVED 后可再次触发
        String alertId = jdbc.queryForObject("select id from ds_alert_event where status='OPEN' and source='SANDBOX' and title='沙箱异常'", String.class);
        service.resolveAlert(alertId);
        service.sandboxAction(Map.of("id", id, "action", "START"));
        assertEquals(1L, openAlerts("SANDBOX", "沙箱异常"));
    }

    @Test
    public void alertWebhookDeliveredWithHmac() throws Exception {
        AtomicReference<String> capturedSignature = new AtomicReference<>();
        AtomicReference<String> capturedBody = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/hook", exchange -> {
            capturedSignature.set(exchange.getRequestHeaders().getFirst("X-Data-Sandbox-Signature"));
            capturedBody.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();
        try {
            jdbc.update("insert into ds_webhook(id,name,url,events,secret,enabled,created_by,created_at,updated_at) values(?,?,'http://127.0.0.1:" + server.getAddress().getPort() + "/hook','*','s3cr3t',1,'test',?,?)",
                    "wh-test", "test-webhook", now(), now());
            seedNodeMetric(95, 70, 50);
            service.checkNodeMetricsAlerts();
            // 告警已生成
            assertEquals(1L, openAlerts("NODE_METRIC", "CPU 节点使用率危险"));
            // webhook 送达 SUCCESS + HMAC 签名
            Map<String, Object> delivery = jdbc.queryForMap("select status,payload from ds_webhook_delivery where webhook_id='wh-test' order by created_at desc limit 1");
            assertEquals("SUCCESS", delivery.get("status"));
            String payload = String.valueOf(delivery.get("payload"));
            assertTrue(payload.contains("\"event\":\"alert.created\""), payload);
            // 签名 = sha256(secret + payload)
            assertEquals(sha256("s3cr3t" + payload), capturedSignature.get());
            assertEquals(payload, capturedBody.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    public void reclaimAbnormalAlertRecorded() {
        String id = createSandbox(1, 2);
        // 构造卡死：ERROR + 分配超 10 分钟
        jdbc.update("update ds_sandbox set status='ERROR',intent='' where id=?", id);
        jdbc.update("update ds_resource_allocation set created_at=datetime('now','-11 minutes') where sandbox_id=?", id);
        service.reclaimAbnormalAllocations();
        assertEquals(1L, openAlerts("SANDBOX", "资源异常回收"));
        // 去重
        service.reclaimAbnormalAllocations();
        assertEquals(1L, openAlerts("SANDBOX", "资源异常回收"));
    }
}
