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
import org.secretflow.secretpad.common.errorcode.AuthErrorCode;
import org.secretflow.secretpad.common.exception.SecretpadException;
import org.secretflow.secretpad.common.util.UserContext;
import org.secretflow.secretpad.kuscia.v1alpha1.service.impl.KusciaGrpcClientAdapter;
import org.secretflow.secretpad.web.service.sandbox.SandboxStatusMachine;
import org.secretflow.secretpad.web.service.storage.SandboxDbService;
import org.secretflow.secretpad.web.util.RequestUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.secretflow.v1alpha1.kusciaapi.Health;
import org.secretflow.v1alpha1.kusciaapi.Job;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Compact application service for the Data Sandbox MVP.
 *
 * <p>The service deliberately keeps the new feature inside SecretPad and uses the existing
 * JdbcTemplate and Kuscia client. It is intended as an operational MVP, not a replacement for
 * a full Kubernetes resource controller.</p>
 */
@Slf4j
@Service
public class DataSandboxMvpService {

    private static final Set<String> NETWORK_POLICIES = Set.of("INTERNAL_ONLY", "ALLOW_LIST", "NO_NETWORK");
    private static final Set<String> LOG_TYPES = Set.of("OPERATION", "AUDIT", "LOGIN", "SYSTEM");
    private static final Set<String> MODEL_STATES = Set.of("MODEL_REVIEW", "RESOURCE_REVIEW", "APPROVED", "REJECTED", "PUBLISHED");

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final KusciaGrpcClientAdapter kuscia;
    private final SandboxDbService sandboxDbService;
    private final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @Value("${secretpad.node-id:kuscia-system}")
    private String nodeId;

    @Value("${secretpad.data-sandbox.kuscia.enabled:false}")
    private boolean kusciaEnabled;

    @Value("${secretpad.data-sandbox.snapshot-root:/nas/Misc/data-sandbox/snapshots}")
    private String snapshotRoot;

    @Value("${secretpad.data-sandbox.backup-root:/nas/Misc/data-sandbox/backups}")
    private String backupRoot;

    @Value("${secretpad.data-sandbox.dev-endpoint.token-ttl-minutes:30}")
    private int devTokenTtlMinutes;

    @Value("${secretpad.data-sandbox.alerts.quota-warning-percent:90}")
    private int quotaWarningPercent;

    @Value("${spring.datasource.default.jdbc-url:jdbc:sqlite:./db/secretpad.sqlite}")
    private String databaseUrl;

    public DataSandboxMvpService(
            @Qualifier("jdbcTemplate") JdbcTemplate jdbc,
            ObjectMapper objectMapper,
            KusciaGrpcClientAdapter kuscia,
            SandboxDbService sandboxDbService) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.kuscia = kuscia;
        this.sandboxDbService = sandboxDbService;
    }

    /** Kuscia 运行时是否启用（供 Z-03 审批执行引擎判断 CREATE/SPEC_CHANGE 可否拉起）。 */
    public boolean isKusciaEnabled() {
        return kusciaEnabled;
    }

    /* ------------------------------- Sandbox ------------------------------- */

    public List<Map<String, Object>> listSandboxes(String ownerId, String keyword, String status) {
        StringBuilder sql = new StringBuilder("select s.*, i.name image_name, i.image_ref,n.name owner_node_name,p.name project_name from ds_sandbox s left join ds_sandbox_image i on i.id=s.image_id left join node n on (n.node_id=s.owner_id or n.inst_id=s.owner_id) and n.is_deleted=0 left join project p on p.project_id=s.project_id and p.is_deleted=0 where s.deleted=0");
        List<Object> args = new ArrayList<>();
        if (notBlank(ownerId)) {
            // 页面路由历史上使用机构实例 ID（node.inst_id），而审批创建的沙箱使用节点 ID。
            // 同时匹配两种身份，避免审批成功后因 ownerId 形态不同而在资源列表中消失。
            sql.append(" and (s.owner_id=? or s.owner_id in (select node_id from node where (node_id=? or inst_id=?) and is_deleted=0) or exists (select 1 from project_node pn where pn.project_id=s.project_id and (pn.node_id=? or pn.node_id in (select node_id from node where inst_id=? and is_deleted=0)) and pn.is_deleted=0))");
            args.add(ownerId);
            args.add(ownerId);
            args.add(ownerId);
            args.add(ownerId);
            args.add(ownerId);
        }
        if (notBlank(keyword)) {
            sql.append(" and (lower(s.name) like ? or lower(s.id) like ?)");
            String value = "%" + keyword.toLowerCase(Locale.ROOT) + "%";
            args.add(value);
            args.add(value);
        }
        if (notBlank(status)) {
            sql.append(" and s.status=?");
            args.add(status.toUpperCase(Locale.ROOT));
        }
        sql.append(" order by s.created_at desc");
        return jdbc.queryForList(sql.toString(), args.toArray());
    }

    @Transactional
    public Map<String, Object> createSandbox(Map<String, Object> request) {
        String id = "sbx-" + shortId();
        String name = required(request, "name");
        String ownerId = value(request, "ownerId", currentOwner());
        String projectId = required(request, "projectId");
        requireActiveProject(projectId, "创建沙箱");
        String imageId = required(request, "imageId");
        String networkPolicy = value(request, "networkPolicy", "INTERNAL_ONLY").toUpperCase(Locale.ROOT);
        if (!NETWORK_POLICIES.contains(networkPolicy)) {
            throw new IllegalArgumentException("不支持的网络策略: " + networkPolicy);
        }
        requireRow("select id from ds_sandbox_image where id=? and enabled=1", imageId);
        double cpu = positive(request, "cpuCores", 1);
        double memory = positive(request, "memoryGb", 2);
        int gpu = nonNegativeInt(request, "gpuCount", 0);
        double storage = positive(request, "storageGb", 10);
        String expiresAt;
        if (notBlank(string(request.get("expiresAt")))) {
            try {
                LocalDateTime expires = LocalDateTime.parse(string(request.get("expiresAt")))
                        .truncatedTo(ChronoUnit.SECONDS);
                if (!expires.isAfter(LocalDateTime.now())) {
                    throw new IllegalArgumentException("到期时间必须晚于当前时间");
                }
                if (expires.isAfter(LocalDateTime.now().plusDays(365))) {
                    throw new IllegalArgumentException("到期时间不能超过一年");
                }
                expiresAt = expires.toString();
            } catch (java.time.format.DateTimeParseException e) {
                throw new IllegalArgumentException("expiresAt 必须是有效的日期时间");
            }
        } else {
            // 兼容已进入审批流程、仍使用 validDays 的历史申请。
            int days = Math.max(1, Math.min(nonNegativeInt(request, "validDays", 7), 365));
            expiresAt = LocalDateTime.now().plusDays(days).truncatedTo(ChronoUnit.SECONDS).toString();
        }
        ensureQuota(ownerId);
        assertCapacity(ownerId, cpu, memory, gpu, storage);
        String now = now();
        jdbc.update("insert into ds_sandbox(id,name,description,owner_id,project_id,image_id,status,expires_at,network_policy,cpu_cores,memory_gb,gpu_count,storage_gb,created_by,created_at,updated_at) values(?,?,?,?,?,?,'STOPPED',?,?,?,?,?,?,?, ?,?)",
                id, name, value(request, "description", ""), ownerId, projectId, imageId,
                expiresAt, networkPolicy, cpu, memory, gpu, storage,
                value(request, "createdBy", actor()), now, now);
        audit("OPERATION", "SANDBOX_CREATE", "SANDBOX", id, json(request), true);
        // Z-02：创建即按规格预占资源（RESERVED），占住容量直到绑定或释放
        Map<String, Object> created = sandbox(id);
        reserveAllocations(created);
        appendRuntimeMeta(id, Map.of("spec", Map.of("cpu", cpu, "memory_gb", memory, "gpu", gpu, "storage_gb", storage), "alloc_state", "RESERVED"));
        dispatchWebhooks("sandbox.created", Map.of("sandboxId", id, "name", name, "ownerId", ownerId));
        return sandbox(id);
    }

    @Transactional
    public Map<String, Object> sandboxAction(Map<String, Object> request) {
        String id = required(request, "id");
        String action = required(request, "action").toUpperCase(Locale.ROOT);
        Map<String, Object> sandbox = sandbox(id);
        String status = string(sandbox.get("status"));
        if (Set.of("START", "STOP", "SNAPSHOT").contains(action)) {
            requireCreator(sandbox);
        }
        String error = "";
        switch (action) {
            case "START" -> {
                if ("EXPIRED".equals(status) || "DESTROYED".equals(status)) {
                    throw new IllegalStateException("已过期或销毁的沙箱不能启动");
                }
                // STARTING 可能是上次启动请求在状态同步前留下的中间态；启动操作本身幂等，
                // 允许再次触发已有 Kuscia Job 的 restart，或补建缺失的 Job。
                if (!"STARTING".equals(status)
                        && !SandboxStatusMachine.canAction(status, SandboxStatusMachine.Action.START)) {
                    throw new IllegalStateException("当前状态不允许启动: " + status);
                }
                // 先落库启动意图（STARTING + intent=START）再请求 Kuscia 创建真实 Job；
                // 成功保持 STARTING，由 syncKusciaStatuses 确认 Job RUNNING 后推进为 RUNNING；
                // 失败置 ERROR——禁止将未创建容器的记录标记为运行中。
                jdbc.update("update ds_sandbox set status='STARTING',intent='START',last_error='',updated_at=? where id=?", now(), id);
                // Z-02：重启（STOPPED 后已释放）时重新预占资源
                reserveAllocations(sandbox);
                error = startKuscia(sandbox);
                if (!error.isEmpty()) {
                    jdbc.update("update ds_sandbox set status='ERROR',intent='',last_error=?,updated_at=? where id=?", error, now(), id);
                    raiseSandboxErrorAlert(id, "启动失败：" + error);
                }
            }
            case "STOP" -> {
                if (!SandboxStatusMachine.canAction(status, SandboxStatusMachine.Action.STOP)) {
                    throw new IllegalStateException("当前状态不允许停止: " + status);
                }
                // 先落库停止意图（STOPPING + intent=STOP），由 sync 在 Kuscia Job 终态后置 STOPPED；
                // 停止失败置 ERROR，不假 STOPPED。
                jdbc.update("update ds_sandbox set status='STOPPING',intent='STOP',updated_at=? where id=?", now(), id);
                error = stopKuscia(sandbox, "Stopped from Data Sandbox console");
                if (!error.isEmpty()) {
                    jdbc.update("update ds_sandbox set status='ERROR',intent='',last_error=?,updated_at=? where id=?", error, now(), id);
                    raiseSandboxErrorAlert(id, "停止失败：" + error);
                } else if (!notBlank(string(sandbox.get("kuscia_job_id")))) {
                    // 无真实 Kuscia Job（运行时未启用或从未成功创建）：本地直接完成，
                    // 避免卡在 STOPPING（sync 只处理带 job 的记录）
                    jdbc.update("update ds_sandbox set status='STOPPED',intent='',updated_at=? where id=?", now(), id);
                    releaseAllocations(sandbox, "MANUAL");
                }
            }
            case "DESTROY" -> {
                if (!SandboxStatusMachine.canAction(status, SandboxStatusMachine.Action.DESTROY)) {
                    throw new IllegalStateException("当前状态不允许销毁: " + status);
                }
                error = deleteKuscia(sandbox);
                if (error.isEmpty()) {
                    jdbc.update("update ds_sandbox set status='DESTROYED',deleted=1,intent='',last_error='',updated_at=? where id=?", now(), id);
                    releaseAllocations(sandbox, "DESTROY");
                } else {
                    // 销毁失败保留记录并置 ERROR（不 throw，避免事务回滚丢失错误状态）
                    jdbc.update("update ds_sandbox set status='ERROR',intent='',last_error=?,updated_at=? where id=?", error, now(), id);
                    raiseSandboxErrorAlert(id, "销毁失败：" + error);
                }
            }
            case "RENEW" -> {
                LocalDateTime expiresAt;
                try {
                    expiresAt = LocalDateTime.parse(required(request, "expiresAt"));
                } catch (java.time.format.DateTimeParseException e) {
                    throw new IllegalArgumentException("expiresAt 必须是有效的日期时间");
                }
                if (!expiresAt.isAfter(LocalDateTime.now())) {
                    throw new IllegalArgumentException("新的到期时间必须晚于当前时间");
                }
                LocalDateTime currentExpiresAt = LocalDateTime.parse(string(sandbox.get("expires_at")));
                if (!expiresAt.isAfter(currentExpiresAt)) {
                    throw new IllegalArgumentException("新的到期时间必须晚于原到期时间");
                }
                jdbc.update("update ds_sandbox set expires_at=?,status=case when status='EXPIRED' then 'STOPPED' else status end,updated_at=? where id=?",
                        expiresAt.truncatedTo(ChronoUnit.SECONDS).toString(), now(), id);
            }
            case "SNAPSHOT" -> {
                if (!SandboxStatusMachine.canAction(status, SandboxStatusMachine.Action.SNAPSHOT)) {
                    throw new IllegalStateException("当前状态不允许快照: " + status);
                }
                createSnapshot(sandbox);
            }
            default -> throw new IllegalArgumentException("不支持的沙箱操作: " + action);
        }
        audit("OPERATION", "SANDBOX_" + action, "SANDBOX", id, error, error.isEmpty());
        dispatchWebhooks("sandbox." + action.toLowerCase(Locale.ROOT), Map.of("sandboxId", id, "status", status));
        return sandbox(id);
    }

    public Map<String, Object> sandbox(String id) {
        return requireRow("select s.*, i.name image_name, i.image_ref, i.kuscia_app_image from ds_sandbox s left join ds_sandbox_image i on i.id=s.image_id where s.id=?", id);
    }

    /* ------------------------------- Dev endpoint ------------------------------- */

    /**
     * 为运行中的沙箱签发一次性开发端点 token（DB 只存 sha256，明文仅本次返回），
     * 返回同域跳板 URL。每次进入重置有效期（默认 30 分钟）。
     */
    public Map<String, Object> generateDevToken(String sandboxId) {
        Map<String, Object> sandbox = sandbox(sandboxId);
        requireCreator(sandbox);
        // Z-02 网络隔离：NO_NETWORK 沙箱不暴露任何集群外端点，开发跳板一并拒绝
        if ("NO_NETWORK".equals(string(sandbox.get("network_policy")))) {
            throw new IllegalStateException("NO_NETWORK 沙箱不提供开发端点");
        }
        if (!"RUNNING".equals(string(sandbox.get("status")))) {
            throw new IllegalStateException("沙箱未运行，无法进入开发环境（当前状态: " + string(sandbox.get("status")) + "）");
        }
        if (!notBlank(string(sandbox.get("endpoint")))) {
            throw new IllegalStateException("沙箱开发端点尚未就绪，请稍后重试");
        }
        byte[] raw = new byte[16];
        new java.security.SecureRandom().nextBytes(raw);
        String token = java.util.HexFormat.of().formatHex(raw);
        String expiresAt = LocalDateTime.now().plusMinutes(Math.max(1, devTokenTtlMinutes)).truncatedTo(ChronoUnit.SECONDS).toString();
        jdbc.update("update ds_sandbox set endpoint_token=?,endpoint_token_expires_at=?,endpoint_updated_at=?,updated_at=? where id=?",
                sha256(token.getBytes(StandardCharsets.UTF_8)), expiresAt, now(), now(), sandboxId);
        audit("AUDIT", "DEV_TOKEN_ISSUE", "SANDBOX", sandboxId, "ttl=" + devTokenTtlMinutes + "m", true);
        return Map.of("url", "/api/v1alpha1/data-sandbox/proxy/" + sandboxId + "?token=" + token, "expiresAt", expiresAt);
    }

    /**
     * 校验开发端点访问凭证（恒时比较 + 未过期 + 沙箱 RUNNING），供跳板鉴权使用。
     * 失败抛 AuthErrorCode.AUTH_FAILED，由全局异常处理器返回明确业务错误码。
     */
    public void validateDevToken(String sandboxId, String token) {
        if (!notBlank(token)) {
            throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "缺少开发端点访问凭证，请重新进入开发环境");
        }
        Map<String, Object> sandbox = requireRow("select id,status,owner_id,endpoint,network_policy,endpoint_token,endpoint_token_expires_at from ds_sandbox where id=? and deleted=0", sandboxId);
        // Z-02 网络隔离纵深防御：NO_NETWORK 即使伪造 token 也拒绝
        if ("NO_NETWORK".equals(string(sandbox.get("network_policy")))) {
            throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "NO_NETWORK 沙箱不提供开发端点");
        }
        byte[] expected = string(sandbox.get("endpoint_token")).getBytes(StandardCharsets.UTF_8);
        byte[] actual = sha256(token.getBytes(StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
        if (!MessageDigest.isEqual(expected, actual)) {
            throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "开发端点访问凭证无效或已失效，请重新进入开发环境");
        }
        String expiresAt = string(sandbox.get("endpoint_token_expires_at"));
        if (notBlank(expiresAt)) {
            try {
                if (LocalDateTime.parse(expiresAt).isBefore(LocalDateTime.now())) {
                    throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "开发端点访问凭证已过期，请重新进入开发环境");
                }
            } catch (java.time.format.DateTimeParseException e) {
                throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "开发端点访问凭证已失效，请重新进入开发环境");
            }
        }
        if (!"RUNNING".equals(string(sandbox.get("status")))) {
            throw SecretpadException.of(AuthErrorCode.AUTH_FAILED,
                    "沙箱未运行，开发环境不可访问（当前状态: " + string(sandbox.get("status")) + "）");
        }
        auditAs("AUDIT", "INFO", "dev-proxy:" + sandboxId, "DEV_ENDPOINT_ACCESS", "SANDBOX", sandboxId, "", true);
    }

    /**
     * 跳板转发目标（token 已在拦截器校验通过）：返回 DB 中的 endpoint 原值，防 SSRF。
     * <p>Kuscia 的 endpoint 是集群头服务 hostname（如 ds-sbx-xxx-task-server-0-web.dev-zgz.svc）。
     * SandboxProxyController 依据 endpoint 是否 .svc 集群服务决定路由：集群服务经
     * {@code secretpad.gateway}（Kuscia 节点 envoy，按 Host 头路由到沙箱容器）转发；
     * 普通 host:port 则直连。endpoint 为空 / NO_NETWORK 一律拒绝。</p>
     */
    public String proxyTarget(String sandboxId) {
        Map<String, Object> sandbox = requireRow("select endpoint,network_policy from ds_sandbox where id=? and deleted=0", sandboxId);
        // Z-02 网络隔离纵深防御：NO_NETWORK 沙箱无集群外端点，拒绝转发
        if ("NO_NETWORK".equals(string(sandbox.get("network_policy")))) {
            throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "NO_NETWORK 沙箱不提供开发端点");
        }
        String endpoint = string(sandbox.get("endpoint"));
        if (!notBlank(endpoint)) {
            throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "沙箱开发端点尚未就绪，请稍后重试");
        }
        return endpoint;
    }

    /** Development environments are private to the account that created them. */
    private void requireCreator(Map<String, Object> sandbox) {
        UserContextDTO user = UserContext.getUserOrNotExist();
        if (user == null) {
            throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "无权访问该沙箱的开发环境");
        }
        if (!Objects.equals(user.getName(), string(sandbox.get("created_by")))) {
            throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "无权访问该沙箱的开发环境");
        }
    }

    public List<Map<String, Object>> listSnapshots(String sandboxId) {
        return jdbc.queryForList("select * from ds_sandbox_snapshot where sandbox_id=? order by created_at desc", sandboxId);
    }

    public List<Map<String, Object>> listImages() {
        return jdbc.queryForList("select * from ds_sandbox_image order by enabled desc, created_at desc");
    }

    @Transactional
    public Map<String, Object> saveImage(Map<String, Object> request) {
        String id = value(request, "id", "img-" + shortId());
        String now = now();
        int changed = jdbc.update("update ds_sandbox_image set name=?,image_ref=?,kuscia_app_image=?,description=?,enabled=?,updated_at=? where id=?",
                required(request, "name"), required(request, "imageRef"), value(request, "kusciaAppImage", ""),
                value(request, "description", ""), bool(request, "enabled", true) ? 1 : 0, now, id);
        if (changed == 0) {
            jdbc.update("insert into ds_sandbox_image(id,name,image_ref,kuscia_app_image,description,enabled,created_by,created_at,updated_at) values(?,?,?,?,?,?,?,?,?)",
                    id, required(request, "name"), required(request, "imageRef"), value(request, "kusciaAppImage", ""),
                    value(request, "description", ""), bool(request, "enabled", true) ? 1 : 0, actor(), now, now);
        }
        audit("OPERATION", "IMAGE_SAVE", "SANDBOX_IMAGE", id, json(request), true);
        return requireRow("select * from ds_sandbox_image where id=?", id);
    }

    /* ------------------------------- Resources ------------------------------- */

    public Map<String, Object> resourceOverview(String ownerId) {
        List<Map<String, Object>> pools = jdbc.queryForList("select * from ds_resource_pool where enabled=1 order by resource_type");
        Map<String, Double> totalUsage = usage(null);
        Map<String, Double> ownerUsage = usage(ownerId);
        for (Map<String, Object> pool : pools) {
            String type = string(pool.get("resource_type"));
            double total = number(pool.get("total_amount"), 0);
            double used = totalUsage.getOrDefault(type, 0d);
            pool.put("used_amount", used);
            pool.put("available_amount", Math.max(0, total - used));
            pool.put("usage_percent", total == 0 ? 0 : Math.round(used * 10000 / total) / 100d);
        }
        ensureQuota(notBlank(ownerId) ? ownerId : currentOwner());
        Map<String, Object> quota = requireRow("select * from ds_resource_quota where owner_id=?", notBlank(ownerId) ? ownerId : currentOwner());
        // Z-02：真实节点指标（ResourceCollector 写入 ds_node_metric）与 GPU 台账（ds_gpu_ledger）
        Map<String, Object> nodeMetrics = latestNodeMetric();
        Map<String, Object> metrics = new LinkedHashMap<>();
        if (nodeMetrics.isEmpty()) {
            metrics.put("status", "N/A");
            metrics.put("lastUpdatedAt", "");
        } else {
            metrics.put("status", nodeMetrics.get("status"));
            metrics.put("lastUpdatedAt", nodeMetrics.get("created_at"));
        }
        double gpuUtilization = number(nodeMetrics.get("gpu_utilization_percent"), -1);
        List<Map<String, Object>> gpuInventory = jdbc.queryForList("select * from ds_gpu_ledger order by id");
        for (Map<String, Object> gpu : gpuInventory) {
            gpu.put("utilization", gpuUtilization);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("pools", pools);
        result.put("quota", quota);
        result.put("ownerUsage", ownerUsage);
        result.put("gpuInventory", gpuInventory);
        result.put("nodeMetrics", nodeMetrics);
        result.put("metrics", metrics);
        return result;
    }

    private Map<String, Object> latestNodeMetric() {
        List<Map<String, Object>> rows = jdbc.queryForList("select * from ds_node_metric order by created_at desc limit 1");
        return rows.isEmpty() ? new LinkedHashMap<>() : rows.get(0);
    }

    @Transactional
    public Map<String, Object> saveQuota(Map<String, Object> request) {
        String ownerId = required(request, "ownerId");
        ensureQuota(ownerId);
        jdbc.update("update ds_resource_quota set cpu_cores=?,memory_gb=?,gpu_count=?,storage_gb=?,updated_by=?,updated_at=? where owner_id=?",
                positive(request, "cpuCores", 16), positive(request, "memoryGb", 64), nonNegativeInt(request, "gpuCount", 0),
                positive(request, "storageGb", 1024), actor(), now(), ownerId);
        audit("AUDIT", "RESOURCE_QUOTA_UPDATE", "RESOURCE_QUOTA", ownerId, json(request), true);
        return requireRow("select * from ds_resource_quota where owner_id=?", ownerId);
    }

    public List<Map<String, Object>> listAlerts(String status) {
        if (notBlank(status)) {
            return jdbc.queryForList("select * from ds_alert_event where status=? order by created_at desc", status.toUpperCase(Locale.ROOT));
        }
        return jdbc.queryForList("select * from ds_alert_event order by created_at desc limit 200");
    }

    @Transactional
    public void resolveAlert(String id) {
        jdbc.update("update ds_alert_event set status='RESOLVED',resolved_at=? where id=?", now(), id);
        audit("OPERATION", "ALERT_RESOLVE", "ALERT", id, "", true);
    }

    /* ------------------------------- Alerts (Z-02) ------------------------------- */

    /**
     * 告警统一入口：按 (source, dedupe_key) 对 OPEN 告警去重（dedupeKey 为空时按 source+title），
     * 插入后派发 alert.created webhook。dedupeKey 用于高频告警（节点指标/配额/沙箱异常）防刷屏。
     * 供 Z-03 审批执行引擎复用。
     */
    public void raiseAlert(String severity, String source, String title, String detail, String dedupeKey) {
        String dedupe = dedupeKey == null ? "" : dedupeKey;
        long open;
        if (notBlank(dedupe)) {
            open = count("select count(1) from ds_alert_event where status='OPEN' and source=? and dedupe_key=?", source, dedupe);
        } else {
            open = count("select count(1) from ds_alert_event where status='OPEN' and source=? and title=?", source, title);
        }
        if (open > 0) return;
        String id = "alert-" + shortId();
        String created = now();
        jdbc.update("insert into ds_alert_event(id,severity,source,title,detail,dedupe_key,status,created_at) values(?,?,?,?,?,?,'OPEN',?)",
                id, severity, source, title, truncate(detail, 1024), dedupe, created);
        dispatchWebhooks("alert.created", Map.of("id", id, "severity", severity, "source", source, "title", title, "detail", detail, "dedupeKey", dedupe));
    }

    /** 沙箱进入 ERROR 的统一告警（source=SANDBOX，按沙箱去重，RESOLVED 后可再次触发）。供 Z-03 审批执行引擎复用。 */
    public void raiseSandboxErrorAlert(String sandboxId, String detail) {
        raiseAlert("WARNING", "SANDBOX", "沙箱异常",
                "沙箱 " + sandboxId + " 进入 ERROR：" + truncate(detail, 900),
                "sandbox:" + sandboxId + ":error");
    }

    /* ------------------------------- Network allowlist (Z-02) ------------------------------- */

    public List<Map<String, Object>> listNetworkAllowlist(String sandboxId) {
        if (notBlank(sandboxId)) {
            return jdbc.queryForList("select * from ds_network_allowlist where sandbox_id=? order by created_at desc", sandboxId);
        }
        return jdbc.queryForList("select * from ds_network_allowlist order by created_at desc limit 500");
    }

    @Transactional
    public Map<String, Object> addNetworkAllowlist(Map<String, Object> request) {
        String sandboxId = required(request, "sandboxId");
        String host = required(request, "host").trim().toLowerCase(Locale.ROOT);
        int port = (int) number(request.get("port"), 0);
        String proto = value(request, "proto", "tcp").trim().toLowerCase(Locale.ROOT);
        if (port <= 0 || port > 65535) throw new IllegalArgumentException("port 必须为 1-65535");
        if (!Set.of("tcp", "udp").contains(proto)) throw new IllegalArgumentException("proto 仅支持 tcp/udp");
        String id = "al-" + shortId();
        jdbc.update("insert into ds_network_allowlist(id,sandbox_id,host,port,proto,remark,created_by,created_at) values(?,?,?,?,?,?,?,?)",
                id, sandboxId, host, port, proto, value(request, "remark", ""), actor(), now());
        audit("OPERATION", "NETWORK_ALLOWLIST_ADD", "NETWORK_ALLOWLIST", sandboxId, host + ":" + port + "/" + proto, true);
        return requireRow("select * from ds_network_allowlist where id=?", id);
    }

    @Transactional
    public void deleteNetworkAllowlist(String id) {
        if (count("select count(1) from ds_network_allowlist where id=?", id) == 0) {
            throw new IllegalArgumentException("白名单记录不存在: " + id);
        }
        jdbc.update("delete from ds_network_allowlist where id=?", id);
        audit("OPERATION", "NETWORK_ALLOWLIST_DELETE", "NETWORK_ALLOWLIST", id, "", true);
    }

    /* ------------------------------- Model approval ------------------------------- */

    public List<Map<String, Object>> listApprovals(String status, String keyword) {
        StringBuilder sql = new StringBuilder("select * from ds_model_approval where 1=1");
        List<Object> args = new ArrayList<>();
        if (notBlank(status)) {
            sql.append(" and status=?");
            args.add(status.toUpperCase(Locale.ROOT));
        }
        if (notBlank(keyword)) {
            sql.append(" and (lower(model_name) like ? or lower(model_id) like ?)");
            String q = "%" + keyword.toLowerCase(Locale.ROOT) + "%";
            args.add(q);
            args.add(q);
        }
        sql.append(" order by updated_at desc");
        return jdbc.queryForList(sql.toString(), args.toArray());
    }

    @Transactional
    public Map<String, Object> submitModel(Map<String, Object> request) {
        String id = "apr-" + shortId();
        String now = now();
        jdbc.update("insert into ds_model_approval(id,model_id,model_name,project_id,version,status,current_stage,description,submitter,submitted_at,updated_at) values(?,?,?,?,1,'MODEL_REVIEW','MODEL_REVIEW',?,?,?,?)",
                id, required(request, "modelId"), required(request, "modelName"), value(request, "projectId", ""),
                value(request, "description", ""), actor(), now, now);
        approvalHistory(id, "SUBMIT", "", "MODEL_REVIEW", value(request, "comment", ""));
        audit("AUDIT", "MODEL_SUBMIT", "MODEL_APPROVAL", id, json(request), true);
        dispatchWebhooks("model.submitted", Map.of("approvalId", id, "modelId", required(request, "modelId")));
        return approval(id);
    }

    @Transactional
    public Map<String, Object> approvalAction(Map<String, Object> request) {
        String id = required(request, "id");
        String action = required(request, "action").toUpperCase(Locale.ROOT);
        String comment = value(request, "comment", "");
        Map<String, Object> approval = approval(id);
        String from = string(approval.get("status"));
        String to;
        String stage;
        switch (action) {
            case "APPROVE" -> {
                if ("MODEL_REVIEW".equals(from)) {
                    to = "RESOURCE_REVIEW";
                    stage = "RESOURCE_REVIEW";
                } else if ("RESOURCE_REVIEW".equals(from)) {
                    to = "APPROVED";
                    stage = "COMPLETED";
                } else {
                    throw new IllegalStateException("当前状态不能审批通过: " + from);
                }
            }
            case "REJECT" -> {
                if (!Set.of("MODEL_REVIEW", "RESOURCE_REVIEW").contains(from)) {
                    throw new IllegalStateException("当前状态不能驳回: " + from);
                }
                to = "REJECTED";
                stage = string(approval.get("current_stage"));
            }
            case "RESUBMIT" -> {
                if (!"REJECTED".equals(from)) {
                    throw new IllegalStateException("只有已驳回的模型可以复审");
                }
                to = "MODEL_REVIEW";
                stage = "MODEL_REVIEW";
                jdbc.update("update ds_model_approval set version=version+1 where id=?", id);
            }
            case "PUBLISH" -> {
                if (!"APPROVED".equals(from)) {
                    throw new IllegalStateException("模型审批完成后才能发布");
                }
                to = "PUBLISHED";
                stage = "COMPLETED";
            }
            default -> throw new IllegalArgumentException("不支持的审批动作: " + action);
        }
        jdbc.update("update ds_model_approval set status=?,current_stage=?,reviewer=?,review_comment=?,updated_at=?,published_at=case when ?='PUBLISHED' then ? else published_at end where id=?",
                to, stage, actor(), comment, now(), to, now(), id);
        approvalHistory(id, action, from, to, comment);
        audit("AUDIT", "MODEL_" + action, "MODEL_APPROVAL", id, comment, true);
        dispatchWebhooks("model." + action.toLowerCase(Locale.ROOT), Map.of("approvalId", id, "from", from, "to", to));
        return approval(id);
    }

    public Map<String, Object> approval(String id) {
        Map<String, Object> data = requireRow("select * from ds_model_approval where id=?", id);
        data.put("history", approvalHistory(id));
        return data;
    }

    public List<Map<String, Object>> approvalHistory(String id) {
        return jdbc.queryForList("select * from ds_model_approval_history where approval_id=? order by id desc", id);
    }

    public void assertModelApproved(String modelId) {
        Integer count = jdbc.queryForObject("select count(1) from ds_model_approval where model_id=? and status in ('APPROVED','PUBLISHED')", Integer.class, modelId);
        if (count == null || count == 0) {
            throw new IllegalStateException("模型尚未完成模型与资源两级审批，不能发布 Serving");
        }
    }

    /* ------------------------------- Unified logs ------------------------------- */

    public List<Map<String, Object>> listLogs(String type, String level, String actor, String keyword, String start, String end, int limit) {
        StringBuilder sql = new StringBuilder("select * from ds_unified_log where 1=1");
        List<Object> args = new ArrayList<>();
        if (notBlank(type)) { sql.append(" and log_type=?"); args.add(type.toUpperCase(Locale.ROOT)); }
        if (notBlank(level)) { sql.append(" and level=?"); args.add(level.toUpperCase(Locale.ROOT)); }
        if (notBlank(actor)) { sql.append(" and actor=?"); args.add(actor); }
        if (notBlank(keyword)) {
            sql.append(" and (lower(action) like ? or lower(detail) like ? or lower(resource_id) like ?)");
            String q = "%" + keyword.toLowerCase(Locale.ROOT) + "%";
            args.add(q); args.add(q); args.add(q);
        }
        if (notBlank(start)) { sql.append(" and created_at>=?"); args.add(start); }
        if (notBlank(end)) { sql.append(" and created_at<=?"); args.add(end); }
        sql.append(" order by id desc limit ?");
        args.add(Math.max(1, Math.min(limit, 5000)));
        return jdbc.queryForList(sql.toString(), args.toArray());
    }

    public byte[] exportLogs(String type, String level, String actor, String keyword, String start, String end) {
        List<Map<String, Object>> logs = listLogs(type, level, actor, keyword, start, end, 5000);
        StringBuilder csv = new StringBuilder("id,log_type,level,actor,action,resource_type,resource_id,success,ip_address,created_at,detail\n");
        for (Map<String, Object> row : logs) {
            csv.append(csv(row.get("id"))).append(',').append(csv(row.get("log_type"))).append(',')
                    .append(csv(row.get("level"))).append(',').append(csv(row.get("actor"))).append(',')
                    .append(csv(row.get("action"))).append(',').append(csv(row.get("resource_type"))).append(',')
                    .append(csv(row.get("resource_id"))).append(',').append(csv(row.get("success"))).append(',')
                    .append(csv(row.get("ip_address"))).append(',').append(csv(row.get("created_at"))).append(',')
                    .append(csv(row.get("detail"))).append('\n');
        }
        return ("\ufeff" + csv).getBytes(StandardCharsets.UTF_8);
    }

    public List<Map<String, Object>> retentionPolicies() {
        return jdbc.queryForList("select * from ds_log_retention order by log_type");
    }

    @Transactional
    public void saveRetention(Map<String, Object> request) {
        String type = required(request, "logType").toUpperCase(Locale.ROOT);
        if (!LOG_TYPES.contains(type)) throw new IllegalArgumentException("无效日志类型");
        int days = Math.max(1, nonNegativeInt(request, "retentionDays", 90));
        jdbc.update("insert into ds_log_retention(log_type,retention_days,updated_by,updated_at) values(?,?,?,?) on conflict(log_type) do update set retention_days=excluded.retention_days,updated_by=excluded.updated_by,updated_at=excluded.updated_at",
                type, days, actor(), now());
        audit("AUDIT", "LOG_RETENTION_UPDATE", "LOG_POLICY", type, "retentionDays=" + days, true);
    }

    public void loginAttempt(String username, boolean success, String detail) {
        auditAs("LOGIN", success ? "INFO" : "WARN", username, "LOGIN", "USER", username, detail, success);
    }

    public void audit(String type, String action, String resourceType, String resourceId, String detail, boolean success) {
        auditAs(type, success ? "INFO" : "ERROR", actor(), action, resourceType, resourceId, detail, success);
    }

    /* ------------------------------- Integrations ------------------------------- */

    public Map<String, Object> integrationOverview() {
        List<Map<String, Object>> clients = jdbc.queryForList("select id,name,client_id,scopes,enabled,last_used_at,secret_version,created_by,created_at from ds_api_client order by created_at desc");
        List<Map<String, Object>> webhooks = jdbc.queryForList("select id,name,url,events,enabled,created_by,created_at,updated_at from ds_webhook order by created_at desc");
        List<Map<String, Object>> deliveries = jdbc.queryForList("select * from ds_webhook_delivery order by created_at desc limit 100");
        Map<String, Object> oidc = new HashMap<>(requireRow("select * from ds_oidc_config where id=1"));
        oidc.put("has_client_secret", notBlank(string(oidc.remove("client_secret"))));
        List<Map<String, Object>> oidcMappings = jdbc.queryForList("select * from ds_oidc_role_mapping order by created_at desc");
        return Map.of("clients", clients, "webhooks", webhooks, "deliveries", deliveries, "oidc", oidc,
                "oidcMappings", oidcMappings,
                "openapi", "/swagger-ui/index.html", "openapiJson", "/v3/api-docs");
    }

    @Transactional
    public Map<String, Object> createApiClient(Map<String, Object> request) {
        String id = "cli-" + shortId();
        String clientId = "ds_" + shortId() + shortId();
        String secret = Base64.getUrlEncoder().withoutPadding().encodeToString(randomBytes());
        jdbc.update("insert into ds_api_client(id,name,client_id,secret_hash,scopes,enabled,created_by,created_at) values(?,?,?,?,?,1,?,?)",
                id, required(request, "name"), clientId, sha256(secret.getBytes(StandardCharsets.UTF_8)), value(request, "scopes", "sandbox:read"), actor(), now());
        audit("AUDIT", "API_CLIENT_CREATE", "API_CLIENT", id, "", true);
        Map<String, Object> result = new HashMap<>(requireRow("select id,name,client_id,scopes,enabled,created_by,created_at from ds_api_client where id=?", id));
        result.put("client_secret", secret);
        result.put("notice", "客户端密钥只显示一次，请立即保存");
        return result;
    }

    /** Validate a machine credential without ever loading or storing the clear-text secret. */
    public boolean authenticateApiClient(String clientId, String clientSecret) {
        if (!notBlank(clientId) || !notBlank(clientSecret)) return false;
        try {
            Map<String, Object> client = requireRow("select id,secret_hash from ds_api_client where client_id=? and enabled=1", clientId);
            byte[] expected = string(client.get("secret_hash")).getBytes(StandardCharsets.UTF_8);
            byte[] actual = sha256(clientSecret.getBytes(StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
            boolean valid = MessageDigest.isEqual(expected, actual);
            if (valid) jdbc.update("update ds_api_client set last_used_at=? where id=?", now(), client.get("id"));
            auditAs("AUDIT", valid ? "INFO" : "WARN", clientId, "API_CLIENT_AUTH", "API_CLIENT", string(client.get("id")), "", valid);
            return valid;
        } catch (IllegalArgumentException e) {
            auditAs("AUDIT", "WARN", clientId, "API_CLIENT_AUTH", "API_CLIENT", "", "unknown client", false);
            return false;
        }
    }

    @Transactional
    public void revokeApiClient(String id) {
        jdbc.update("update ds_api_client set enabled=0 where id=?", id);
        audit("AUDIT", "API_CLIENT_REVOKE", "API_CLIENT", id, "", true);
    }

    @Transactional
    public Map<String, Object> rotateApiClient(String id) {
        Map<String, Object> client = requireRow("select * from ds_api_client where id=? and enabled=1", id);
        String secret = Base64.getUrlEncoder().withoutPadding().encodeToString(randomBytes());
        jdbc.update("update ds_api_client set secret_hash=?,secret_version=secret_version+1 where id=?",
                sha256(secret.getBytes(StandardCharsets.UTF_8)), id);
        audit("AUDIT", "API_CLIENT_ROTATE", "API_CLIENT", id, "", true);
        Map<String, Object> result = new LinkedHashMap<>(client);
        result.remove("secret_hash");
        result.put("client_secret", secret);
        result.put("secret_version", number(client.get("secret_version"), 1) + 1);
        result.put("notice", "新密钥只显示一次，旧密钥已立即失效");
        return result;
    }

    public List<Map<String, Object>> tenants() {
        return jdbc.queryForList("select id,name,owner_id,status,deploy_endpoint,cpu_cores,memory_gb,gpu_count,storage_gb,created_by,created_at,updated_at from ds_tenant order by created_at desc");
    }

    @Transactional
    public Map<String, Object> openTenant(Map<String, Object> request) {
        String id = "ten-" + shortId();
        String secret = Base64.getUrlEncoder().withoutPadding().encodeToString(randomBytes());
        String created = now();
        jdbc.update("insert into ds_tenant(id,name,owner_id,status,cpu_cores,memory_gb,gpu_count,storage_gb,signing_secret,created_by,created_at,updated_at) values(?,?,?,'CREATED',?,?,0,?,?,?,?,?)",
                id, required(request, "name"), value(request, "ownerId", currentOwner()),
                Math.max(1, nonNegativeInt(request, "cpuCores", 4)), Math.max(1, nonNegativeInt(request, "memoryGb", 16)),
                Math.max(1, nonNegativeInt(request, "storageGb", 100)), sha256(secret.getBytes(StandardCharsets.UTF_8)), actor(), created, created);
        audit("AUDIT", "TENANT_OPEN", "TENANT", id, "", true);
        Map<String, Object> result = new LinkedHashMap<>(requireRow("select id,name,owner_id,status,deploy_endpoint,cpu_cores,memory_gb,gpu_count,storage_gb,created_by,created_at,updated_at from ds_tenant where id=?", id));
        result.put("signingSecret", secret);
        result.put("notice", "签名密钥只显示一次，请立即保存");
        return result;
    }

    @Transactional
    public Map<String, Object> resizeTenant(Map<String, Object> request) {
        String id = required(request, "tenantId");
        Map<String, Object> old = requireRow("select * from ds_tenant where id=?", id);
        jdbc.update("update ds_tenant set cpu_cores=?,memory_gb=?,storage_gb=?,updated_at=? where id=?",
                Math.max(1, nonNegativeInt(request, "cpuCores", (int) number(old.get("cpu_cores"), 4))),
                Math.max(1, nonNegativeInt(request, "memoryGb", (int) number(old.get("memory_gb"), 16))),
                Math.max(1, nonNegativeInt(request, "storageGb", (int) number(old.get("storage_gb"), 100))), now(), id);
        audit("AUDIT", "TENANT_RESIZE", "TENANT", id, json(request), true);
        return requireRow("select id,name,owner_id,status,deploy_endpoint,cpu_cores,memory_gb,gpu_count,storage_gb,created_by,created_at,updated_at from ds_tenant where id=?", id);
    }

    @Transactional
    public Map<String, Object> deployTenant(String id) {
        requireRow("select id from ds_tenant where id=?", id);
        jdbc.update("update ds_tenant set status='ACTIVE',updated_at=? where id=?", now(), id);
        audit("OPERATION", "TENANT_DEPLOY", "TENANT", id, "MVP tenant activated", true);
        return requireRow("select id,name,owner_id,status,deploy_endpoint,cpu_cores,memory_gb,gpu_count,storage_gb,created_by,created_at,updated_at from ds_tenant where id=?", id);
    }

    public List<Map<String, Object>> billingUsage(String tenantId) {
        return jdbc.queryForList("select * from ds_usage_meter where tenant_id=? order by created_at desc", tenantId);
    }

    @Transactional
    public Map<String, Object> calculateBilling(Map<String, Object> request) {
        String tenantId = required(request, "tenantId");
        Map<String, Object> tenant = requireRow("select * from ds_tenant where id=?", tenantId);
        double cpu = number(tenant.get("cpu_cores"), 0) * 0.05;
        double memory = number(tenant.get("memory_gb"), 0) * 0.01;
        double storage = number(tenant.get("storage_gb"), 0) * 0.001;
        double amount = Math.round((cpu + memory + storage) * 100.0) / 100.0;
        String id = "bill-" + shortId();
        String created = now();
        jdbc.update("insert into ds_billing_record(id,tenant_id,period_start,period_end,amount,currency,status,created_at) values(?,?,?,?,?,?,'CALCULATED',?)",
                id, tenantId, value(request, "periodStart", created), value(request, "periodEnd", created), amount, value(request, "currency", "CNY"), created);
        audit("AUDIT", "BILLING_CALCULATE", "TENANT", tenantId, "amount=" + amount, true);
        return requireRow("select * from ds_billing_record where id=?", id);
    }

    public List<Map<String, Object>> trustedExchanges(String tenantId) {
        if (notBlank(tenantId)) return jdbc.queryForList("select * from ds_trusted_exchange where tenant_id=? order by created_at desc limit 200", tenantId);
        return jdbc.queryForList("select * from ds_trusted_exchange order by created_at desc limit 200");
    }

    @Transactional
    public Map<String, Object> trustedPush(Map<String, Object> request) {
        String tenantId = required(request, "tenantId");
        String key = required(request, "idempotencyKey");
        String secret = required(request, "signingSecret");
        Map<String, Object> tenant = requireRow("select * from ds_tenant where id=?", tenantId);
        if (!MessageDigest.isEqual(string(tenant.get("signing_secret")).getBytes(StandardCharsets.UTF_8),
                sha256(secret.getBytes(StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8))) {
            throw new SecurityException("租户签名密钥无效");
        }
        List<Map<String, Object>> existing = jdbc.queryForList("select * from ds_trusted_exchange where tenant_id=? and idempotency_key=?", tenantId, key);
        if (!existing.isEmpty()) return existing.get(0);
        String event = required(request, "eventType");
        String payload = value(request, "payload", "{}");
        String signature = hmac(secret, tenantId + "\n" + event + "\n" + key + "\n" + payload);
        String id = "tx-" + shortId();
        String created = now();
        jdbc.update("insert into ds_trusted_exchange(id,tenant_id,direction,event_type,idempotency_key,payload,payload_hash,signature,status,attempts,last_error,created_at,updated_at) values(?,?,'OUTBOUND',?,?,?,?,?,'SUCCESS',1,'',?,?)",
                id, tenantId, event, key, payload, sha256(payload.getBytes(StandardCharsets.UTF_8)), signature, created, created);
        audit("AUDIT", "TRUSTED_PUSH", "TRUSTED_EXCHANGE", id, "tenant=" + tenantId, true);
        return requireRow("select * from ds_trusted_exchange where id=?", id);
    }

    @Transactional
    public Map<String, Object> saveOidcMapping(Map<String, Object> request) {
        String id = value(request, "id", "oidcm-" + shortId());
        String updated = now();
        int changed = jdbc.update("update ds_oidc_role_mapping set claim_name=?,claim_value=?,platform_role=?,owner_id=?,enabled=?,updated_at=? where id=?",
                required(request, "claimName"), required(request, "claimValue"), required(request, "platformRole"),
                value(request, "ownerId", ""), bool(request, "enabled", true) ? 1 : 0, updated, id);
        if (changed == 0) jdbc.update("insert into ds_oidc_role_mapping(id,claim_name,claim_value,platform_role,owner_id,enabled,created_by,created_at,updated_at) values(?,?,?,?,?,?,?,?,?)",
                id, required(request, "claimName"), required(request, "claimValue"), required(request, "platformRole"), value(request, "ownerId", ""), bool(request, "enabled", true) ? 1 : 0, actor(), updated, updated);
        return requireRow("select * from ds_oidc_role_mapping where id=?", id);
    }

    public void deleteOidcMapping(String id) {
        jdbc.update("delete from ds_oidc_role_mapping where id=?", id);
    }

    private String hmac(String secret, String content) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return java.util.HexFormat.of().formatHex(mac.doFinal(content.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException("生成签名失败", e);
        }
    }

    @Transactional
    public Map<String, Object> saveWebhook(Map<String, Object> request) {
        String url = required(request, "url");
        validateHttpUrl(url);
        String id = value(request, "id", "wh-" + shortId());
        String now = now();
        int changed = jdbc.update("update ds_webhook set name=?,url=?,events=?,secret=?,enabled=?,updated_at=? where id=?",
                required(request, "name"), url, value(request, "events", "sandbox.created"), value(request, "secret", ""), bool(request, "enabled", true) ? 1 : 0, now, id);
        if (changed == 0) {
            jdbc.update("insert into ds_webhook(id,name,url,events,secret,enabled,created_by,created_at,updated_at) values(?,?,?,?,?,?,?,?,?)",
                    id, required(request, "name"), url, value(request, "events", "sandbox.created"), value(request, "secret", ""), bool(request, "enabled", true) ? 1 : 0, actor(), now, now);
        }
        audit("AUDIT", "WEBHOOK_SAVE", "WEBHOOK", id, url, true);
        return requireRow("select id,name,url,events,enabled,created_by,created_at,updated_at from ds_webhook where id=?", id);
    }

    public Map<String, Object> testWebhook(String id) {
        return deliverWebhook(id, "system.test", json(Map.of("message", "Data Sandbox webhook test", "time", now())));
    }

    public Map<String, Object> retryDelivery(String deliveryId) {
        Map<String, Object> delivery = requireRow("select * from ds_webhook_delivery where id=?", deliveryId);
        return deliverWebhook(string(delivery.get("webhook_id")), string(delivery.get("event_type")), string(delivery.get("payload")));
    }

    @Transactional
    public Map<String, Object> saveOidc(Map<String, Object> request) {
        Map<String, Object> current = requireRow("select * from ds_oidc_config where id=1");
        String secret = value(request, "clientSecret", string(current.get("client_secret")));
        jdbc.update("update ds_oidc_config set issuer=?,client_id=?,client_secret=?,scopes=?,enabled=?,discovery_status='UNTESTED',discovery_message='',updated_by=?,updated_at=? where id=1",
                value(request, "issuer", ""), value(request, "clientId", ""), secret,
                value(request, "scopes", "openid profile email"), bool(request, "enabled", false) ? 1 : 0, actor(), now());
        audit("AUDIT", "OIDC_CONFIG_UPDATE", "OIDC", "1", "issuer=" + value(request, "issuer", ""), true);
        return integrationOverview().get("oidc") instanceof Map<?, ?> map ? castMap(map) : Map.of();
    }

    @Transactional
    public Map<String, Object> testOidc() {
        Map<String, Object> config = requireRow("select * from ds_oidc_config where id=1");
        String issuer = string(config.get("issuer"));
        validateHttpUrl(issuer);
        String discovery = issuer.replaceAll("/$", "") + "/.well-known/openid-configuration";
        String status = "FAILED";
        String message;
        try {
            HttpResponse<String> response = httpClient.send(HttpRequest.newBuilder(URI.create(discovery)).timeout(Duration.ofSeconds(8)).GET().build(), HttpResponse.BodyHandlers.ofString());
            status = response.statusCode() >= 200 && response.statusCode() < 300 && response.body().contains("authorization_endpoint") ? "SUCCESS" : "FAILED";
            message = "HTTP " + response.statusCode() + ": " + truncate(response.body(), 1200);
        } catch (Exception e) {
            message = e.getClass().getSimpleName() + ": " + e.getMessage();
        }
        jdbc.update("update ds_oidc_config set discovery_status=?,discovery_message=?,updated_at=? where id=1", status, message, now());
        audit("SYSTEM", "OIDC_DISCOVERY_TEST", "OIDC", "1", message, "SUCCESS".equals(status));
        return Map.of("status", status, "message", message, "discoveryUrl", discovery);
    }

    /* ------------------------------- Operations ------------------------------- */

    public Map<String, Object> operationOverview() {
        Map<String, Object> counts = new LinkedHashMap<>();
        counts.put("sandboxes", count("select count(1) from ds_sandbox where deleted=0"));
        counts.put("runningSandboxes", count("select count(1) from ds_sandbox where deleted=0 and status='RUNNING'"));
        counts.put("pendingApprovals", count("select count(1) from ds_model_approval where status in ('MODEL_REVIEW','RESOURCE_REVIEW')"));
        // Z-03：沙箱资源申请单待处理数（含待审/已批准待执行/执行中）
        counts.put("pendingSandboxApprovals", count("select count(1) from ds_sandbox_approval where deleted=0 and status in ('DATA_PROVIDER_REVIEW','OPERATOR_REVIEW','APPROVED','EXECUTING')"));
        counts.put("openAlerts", count("select count(1) from ds_alert_event where status='OPEN'"));
        counts.put("failedCallbacks", count("select count(1) from ds_webhook_delivery where status='FAILED'"));
        return Map.of("status", "UP", "counts", counts, "kusciaIntegrationEnabled", kusciaEnabled,
                "snapshotRoot", snapshotRoot, "backupRoot", backupRoot,
                "backups", jdbc.queryForList("select * from ds_backup order by created_at desc limit 50"),
                "tickets", jdbc.queryForList("select * from ds_support_ticket order by updated_at desc limit 50"));
    }

    @Transactional
    public Map<String, Object> createBackup() {
        String id = "bak-" + shortId();
        Path dir = Path.of(backupRoot, id).normalize();
        Path target = dir.resolve("secretpad.sqlite");
        String created = now();
        jdbc.update("insert into ds_backup(id,backup_type,status,artifact_path,created_by,created_at) values(?,'FULL','RUNNING',?,?,?)", id, target.toString(), actor(), created);
        try {
            Files.createDirectories(dir);
            Path source = databasePath();
            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
            String checksum = sha256(Files.readAllBytes(target));
            Files.writeString(dir.resolve("metadata.json"), json(Map.of("backupId", id, "createdAt", created, "source", source.toString(), "sha256", checksum)), StandardCharsets.UTF_8);
            jdbc.update("update ds_backup set status='COMPLETED',size_bytes=?,checksum=?,completed_at=? where id=?", Files.size(target), checksum, now(), id);
            audit("OPERATION", "BACKUP_CREATE", "BACKUP", id, target.toString(), true);
        } catch (Exception e) {
            jdbc.update("update ds_backup set status='FAILED',message=?,completed_at=? where id=?", truncate(e.getMessage(), 900), now(), id);
            audit("SYSTEM", "BACKUP_CREATE", "BACKUP", id, e.getMessage(), false);
        }
        return requireRow("select * from ds_backup where id=?", id);
    }

    @Transactional
    public Map<String, Object> stageRestore(String backupId) {
        Map<String, Object> backup = requireRow("select * from ds_backup where id=? and status='COMPLETED'", backupId);
        Path source = Path.of(string(backup.get("artifact_path"))).normalize();
        Path pending = databasePath().resolveSibling("restore-pending.sqlite");
        try {
            String actual = sha256(Files.readAllBytes(source));
            if (!Objects.equals(actual, string(backup.get("checksum")))) throw new IllegalStateException("备份校验和不一致");
            Files.copy(source, pending, StandardCopyOption.REPLACE_EXISTING);
            jdbc.update("update ds_backup set status='RESTORE_STAGED',message=? where id=?", "已暂存，重启时由 data-sandbox-package 完成切换", backupId);
            audit("AUDIT", "RESTORE_STAGE", "BACKUP", backupId, pending.toString(), true);
            return Map.of("status", "RESTORE_STAGED", "pendingFile", pending.toString(), "message", "恢复文件已校验并暂存，请使用部署包 restore 命令安全重启切换");
        } catch (IOException e) {
            throw new IllegalStateException("暂存恢复失败: " + e.getMessage(), e);
        }
    }

    public Map<String, Object> diagnostics() {
        List<Map<String, Object>> checks = new ArrayList<>();
        checks.add(check("DATABASE", true, "SQLite query OK, tables=" + count("select count(1) from sqlite_master where type='table'")));
        checks.add(pathCheck("SNAPSHOT_STORAGE", Path.of(snapshotRoot)));
        checks.add(pathCheck("BACKUP_STORAGE", Path.of(backupRoot)));
        if (kusciaEnabled) {
            try {
                var response = kuscia.healthZ(Health.HealthRequest.newBuilder().build());
                checks.add(check("KUSCIA", response.getStatus().getCode() == 0, response.getStatus().getMessage()));
            } catch (Exception e) {
                checks.add(check("KUSCIA", false, e.getMessage()));
            }
        } else {
            checks.add(Map.of("name", "KUSCIA", "status", "SKIPPED", "message", "secretpad.data-sandbox.kuscia.enabled=false"));
        }
        boolean ok = checks.stream().noneMatch(it -> "FAILED".equals(it.get("status")));
        audit("SYSTEM", "DIAGNOSTICS_RUN", "SYSTEM", nodeId, "checks=" + checks.size(), ok);
        return Map.of("status", ok ? "HEALTHY" : "DEGRADED", "time", now(), "checks", checks);
    }

    /**
     * 限制生效校验（Z-02）：secretpad 无 kubectl，这里返回期望值 + 由运维脚本
     * （data-sandbox-package/scripts/deploy/data-sandbox/verify-limits.sh）执行的核对指引，
     * 不伪造结果。
     */
    public Map<String, Object> limitVerify(String sandboxId) {
        Map<String, Object> sandbox = sandbox(sandboxId);
        double cpu = number(sandbox.get("cpu_cores"), 0);
        double memoryGb = number(sandbox.get("memory_gb"), 0);
        String jobId = string(sandbox.get("kuscia_job_id"));
        return Map.of(
                "sandboxId", sandboxId,
                "expected", Map.of("cpu", cpu, "memory_gb", memoryGb, "gpu", number(sandbox.get("gpu_count"), 0), "storage_gb", number(sandbox.get("storage_gb"), 0)),
                "jobId", jobId,
                "runtimeMeta", parseRuntimeMeta(sandbox),
                "instructions", "secretpad 无 kubectl：请在 Kuscia 侧核对 pod 资源限制（cpu/memory）与 cgroup 值。"
                        + "运行 verify-limits.sh <kuscia容器> <sandboxId> 输出真实限制并交叉核对。");
    }

    private Map<String, Object> parseRuntimeMeta(Map<String, Object> sandbox) {
        try {
            Object parsed = objectMapper.readValue(string(sandbox.get("runtime_meta")), Object.class);
            return parsed instanceof Map<?, ?> map ? castMap(map) : new LinkedHashMap<>();
        } catch (Exception e) {
            return new LinkedHashMap<>();
        }
    }

    public List<Map<String, Object>> helpArticles() {
        return List.of(
                Map.of("id", "quick-start", "title", "沙箱快速入门", "content", "选择环境镜像和资源配额创建沙箱；启动后可在状态列查看运行情况，到期前可续期。"),
                Map.of("id", "network", "title", "网络策略说明", "content", "INTERNAL_ONLY 仅允许平台内访问；ALLOW_LIST 使用管理员配置的白名单；NO_NETWORK 禁止主动访问外部网络。"),
                Map.of("id", "approval", "title", "模型审批流程", "content", "模型审核 → 资源审核 → 已批准 → 发布。被驳回后修改版本并提交复审。"),
                Map.of("id", "recovery", "title", "备份恢复", "content", "创建备份后先执行恢复暂存，再使用 data-sandbox-package/ops.sh restore 完成停机切换。"));
    }

    @Transactional
    public Map<String, Object> createTicket(Map<String, Object> request) {
        String id = "ticket-" + shortId();
        String now = now();
        jdbc.update("insert into ds_support_ticket(id,title,category,priority,description,status,submitter,created_at,updated_at) values(?,?,?,?,?,'OPEN',?,?,?)",
                id, required(request, "title"), value(request, "category", "TECHNICAL"), value(request, "priority", "NORMAL"),
                required(request, "description"), actor(), now, now);
        audit("OPERATION", "SUPPORT_TICKET_CREATE", "SUPPORT_TICKET", id, "", true);
        return requireRow("select * from ds_support_ticket where id=?", id);
    }

    @Transactional
    public Map<String, Object> updateTicket(Map<String, Object> request) {
        String id = required(request, "id");
        jdbc.update("update ds_support_ticket set status=?,assignee=?,resolution=?,updated_at=? where id=?",
                value(request, "status", "PROCESSING"), value(request, "assignee", actor()), value(request, "resolution", ""), now(), id);
        audit("OPERATION", "SUPPORT_TICKET_UPDATE", "SUPPORT_TICKET", id, json(request), true);
        return requireRow("select * from ds_support_ticket where id=?", id);
    }

    /* ------------------------------- Scheduled jobs ------------------------------- */

    @Scheduled(fixedDelayString = "${secretpad.data-sandbox.status-sync-ms:30000}")
    public void syncKusciaStatuses() {
        if (!kusciaEnabled) return;
        // 只同步带真实 Kuscia Job 的沙箱；结合本地状态与意图做受保护映射，禁止无条件覆盖本地状态
        for (Map<String, Object> item : jdbc.queryForList(
                "select s.id,kuscia_job_id,s.status status,intent,i.dev_port_name,s.owner_id,s.gpu_count from ds_sandbox s left join ds_sandbox_image i on i.id=s.image_id where s.deleted=0 and s.kuscia_job_id<>''")) {
            try {
                Job.QueryJobResponse response = kuscia.queryJob(Job.QueryJobRequest.newBuilder().setJobId(string(item.get("kuscia_job_id"))).build());
                if (response.getStatus().getCode() != 0) {
                    // Job 已不存在（如被外部删除）：记录原始状态但不改写本地状态，交由用户操作处理
                    jdbc.update("update ds_sandbox set kuscia_job_state=?,updated_at=? where id=?", "DELETED", now(), item.get("id"));
                    log.warn("Kuscia Job {} for sandbox {} no longer exists: {}", item.get("kuscia_job_id"), item.get("id"), response.getStatus().getMessage());
                    continue;
                }
                String state = effectiveKusciaState(response);
                SandboxStatusMachine.Decision decision = SandboxStatusMachine.mapKusciaState(state,
                        string(item.get("status")), string(item.get("intent")));
                String target = decision.targetStatus();
                if (target == null) {
                    // 无状态变化也刷新 Kuscia 原始状态与时间，便于排障
                    jdbc.update("update ds_sandbox set kuscia_job_state=?,updated_at=? where id=?", state, now(), item.get("id"));
                    continue;
                }
                List<Object> args = new ArrayList<>();
                StringBuilder sql = new StringBuilder("update ds_sandbox set status=?,intent=?,last_error=?,kuscia_job_state=?,updated_at=?");
                args.add(target);
                args.add(decision.clearIntent() ? "" : string(item.get("intent")));
                args.add(decision.lastError() == null ? "" : decision.lastError());
                args.add(state);
                args.add(now());
                String endpoint = "";
                if (target.equals("RUNNING")) {
                    endpoint = extractEndpoint(response, string(item.get("dev_port_name")));
                    if (!endpoint.isEmpty()) {
                        sql.append(",endpoint=?,endpoint_updated_at=?");
                        args.add(endpoint);
                        args.add(now());
                    }
                } else {
                    sql.append(",endpoint='',endpoint_updated_at=?");
                    args.add(now());
                }
                sql.append(" where id=?");
                args.add(item.get("id"));
                jdbc.update(sql.toString(), args.toArray());
                // Z-02：绑定/释放资源分配，保持与真实运行状态一致
                if (target.equals("RUNNING")) {
                    bindAllocations(item, endpoint);
                } else if (target.equals("STOPPED")) {
                    releaseAllocations(item, "MANUAL");
                } else if (target.equals("ERROR")) {
                    raiseSandboxErrorAlert(string(item.get("id")), "Kuscia 状态异常：" + state);
                }
            } catch (Exception e) {
                log.warn("Failed to synchronize sandbox {} with Kuscia: {}", item.get("id"), e.getMessage());
            }
        }
    }

    /**
     * Kuscia marks a Job RUNNING once it has dispatched its tasks. A task may still be
     * Pending while its image is pulled, so the Job state alone is not proof that the
     * sandbox endpoint is ready. Use the least-ready task/party state instead.
     */
    private String effectiveKusciaState(Job.QueryJobResponse response) {
        String topLevel = response.getData().getStatus().getState().toUpperCase(Locale.ROOT);
        List<String> states = new ArrayList<>();
        for (Job.TaskStatus task : response.getData().getStatus().getTasksList()) {
            if (!task.getState().isBlank()) {
                states.add(task.getState().toUpperCase(Locale.ROOT));
            }
            for (Job.PartyStatus party : task.getPartiesList()) {
                if (!party.getState().isBlank()) {
                    states.add(party.getState().toUpperCase(Locale.ROOT));
                }
            }
        }
        if (states.isEmpty()) {
            return topLevel;
        }
        for (String state : states) {
            if (state.contains("FAIL") || state.equals("REJECTED")) {
                return state;
            }
        }
        for (String state : states) {
            if (state.equals("PENDING") || state.equals("AWAITINGAPPROVAL")) {
                return state;
            }
        }
        for (String state : states) {
            if (state.equals("RUNNING")) {
                return state;
            }
        }
        return topLevel;
    }

    /**
     * 从 Kuscia Job 状态中提取开发端点：取镜像端口名匹配且 scope=Cluster 的 endpoint
     * （scope=Cluster 的端口由 Kuscia 分配集群外可达地址，可用于开发环境访问）。
     */
    private String extractEndpoint(Job.QueryJobResponse response, String portName) {
        if (portName == null || portName.isBlank()) {
            portName = "web";
        }
        for (Job.TaskStatus task : response.getData().getStatus().getTasksList()) {
            for (Job.PartyStatus party : task.getPartiesList()) {
                for (Job.JobPartyEndpoint endpoint : party.getEndpointsList()) {
                    if (portName.equals(endpoint.getPortName()) && "Cluster".equalsIgnoreCase(endpoint.getScope())) {
                        return endpoint.getEndpoint();
                    }
                }
            }
        }
        return "";
    }

    @Scheduled(cron = "0 * * * * *")
    @Transactional
    public void expireSandboxesAndCheckAlerts() {
        List<Map<String, Object>> expired = jdbc.queryForList("select * from ds_sandbox where deleted=0 and status not in ('EXPIRED','DESTROYED') and expires_at<?", now());
        for (Map<String, Object> sandbox : expired) {
            // 到期先真实停止 Kuscia Job，成功才置 EXPIRED；停止失败置 ERROR 并记录原因，不假 EXPIRED
            String error = stopKuscia(sandbox, "Sandbox expired");
            if (error.isEmpty()) {
                jdbc.update("update ds_sandbox set status='EXPIRED',intent='',last_error='',updated_at=? where id=?", now(), sandbox.get("id"));
                releaseAllocations(sandbox, "EXPIRE");
                auditAs("SYSTEM", "WARN", "system", "SANDBOX_EXPIRED", "SANDBOX", string(sandbox.get("id")), "", true);
            } else {
                jdbc.update("update ds_sandbox set status='ERROR',intent='',last_error=?,updated_at=? where id=?", truncate(error, 900), now(), sandbox.get("id"));
                auditAs("SYSTEM", "WARN", "system", "SANDBOX_EXPIRE_FAILED", "SANDBOX", string(sandbox.get("id")), error, false);
                raiseSandboxErrorAlert(string(sandbox.get("id")), "到期停止失败：" + error);
            }
        }
        // Z-02 规则1/规则2：资源池使用率 + 单用户配额使用率告警（source=RESOURCE）
        Map<String, Double> used = usage(null);
        for (Map<String, Object> pool : jdbc.queryForList("select * from ds_resource_pool where enabled=1")) {
            String type = string(pool.get("resource_type"));
            double total = number(pool.get("total_amount"), 0);
            double percent = total == 0 ? 0 : used.getOrDefault(type, 0d) * 100 / total;
            double threshold = number(pool.get("warning_threshold"), 80);
            if (percent >= threshold) {
                raiseAlert("WARNING", "RESOURCE", type + " 使用率告警",
                        String.format(Locale.ROOT, "当前使用率 %.2f%%，阈值 %.2f%%", percent, threshold),
                        "resource:" + type + ":usage");
            }
        }
        checkQuotaUsageAlerts();
    }

    /** Z-02 规则2：单用户配额使用率 ≥ quota-warning-percent 时告警（source=RESOURCE，按用户去重）。 */
    private void checkQuotaUsageAlerts() {
        List<String> resourceTypes = List.of("CPU", "MEMORY", "GPU", "STORAGE");
        for (Map<String, Object> quotaRow : jdbc.queryForList("select owner_id,cpu_cores,memory_gb,gpu_count,storage_gb from ds_resource_quota")) {
            String owner = string(quotaRow.get("owner_id"));
            if (!notBlank(owner)) continue;
            Map<String, Double> ownerUsed = usage(owner);
            List<Object[]> cols = List.of(
                    new Object[]{"CPU", quotaRow.get("cpu_cores")},
                    new Object[]{"MEMORY", quotaRow.get("memory_gb")},
                    new Object[]{"GPU", quotaRow.get("gpu_count")},
                    new Object[]{"STORAGE", quotaRow.get("storage_gb")});
            for (int i = 0; i < resourceTypes.size(); i++) {
                String rt = resourceTypes.get(i);
                double cap = number(cols.get(i)[1], 0);
                if (cap <= 0) continue;
                double pct = ownerUsed.getOrDefault(rt, 0d) * 100 / cap;
                if (pct >= quotaWarningPercent) {
                    raiseAlert("WARNING", "RESOURCE", owner + " 配额使用率告警",
                            String.format(Locale.ROOT, "用户 %s 的 %s 配额使用率 %.2f%%（阈值 %d%%）", owner, rt, pct, quotaWarningPercent),
                            "quota:" + owner + ":" + rt);
                }
            }
        }
    }

    /** Z-02 规则1：真实节点指标（ds_node_metric）达到资源池 warning/critical 阈值时告警。 */
    @Scheduled(fixedDelayString = "${secretpad.data-sandbox.metrics.interval-ms:30000}")
    @Transactional
    public void checkNodeMetricsAlerts() {
        Map<String, Object> metric = latestNodeMetric();
        if (metric.isEmpty() || !"FRESH".equals(string(metric.get("status")))) return;
        Map<String, Double> nodePct = Map.of(
                "CPU", number(metric.get("cpu_usage_percent"), 0),
                "MEMORY", number(metric.get("memory_usage_percent"), 0),
                "STORAGE", number(metric.get("storage_usage_percent"), 0));
        for (Map<String, Object> pool : jdbc.queryForList("select * from ds_resource_pool where enabled=1")) {
            String type = string(pool.get("resource_type"));
            Double pct = nodePct.get(type);
            if (pct == null) continue;
            double critical = number(pool.get("critical_threshold"), 90);
            double warning = number(pool.get("warning_threshold"), 80);
            if (pct >= critical) {
                raiseAlert("CRITICAL", "NODE_METRIC", type + " 节点使用率危险",
                        String.format(Locale.ROOT, "节点 %s 使用率 %.2f%%（危险阈值 %.2f%%）", type, pct, critical),
                        "node:" + type + ":critical");
            } else if (pct >= warning) {
                raiseAlert("WARNING", "NODE_METRIC", type + " 节点使用率告警",
                        String.format(Locale.ROOT, "节点 %s 使用率 %.2f%%（告警阈值 %.2f%%）", type, pct, warning),
                        "node:" + type + ":warning");
            }
        }
    }

    @Scheduled(cron = "0 20 2 * * *")
    @Transactional
    public void purgeExpiredLogs() {
        for (Map<String, Object> policy : retentionPolicies()) {
            int days = ((Number) policy.get("retention_days")).intValue();
            jdbc.update("delete from ds_unified_log where log_type=? and created_at<?", policy.get("log_type"), LocalDateTime.now().minusDays(days).toString());
        }
    }

    /* ------------------------------- Internal helpers ------------------------------- */

    /** 启动 Kuscia Job（幂等：有 job 则 restart，否则 create）。供 Z-03 审批执行引擎复用。 */
    public String startKuscia(Map<String, Object> sandbox) {
        // Stage 3：START 即重建沙箱权威库，保证 sandbox_data.db 最新（挂载变更亦触发；失败不阻断启动，下次重试）
        try {
            sandboxDbService.rebuild(string(sandbox.get("id")));
        } catch (Exception e) {
            log.warn("沙箱 {} 权威库 START 重建失败（启动继续，下次重建重试）: {}", sandbox.get("id"), e.getMessage());
        }
        if (!kusciaEnabled) {
            // 运行时未启用时禁止“假 RUNNING”：返回明确错误，由调用方将状态置为 ERROR
            return "Kuscia 运行时未启用（secretpad.data-sandbox.kuscia.enabled=false），请启用后重试";
        }
        try {
            String existing = string(sandbox.get("kuscia_job_id"));
            if (notBlank(existing)) {
                var response = kuscia.restartJob(Job.RestartJobRequest.newBuilder().setJobId(existing).setReason("Started from Data Sandbox console").build());
                return response.getStatus().getCode() == 0 ? "" : response.getStatus().getMessage();
            }
            String appImage = string(sandbox.get("kuscia_app_image"));
            if (!notBlank(appImage)) return "环境镜像未配置 Kuscia AppImage 名称";
            // Z-02 网络隔离：NO_NETWORK 使用 -nonet 变体（无 scope=Cluster 端口，Kuscia 不分配集群外端点）
            String networkPolicy = string(sandbox.get("network_policy"));
            if ("NO_NETWORK".equals(networkPolicy) && !appImage.endsWith("-nonet")) {
                appImage = appImage + "-nonet";
            }
            String jobId = ("ds-" + string(sandbox.get("id"))).replace("_", "-").toLowerCase(Locale.ROOT);
            Job.JobResource.Builder resources = Job.JobResource.newBuilder()
                    .setCpu(String.valueOf(sandbox.get("cpu_cores")))
                    .setMemory(sandbox.get("memory_gb") + "Gi")
                    .setEphemeralStorage(sandbox.get("storage_gb") + "Gi");
            if (number(sandbox.get("gpu_count"), 0) > 0) {
                resources.setGpu(String.valueOf(sandbox.get("gpu_count")));
            }
            Job.Party party = Job.Party.newBuilder().setDomainId(nodeId).setRole("server")
                    .setResources(resources)
                    .build();
            Job.Task task = Job.Task.newBuilder().setTaskId(jobId + "-task").setAlias("data-sandbox")
                    .setAppImage(appImage).addParties(party).setTaskInputConfig("{}").build();
            Job.CreateJobResponse response = kuscia.createJob(Job.CreateJobRequest.newBuilder().setJobId(jobId).setInitiator(nodeId).setMaxParallelism(1).addTasks(task)
                    .putCustomFields("data_sandbox_id", string(sandbox.get("id"))).putCustomFields("network_policy", string(sandbox.get("network_policy"))).build());
            if (response.getStatus().getCode() == 0) {
                jdbc.update("update ds_sandbox set kuscia_job_id=? where id=?", jobId, sandbox.get("id"));
                // Z-02：记录真实下发的规格（kusciaapi 仅支持 cpu/memory 下发）
                appendRuntimeMeta(string(sandbox.get("id")), Map.of("job_id", jobId, "app_image", appImage,
                        "resources", Map.of("cpu", string(sandbox.get("cpu_cores")), "memory", string(sandbox.get("memory_gb")) + "Gi")));
                return "";
            }
            return response.getStatus().getMessage();
        } catch (Exception e) {
            return truncate(e.getMessage(), 900);
        }
    }

    /** 停止 Kuscia Job（幂等，job 为空返回 ""）。供 Z-03 审批执行引擎复用。 */
    public String stopKuscia(Map<String, Object> sandbox, String reason) {
        if (!kusciaEnabled || !notBlank(string(sandbox.get("kuscia_job_id")))) return "";
        try {
            var response = kuscia.stopJob(Job.StopJobRequest.newBuilder().setJobId(string(sandbox.get("kuscia_job_id"))).setReason(reason).build());
            return response.getStatus().getCode() == 0 ? "" : response.getStatus().getMessage();
        } catch (Exception e) {
            return truncate(e.getMessage(), 900);
        }
    }

    /** 删除 Kuscia Job（幂等，job 为空返回 ""）。供 Z-03 审批执行引擎复用。 */
    public String deleteKuscia(Map<String, Object> sandbox) {
        if (!kusciaEnabled || !notBlank(string(sandbox.get("kuscia_job_id")))) return "";
        try {
            var response = kuscia.deleteJob(Job.DeleteJobRequest.newBuilder().setJobId(string(sandbox.get("kuscia_job_id"))).build());
            return response.getStatus().getCode() == 0 ? "" : response.getStatus().getMessage();
        } catch (Exception e) {
            return truncate(e.getMessage(), 900);
        }
    }

    /**
     * Wait until Kuscia has finished deleting a job.
     *
     * <p>DeleteJob returning success only means that the API request was accepted. The
     * KusciaJob and its task resources are removed asynchronously. Creating another job with
     * the same deterministic ID before that cleanup finishes can resurrect stale task status
     * and leave the new task pending without a pod. Call this before recreating a job.</p>
     */
    public String waitForKusciaJobDeletion(String jobId, Duration timeout) {
        if (!kusciaEnabled || !notBlank(jobId)) return "";
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            try {
                var response = kuscia.queryJob(Job.QueryJobRequest.newBuilder().setJobId(jobId).build());
                if (response.getStatus().getCode() != 0) return "";
            } catch (Exception e) {
                return truncate(e.getMessage(), 900);
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return "等待 Kuscia Job 删除被中断: " + jobId;
            }
        }
        return "等待 Kuscia Job 删除超时: " + jobId;
    }

    private void createSnapshot(Map<String, Object> sandbox) {
        String sandboxId = string(sandbox.get("id"));
        String snapshotId = "snap-" + shortId();
        Path dir = Path.of(snapshotRoot, sandboxId, snapshotId).normalize();
        try {
            Files.createDirectories(dir);
            Map<String, Object> metadata = new LinkedHashMap<>(sandbox);
            metadata.put("snapshotId", snapshotId);
            metadata.put("createdAt", now());
            Path metadataFile = dir.resolve("metadata.json");
            Files.writeString(metadataFile, objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(metadata), StandardCharsets.UTF_8);
            String checksum = sha256(Files.readAllBytes(metadataFile));
            Files.writeString(dir.resolve("metadata.json.sha256"), checksum + "  metadata.json\n", StandardCharsets.UTF_8);
            jdbc.update("insert into ds_sandbox_snapshot(id,sandbox_id,status,artifact_path,size_bytes,checksum,created_by,created_at) values(?,?,'COMPLETED',?,?,?,?,?)",
                    snapshotId, sandboxId, dir.toString(), Files.size(metadataFile), checksum, actor(), now());
        } catch (IOException e) {
            jdbc.update("insert into ds_sandbox_snapshot(id,sandbox_id,status,artifact_path,created_by,created_at) values(?,?,'FAILED',?,?,?)",
                    snapshotId, sandboxId, dir.toString(), actor(), now());
            throw new IllegalStateException("创建快照失败: " + e.getMessage(), e);
        }
    }

    /** 容量与配额校验（提交到批准间容量可能变化，执行时重新校验）。供 Z-03 审批执行引擎复用。 */
    public void assertCapacity(String ownerId, double cpu, double memory, int gpu, double storage) {
        Map<String, Double> global = usage(null);
        Map<String, Double> owner = usage(ownerId);
        Map<String, Object> quota = requireRow("select * from ds_resource_quota where owner_id=?", ownerId);
        Map<String, Double> requested = Map.of("CPU", cpu, "MEMORY", memory, "GPU", (double) gpu, "STORAGE", storage);
        Map<String, String> quotaColumns = Map.of("CPU", "cpu_cores", "MEMORY", "memory_gb", "GPU", "gpu_count", "STORAGE", "storage_gb");
        for (Map<String, Object> pool : jdbc.queryForList("select * from ds_resource_pool where enabled=1")) {
            String type = string(pool.get("resource_type"));
            double amount = requested.getOrDefault(type, 0d);
            if (global.getOrDefault(type, 0d) + amount > number(pool.get("total_amount"), 0)) {
                throw new IllegalStateException(type + " 资源池余量不足");
            }
            if (owner.getOrDefault(type, 0d) + amount > number(quota.get(quotaColumns.get(type)), 0)) {
                throw new IllegalStateException(type + " 超出用户配额");
            }
        }
    }

    /** 资源用量统计（仅 RESERVED/BOUND）。供 Z-03 审批执行引擎复用。 */
    public Map<String, Double> usage(String ownerId) {
        // Z-02：资源用量改为生命周期感知——只统计 RESERVED/BOUND 的分配行，
        // RELEASED 不再计数（停止/过期/销毁后 quota 与资源池余量立即回落）
        String suffix = ownerId == null ? "" : " and owner_id=?";
        Object[] args = ownerId == null ? new Object[]{} : new Object[]{ownerId};
        Map<String, Object> row = jdbc.queryForMap("select "
                + "coalesce(sum(case when resource_type='CPU' then amount else 0 end),0) cpu,"
                + "coalesce(sum(case when resource_type='MEMORY' then amount else 0 end),0) memory,"
                + "coalesce(sum(case when resource_type='GPU' then amount else 0 end),0) gpu,"
                + "coalesce(sum(case when resource_type='STORAGE' then amount else 0 end),0) storage "
                + "from ds_resource_allocation where state in ('RESERVED','BOUND')" + suffix, args);
        return Map.of("CPU", number(row.get("cpu"), 0), "MEMORY", number(row.get("memory"), 0), "GPU", number(row.get("gpu"), 0), "STORAGE", number(row.get("storage"), 0));
    }

    /* ------------------------------- Resource lifecycle (Z-02) ------------------------------- */

    /**
     * 按沙箱规格幂等创建 RESERVED 分配行（先清理同沙箱已有 RESERVED/BOUND，避免重复占额），
     * 并把 sandbox.alloc_state 置为 RESERVED。零配额类型不生成行（GPU=0 不占 GPU 额度）。
     */
    /** 按沙箱规格幂等创建 RESERVED 分配行。供 Z-03 审批执行引擎复用。 */
    public void reserveAllocations(Map<String, Object> sandbox) {
        String sandboxId = string(sandbox.get("id"));
        jdbc.update("delete from ds_resource_allocation where sandbox_id=? and state in ('RESERVED','BOUND')", sandboxId);
        insertAllocation(sandboxId, "CPU", number(sandbox.get("cpu_cores"), 0), "RESERVED", sandbox);
        insertAllocation(sandboxId, "MEMORY", number(sandbox.get("memory_gb"), 0), "RESERVED", sandbox);
        int gpu = (int) number(sandbox.get("gpu_count"), 0);
        if (gpu > 0) {
            insertAllocation(sandboxId, "GPU", gpu, "RESERVED", sandbox);
        }
        insertAllocation(sandboxId, "STORAGE", number(sandbox.get("storage_gb"), 0), "RESERVED", sandbox);
        jdbc.update("update ds_sandbox set alloc_state='RESERVED',updated_at=? where id=?", now(), sandboxId);
    }

    private void insertAllocation(String sandboxId, String resourceType, double amount, String state, Map<String, Object> sandbox) {
        // 每次预占生成唯一 id：同一沙箱可经历多次 RESERVED→BOUND→RELEASED 周期，
        // 释放历史保留，重复预占不触发主键冲突
        jdbc.update("insert into ds_resource_allocation(id,sandbox_id,resource_type,amount,state,owner_id,sandbox_status,created_at) values(?,?,?,?,?,?,?,?)",
                "alloc-" + sandboxId.replace("-", "") + "-" + resourceType + "-" + shortId(), sandboxId, resourceType, amount, state,
                string(sandbox.get("owner_id")), string(sandbox.get("status")), now());
    }

    /** Job 确认 RUNNING 时把 RESERVED 分配绑定为 BOUND，并绑定 GPU 台账。 */
    private void bindAllocations(Map<String, Object> sandbox, String endpoint) {
        String sandboxId = string(sandbox.get("id"));
        jdbc.update("update ds_resource_allocation set state='BOUND',bound_at=?,sandbox_status='RUNNING',released_at='',released_by='' "
                + "where sandbox_id=? and state in ('RESERVED','BOUND')", now(), sandboxId);
        bindGpuLedger(sandbox);
        jdbc.update("update ds_sandbox set alloc_state='BOUND',updated_at=? where id=?", now(), sandboxId);
        if (notBlank(endpoint)) {
            appendRuntimeMeta(sandboxId, Map.of("endpoint", endpoint, "alloc_state", "BOUND"));
        }
    }

    /** 释放：RESERVED/BOUND → RELEASED，GPU 台账归还，sandbox.alloc_state 置 RELEASED。供 Z-03 审批执行引擎复用。 */
    public void releaseAllocations(Map<String, Object> sandbox, String by) {
        String sandboxId = string(sandbox.get("id"));
        jdbc.update("update ds_resource_allocation set state='RELEASED',released_at=?,released_by=? "
                + "where sandbox_id=? and state in ('RESERVED','BOUND')", now(), by, sandboxId);
        releaseGpuLedger(sandbox);
        jdbc.update("update ds_sandbox set alloc_state='RELEASED',updated_at=? where id=?", now(), sandboxId);
    }

    /**
     * GPU 台账绑定：按沙箱 gpu_count 把 AVAILABLE 的 GPU 登记到 owner（台账级，无容器直通）。
     * 同 owner 已分配的 GPU 保留，不足时从可用池补充。
     */
    private void bindGpuLedger(Map<String, Object> sandbox) {
        int gpu = (int) number(sandbox.get("gpu_count"), 0);
        if (gpu <= 0) {
            return;
        }
        String owner = string(sandbox.get("owner_id"));
        long allocated = count("select count(1) from ds_gpu_ledger where status='ALLOCATED' and owner_id=?", owner);
        if (allocated >= gpu) {
            return;
        }
        jdbc.update("update ds_gpu_ledger set status='ALLOCATED',owner_id=?,allocated_at=? where id in "
                + "(select id from ds_gpu_ledger where status='AVAILABLE' order by id limit ?)", owner, now(), gpu - (int) allocated);
    }

    /** GPU 台账归还：该 owner 所有 ALLOCATED 的 GPU 恢复 AVAILABLE（台账级粒度）。 */
    private void releaseGpuLedger(Map<String, Object> sandbox) {
        String owner = string(sandbox.get("owner_id"));
        if (notBlank(owner)) {
            jdbc.update("update ds_gpu_ledger set status='AVAILABLE',owner_id='',allocated_at='' where status='ALLOCATED' and owner_id=?", owner);
        }
    }

    /** 合并补丁到 runtime_meta（JSON），超长截断为 2048。供 Z-03 审批执行引擎复用。 */
    public void appendRuntimeMeta(String sandboxId, Map<String, Object> patch) {
        try {
            Map<String, Object> meta = new LinkedHashMap<>();
            String raw = string(jdbc.queryForObject("select runtime_meta from ds_sandbox where id=?", String.class, sandboxId));
            if (notBlank(raw)) {
                Object parsed = objectMapper.readValue(raw, Object.class);
                if (parsed instanceof Map<?, ?> existing) {
                    meta.putAll(castMap(existing));
                }
            }
            meta.putAll(patch);
            jdbc.update("update ds_sandbox set runtime_meta=?,updated_at=? where id=?",
                    truncate(json(meta), 2048), now(), sandboxId);
        } catch (Exception e) {
            log.warn("Unable to append runtime_meta for sandbox {}: {}", sandboxId, e.getMessage());
        }
    }

    /** 按剩余活动分配刷新 sandbox.alloc_state（BOUND>RESERVED>RELEASED）。 */
    private void refreshAllocState(String sandboxId) {
        String state = string(jdbc.queryForObject("select coalesce(max(case state when 'BOUND' then 2 when 'RESERVED' then 1 else 0 end),0) "
                + "from ds_resource_allocation where sandbox_id=?", Integer.class, sandboxId));
        jdbc.update("update ds_sandbox set alloc_state=? where id=?",
                "2".equals(state) ? "BOUND" : ("1".equals(state) ? "RESERVED" : "RELEASED"), sandboxId);
    }

    /**
     * 异常回收：每分钟兜底清理卡死的资源分配。
     * 1) DESTROYED/deleted 沙箱遗留的 RESERVED/BOUND → 强制释放（RECLAIM）；
     * 2) ERROR/STARTING 沙箱的 RESERVED/BOUND 分配超过 10 分钟仍未绑定 → RECLAIM + 告警 + 审计。
     */
    @Scheduled(cron = "0 * * * * *")
    @Transactional
    public void reclaimAbnormalAllocations() {
        String now = now();
        Set<String> affected = new java.util.HashSet<>();
        for (Map<String, Object> row : jdbc.queryForList(
                "select a.id,a.sandbox_id from ds_resource_allocation a where a.state in ('RESERVED','BOUND') "
                        + "and exists(select 1 from ds_sandbox s where s.id=a.sandbox_id and (s.deleted=1 or s.status='DESTROYED'))")) {
            jdbc.update("update ds_resource_allocation set state='RELEASED',released_at=?,released_by='RECLAIM' where id=?", now, row.get("id"));
            affected.add(string(row.get("sandbox_id")));
        }
        String threshold = LocalDateTime.now().minusMinutes(10).toString();
        List<Map<String, Object>> stuck = jdbc.queryForList(
                "select a.id,a.sandbox_id,a.resource_type from ds_resource_allocation a "
                        + "where a.state in ('RESERVED','BOUND') and a.created_at<? "
                        + "and exists(select 1 from ds_sandbox s where s.id=a.sandbox_id and s.deleted=0 and s.status in ('ERROR','STARTING'))",
                threshold);
        if (!stuck.isEmpty()) {
            String ids = String.join(",", stuck.stream().map(r -> "'" + string(r.get("id")) + "'").toList());
            jdbc.update("update ds_resource_allocation set state='RELEASED',released_at=?,released_by='RECLAIM' where id in (" + ids + ")", now);
            for (Map<String, Object> row : stuck) {
                affected.add(string(row.get("sandbox_id")));
                auditAs("SYSTEM", "WARN", "system", "RESOURCE_RECLAIM", "SANDBOX", string(row.get("sandbox_id")),
                        "异常回收 " + string(row.get("resource_type")) + " 分配（超时未绑定）", true);
            }
            raiseAlert("WARNING", "SANDBOX", "资源异常回收",
                    "异常回收 " + stuck.size() + " 条超时未绑定的资源分配", "reclaim:abnormal");
        }
        // Z-02 规则3：STARTING + intent=START 超过 5 分钟仍未 RUNNING → 资源绑定超时告警
        String bindThreshold = LocalDateTime.now().minusMinutes(5).toString();
        for (Map<String, Object> row : jdbc.queryForList(
                "select id,kuscia_job_id from ds_sandbox where deleted=0 and status='STARTING' and intent='START' and updated_at<?",
                bindThreshold)) {
            String sandboxId = string(row.get("id"));
            raiseAlert("WARNING", "SANDBOX", "资源绑定超时",
                    "沙箱 " + sandboxId + " STARTING 超 5 分钟仍未 RUNNING（job=" + string(row.get("kuscia_job_id")) + "）",
                    "sandbox:" + sandboxId + ":bind-timeout");
        }
        for (String sandboxId : affected) {
            refreshAllocState(sandboxId);
        }
    }

    /** 幂等补齐 owner 配额行。供 Z-03 审批执行引擎复用。 */
    public void ensureQuota(String ownerId) {
        jdbc.update("insert or ignore into ds_resource_quota(owner_id,updated_by,updated_at) values(?,?,?)", ownerId, "system", now());
    }

    private void approvalHistory(String id, String action, String from, String to, String comment) {
        if (!MODEL_STATES.contains(to)) throw new IllegalArgumentException("无效审批状态: " + to);
        jdbc.update("insert into ds_model_approval_history(approval_id,action,from_status,to_status,operator,comment,created_at) values(?,?,?,?,?,?,?)",
                id, action, from, to, actor(), comment, now());
    }

    /** 统一审计落库（显式 actor，供引擎身份使用）。供 Z-03 审批执行引擎复用。 */
    public void auditAs(String type, String level, String actor, String action, String resourceType, String resourceId, String detail, boolean success) {
        try {
            String ip = "";
            try { if (RequestUtils.getCurrentHttpRequest() != null) ip = RequestUtils.getRemoteHost(); } catch (Exception ignored) { }
            jdbc.update("insert into ds_unified_log(log_type,level,actor,action,resource_type,resource_id,detail,ip_address,trace_id,success,created_at) values(?,?,?,?,?,?,?,?,?,?,?)",
                    type, level, actor, action, resourceType, resourceId, truncate(detail, 2000), ip, value(MDC.getCopyOfContextMap(), "Trace-Id", ""), success ? 1 : 0, now());
        } catch (Exception e) {
            log.warn("Unable to persist unified audit log: {}", e.getMessage());
        }
    }

    /** 按事件派发 webhook（精确/通配匹配）。供 Z-03 审批执行引擎复用。 */
    public void dispatchWebhooks(String event, Map<String, Object> payload) {
        String body = json(Map.of("event", event, "time", now(), "data", payload));
        for (Map<String, Object> webhook : jdbc.queryForList("select id,events from ds_webhook where enabled=1")) {
            String events = string(webhook.get("events"));
            if (events.contains("*") || events.contains(event)) {
                try { deliverWebhook(string(webhook.get("id")), event, body); }
                catch (Exception e) { log.warn("Webhook {} failed: {}", webhook.get("id"), e.getMessage()); }
            }
        }
    }

    private Map<String, Object> deliverWebhook(String webhookId, String event, String payload) {
        Map<String, Object> webhook = requireRow("select * from ds_webhook where id=? and enabled=1", webhookId);
        String deliveryId = "delivery-" + shortId();
        String created = now();
        jdbc.update("insert into ds_webhook_delivery(id,webhook_id,event_type,payload,status,created_at,updated_at) values(?,?,?,?,'DELIVERING',?,?)",
                deliveryId, webhookId, event, payload, created, created);
        int code = 0;
        String responseBody = "";
        int attempts = 0;
        for (int i = 1; i <= 3; i++) {
            attempts = i;
            try {
                HttpRequest request = HttpRequest.newBuilder(URI.create(string(webhook.get("url")))).timeout(Duration.ofSeconds(8))
                        .header("Content-Type", "application/json").header("X-Data-Sandbox-Event", event)
                        .header("X-Data-Sandbox-Signature", sha256((string(webhook.get("secret")) + payload).getBytes(StandardCharsets.UTF_8)))
                        .POST(HttpRequest.BodyPublishers.ofString(payload)).build();
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                code = response.statusCode();
                responseBody = truncate(response.body(), 1800);
                if (code >= 200 && code < 300) break;
            } catch (Exception e) {
                responseBody = truncate(e.getMessage(), 1800);
            }
        }
        String status = code >= 200 && code < 300 ? "SUCCESS" : "FAILED";
        jdbc.update("update ds_webhook_delivery set status=?,attempts=?,response_code=?,response_body=?,next_retry_at=?,updated_at=? where id=?",
                status, attempts, code, responseBody, "FAILED".equals(status) ? LocalDateTime.now().plusMinutes(5).toString() : "", now(), deliveryId);
        return requireRow("select * from ds_webhook_delivery where id=?", deliveryId);
    }

    private Map<String, Object> pathCheck(String name, Path path) {
        try {
            Files.createDirectories(path);
            FileStore store = Files.getFileStore(path);
            return Map.of("name", name, "status", Files.isWritable(path) ? "PASSED" : "FAILED", "message", path + ", usable=" + store.getUsableSpace());
        } catch (Exception e) {
            return check(name, false, e.getMessage());
        }
    }

    private Map<String, Object> check(String name, boolean passed, String message) {
        return Map.of("name", name, "status", passed ? "PASSED" : "FAILED", "message", string(message));
    }

    private Path databasePath() {
        String prefix = "jdbc:sqlite:";
        if (!databaseUrl.startsWith(prefix)) throw new IllegalStateException("MVP 备份当前仅支持 SQLite");
        return Path.of(databaseUrl.substring(prefix.length())).toAbsolutePath().normalize();
    }

    private Map<String, Object> requireRow(String sql, Object... args) {
        try {
            return new LinkedHashMap<>(jdbc.queryForMap(sql, args));
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("记录不存在");
        }
    }

    private long count(String sql, Object... args) {
        Long value = jdbc.queryForObject(sql, Long.class, args);
        return value == null ? 0 : value;
    }

    private String actor() {
        UserContextDTO user = UserContext.getUserOrNotExist();
        return user == null || !notBlank(user.getName()) ? "system" : user.getName();
    }

    private String currentOwner() {
        UserContextDTO user = UserContext.getUserOrNotExist();
        return user == null || !notBlank(user.getOwnerId()) ? nodeId : user.getOwnerId();
    }

    private String required(Map<String, Object> request, String key) {
        String value = string(request.get(key));
        if (!notBlank(value)) throw new IllegalArgumentException(key + " 不能为空");
        return value;
    }

    private String value(Map<?, ?> request, String key, String defaultValue) {
        if (request == null) return defaultValue;
        String value = string(request.get(key));
        return notBlank(value) ? value : defaultValue;
    }

    private double positive(Map<String, Object> request, String key, double defaultValue) {
        double value = number(request.get(key), defaultValue);
        if (value <= 0) throw new IllegalArgumentException(key + " 必须大于 0");
        return value;
    }

    private int nonNegativeInt(Map<String, Object> request, String key, int defaultValue) {
        int value = (int) number(request.get(key), defaultValue);
        if (value < 0) throw new IllegalArgumentException(key + " 不能小于 0");
        return value;
    }

    private boolean bool(Map<String, Object> request, String key, boolean defaultValue) {
        Object value = request.get(key);
        return value == null ? defaultValue : Boolean.parseBoolean(String.valueOf(value));
    }

    private static double number(Object value, double defaultValue) {
        if (value instanceof Number number) return number.doubleValue();
        try { return value == null ? defaultValue : Double.parseDouble(String.valueOf(value)); }
        catch (NumberFormatException e) { return defaultValue; }
    }

    private String json(Object value) {
        try { return objectMapper.writeValueAsString(value); }
        catch (JsonProcessingException e) { return String.valueOf(value); }
    }

    private static String csv(Object value) {
        return "\"" + string(value).replace("\"", "\"\"").replace("\r", " ").replace("\n", " ") + "\"";
    }

    private static String string(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }

    private static String now() {
        return LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS).toString();
    }

    private void requireActiveProject(String projectId, String operation) {
        List<Integer> statuses = jdbc.queryForList(
                "select status from project where project_id=? and is_deleted=0", Integer.class, projectId);
        if (statuses.isEmpty()) {
            throw new IllegalArgumentException("项目不存在: " + projectId);
        }
        if (!Integer.valueOf(1).equals(statuses.get(0))) {
            throw new IllegalStateException("项目已归档，不能" + operation);
        }
    }

    private static String shortId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    private static byte[] randomBytes() {
        byte[] bytes = new byte[32];
        new java.security.SecureRandom().nextBytes(bytes);
        return bytes;
    }

    private static String sha256(byte[] bytes) {
        try {
            return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static String truncate(String value, int max) {
        String safe = string(value);
        return safe.length() <= max ? safe : safe.substring(0, max);
    }

    private static void validateHttpUrl(String url) {
        URI uri = URI.create(url);
        if (!("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme())) || !notBlank(uri.getHost())) {
            throw new IllegalArgumentException("必须是有效的 HTTP/HTTPS 地址");
        }
    }

    private static Map<String, Object> castMap(Map<?, ?> value) {
        Map<String, Object> result = new LinkedHashMap<>();
        value.forEach((key, item) -> result.put(String.valueOf(key), item));
        return result;
    }
}
