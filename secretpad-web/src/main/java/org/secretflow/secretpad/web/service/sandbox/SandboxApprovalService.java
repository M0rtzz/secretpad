/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.service.sandbox;

import org.secretflow.secretpad.common.dto.UserContextDTO;
import org.secretflow.secretpad.common.errorcode.AuthErrorCode;
import org.secretflow.secretpad.common.exception.SecretpadException;
import org.secretflow.secretpad.common.util.UserContext;
import org.secretflow.secretpad.web.service.DataSandboxMvpService;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Z-03 沙箱资源申请与审批：申请单 CRUD、两级审批动作、权限/并发/幂等控制、审批历史与执行引擎。
 *
 * <p>审批流程由 {@link SandboxApprovalStateMachine} 驱动：DATA_PROVIDER_REVIEW → OPERATOR_REVIEW →
 * APPROVED，任一级可 REJECTED（RESUBMIT 复审 version+1），APPROVED 由轮询器认领
 * （EXECUTING → COMPLETED），失败自动重试（回退 APPROVED，达上限置 FAILED 可人工 RETRY）。</p>
 *
 * <p>并发控制：所有动作走「条件 UPDATE + affected==1」判定，只有一个线程能赢。
 * 幂等控制：同 owner 同类型（变更类按沙箱）存在进行中申请单时禁止重复提交。
 * 执行引擎非事务：每步自提交，避免跨 gRPC 长事务。</p>
 */
@Slf4j
@Service
public class SandboxApprovalService {

    private static final Set<String> APPROVAL_TYPES = Set.of("CREATE", "RENEW", "SPEC_CHANGE", "RECYCLE");
    private static final Set<String> APPROVAL_ACTIONS = Set.of("APPROVE", "REJECT", "RESUBMIT", "RETRY", "CANCEL");
    private static final Set<String> NETWORK_POLICIES = Set.of("INTERNAL_ONLY", "ALLOW_LIST", "NO_NETWORK");
    private static final Set<String> OPEN_STATUSES = Set.of("DATA_PROVIDER_REVIEW", "OPERATOR_REVIEW", "APPROVED", "EXECUTING");

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final DataSandboxMvpService service;
    private final SandboxApprovalGate gate;

    @Value("${secretpad.node-id:kuscia-system}")
    private String nodeId;

    @Value("${secretpad.data-sandbox.approval.max-retries:3}")
    private int maxRetries;

    public SandboxApprovalService(
            JdbcTemplate jdbc,
            ObjectMapper objectMapper,
            DataSandboxMvpService service,
            SandboxApprovalGate gate) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.service = service;
        this.gate = gate;
    }

    /* ------------------------------- 申请单查询 ------------------------------- */

    public List<Map<String, Object>> listApprovals(String status, String type, String keyword) {
        StringBuilder sql = new StringBuilder("select * from ds_sandbox_approval where deleted=0");
        List<Object> args = new ArrayList<>();
        if (notBlank(status)) {
            sql.append(" and status=?");
            args.add(status.toUpperCase(Locale.ROOT));
        }
        if (notBlank(type)) {
            sql.append(" and approval_type=?");
            args.add(type.toUpperCase(Locale.ROOT));
        }
        if (notBlank(keyword)) {
            sql.append(" and (lower(id) like ? or lower(submitter) like ?)");
            String q = "%" + keyword.toLowerCase(Locale.ROOT) + "%";
            args.add(q);
            args.add(q);
        }
        sql.append(" order by created_at desc");
        return jdbc.queryForList(sql.toString(), args.toArray());
    }

    public Map<String, Object> approval(String id) {
        Map<String, Object> data = requireApproval(id);
        data.put("history", approvalHistory(id));
        return data;
    }

    public List<Map<String, Object>> approvalHistory(String id) {
        return jdbc.queryForList("select * from ds_sandbox_approval_history where approval_id=? order by id desc", id);
    }

    /** 门禁与重试配置，供前端感知 required 状态。 */
    public Map<String, Object> approvalConfig() {
        return Map.of("required", gate.isApprovalRequired(),
                "types", List.of("CREATE", "RENEW", "SPEC_CHANGE", "RECYCLE"),
                "maxRetries", maxRetries);
    }

    /* ------------------------------- 提交申请 ------------------------------- */

    /**
     * 提交申请单：校验类型/沙箱/镜像/规格，幂等（同 owner 同类型进行中不重复），
     * 落库 DATA_PROVIDER_REVIEW 并写 SUBMIT 历史、审计与 webhook。
     */
    public Map<String, Object> submit(Map<String, Object> request) {
        String type = required(request, "approvalType").toUpperCase(Locale.ROOT);
        if (!APPROVAL_TYPES.contains(type)) {
            throw new IllegalArgumentException("不支持的申请类型: " + type);
        }
        String sandboxId = "CREATE".equals(type) ? "" : required(request, "sandboxId");
        String ownerId;
        if (notBlank(value(request, "ownerId", ""))) {
            ownerId = value(request, "ownerId", "");
        } else if (!"CREATE".equals(type)) {
            ownerId = sandboxOwner(sandboxId);
        } else {
            ownerId = gate.effectiveOwner();
        }
        if ("CREATE".equals(type)) {
            validateCreatePayload(request);
        } else {
            ensureSandboxActive(sandboxId);
        }
        assertNoOpenApproval(type, ownerId, sandboxId);

        String id = "apr-" + shortId();
        String now = now();
        jdbc.update("insert into ds_sandbox_approval(id,approval_type,sandbox_id,owner_id,submitter,payload_json,status,current_stage,version,executor,reviewer,review_comment,last_error,retry_count,submitted_at,created_at,updated_at,deleted) "
                        + "values(?,?,?,?,?,?,'DATA_PROVIDER_REVIEW','DATA_PROVIDER_REVIEW',1,'','','','',0,?,?,?,0)",
                id, type, sandboxId, ownerId, operator(), json(new LinkedHashMap<>(request)), now, now, now);
        history(id, "SUBMIT", "", "DATA_PROVIDER_REVIEW", value(request, "reason", ""));
        service.audit("AUDIT", "SANDBOX_APPROVAL_SUBMIT", "SANDBOX_APPROVAL", id,
                type + " reason=" + value(request, "reason", ""), true);
        service.dispatchWebhooks("sandbox.approval.submitted",
                Map.of("approvalId", id, "approvalType", type, "ownerId", ownerId, "sandboxId", sandboxId));
        return approval(id);
    }

    /* ------------------------------- 审批动作 ------------------------------- */

    /**
     * 审批动作：状态机预检 → 角色校验 → 条件 UPDATE（affected==1 才成功，否则并发冲突）→
     * 历史/审计/webhook。RETRY 成功后同步执行（不等轮询）。
     */
    public Map<String, Object> approvalAction(Map<String, Object> request) {
        String id = required(request, "id");
        String action = required(request, "action").toUpperCase(Locale.ROOT);
        if (!APPROVAL_ACTIONS.contains(action)) {
            throw new IllegalArgumentException("不支持的审批动作: " + action);
        }
        String comment = value(request, "comment", "");
        Map<String, Object> approval = requireApproval(id);
        String from = string(approval.get("status"));
        String submitter = string(approval.get("submitter"));
        String ownerId = string(approval.get("owner_id"));
        UserContextDTO user = gate.currentUser();
        SandboxApprovalStateMachine.Action machineAction = SandboxApprovalStateMachine.Action.valueOf(action);
        if (!SandboxApprovalStateMachine.canTransition(from, machineAction)) {
            throw new IllegalStateException("当前状态不允许操作: " + action + " (当前 " + from + ")");
        }
        switch (action) {
            case "APPROVE", "REJECT" -> {
                if ("DATA_PROVIDER_REVIEW".equals(from)) {
                    if (!gate.isDataProvider(user, submitter, ownerId)) {
                        throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "您不是供数方，无权审核该申请");
                    }
                } else if (!gate.isAdminOrOperator(user, ownerId)) {
                    throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "仅运营方/管理员可审核该申请");
                }
            }
            case "RESUBMIT" -> {
                if (!gate.isApplicant(user, submitter)) {
                    throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "仅申请人可提交复审");
                }
            }
            case "CANCEL" -> {
                if (!gate.isApplicant(user, submitter)) {
                    throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "仅申请人可撤回申请");
                }
            }
            case "RETRY" -> {
                if (!gate.isApplicant(user, submitter) && !gate.isAdminOrOperator(user, ownerId)) {
                    throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "仅申请人或运营方可重试");
                }
            }
            default -> throw new IllegalArgumentException("不支持的审批动作: " + action);
        }
        String to = SandboxApprovalStateMachine.transition(from, machineAction);
        int changed = conditionalUpdate(id, from, action, to, comment);
        if (changed != 1) {
            // 条件 UPDATE 失败 = 状态已被他人先改，并发审批冲突
            throw new IllegalStateException("该申请单已被他人处理，请刷新");
        }
        history(id, action, from, to, comment);
        service.audit("AUDIT", "SANDBOX_APPROVAL_" + action, "SANDBOX_APPROVAL", id, comment, true);
        service.dispatchWebhooks("sandbox.approval." + action.toLowerCase(Locale.ROOT),
                Map.of("approvalId", id, "from", from, "to", to));
        if ("RETRY".equals(action)) {
            // 已认领为 EXECUTING，同步执行（结果由 executeOne 落库）
            executeOne(id);
        }
        return approval(id);
    }

    /* ------------------------------- 执行引擎（Z-03 Stage 3） ------------------------------- */

    /**
     * 轮询执行已批准的申请单：认领（只有 status='APPROVED' 可认领，affected==1 才是赢家），
     * 随后执行四类型流程；最后兜底回收卡死的 EXECUTING。
     * 无论 approval.required 开关如何，已存在的 APPROVED 申请单都必须执行（门禁只拦直接操作）。
     */
    @Scheduled(fixedDelayString = "${secretpad.data-sandbox.approval.executor-interval-ms:10000}")
    public void executeApprovals() {
        for (Map<String, Object> row : jdbc.queryForList(
                "select id from ds_sandbox_approval where status='APPROVED' and deleted=0 order by approved_at asc limit 20")) {
            String id = string(row.get("id"));
            int claimed = jdbc.update("update ds_sandbox_approval set status='EXECUTING',current_stage='EXECUTING',executor=?,updated_at=? "
                            + "where id=? and status='APPROVED' and deleted=0",
                    engineActor(), now(), id);
            if (claimed != 1) {
                continue; // 已被其他实例/线程认领
            }
            history(id, "EXECUTE", "APPROVED", "EXECUTING", "");
            executeOne(id);
        }
        reclaimStuckExecuting();
    }

    /** 执行单条申请单：按类型分发，成功 complete，异常 failAndRetry（自动重试/置 FAILED）。 */
    public void executeOne(String id) {
        Map<String, Object> approval = requireApproval(id);
        String type = string(approval.get("approval_type"));
        try {
            if (!service.isKusciaEnabled() && ("CREATE".equals(type) || "SPEC_CHANGE".equals(type))) {
                throw new IllegalStateException("Kuscia 运行时未启用，无法执行沙箱拉起类申请");
            }
            switch (type) {
                case "CREATE" -> execCreate(approval);
                case "RENEW" -> execRenew(approval);
                case "SPEC_CHANGE" -> execSpecChange(approval);
                case "RECYCLE" -> execRecycle(approval);
                default -> throw new IllegalStateException("未知申请类型: " + type);
            }
            complete(id);
        } catch (Exception e) {
            failAndRetry(id, truncate(e.getMessage(), 900));
        }
    }

    private void complete(String id) {
        int changed = jdbc.update("update ds_sandbox_approval set status='COMPLETED',current_stage='COMPLETED',completed_at=?,last_error='',updated_at=? "
                + "where id=? and status='EXECUTING' and deleted=0", now(), now(), id);
        if (changed != 1) {
            log.warn("Approval {} not in EXECUTING when completing, skip", id);
            return;
        }
        Map<String, Object> approval = requireApproval(id);
        history(id, "COMPLETE", "EXECUTING", "COMPLETED", "");
        service.auditAs("AUDIT", "INFO", engineActor(), "SANDBOX_APPROVAL_COMPLETE", "SANDBOX_APPROVAL", id,
                string(approval.get("approval_type")) + " " + string(approval.get("sandbox_id")), true);
        service.dispatchWebhooks("sandbox.approval.completed",
                Map.of("approvalId", id, "approvalType", approval.get("approval_type"), "sandboxId", approval.get("sandbox_id")));
    }

    /** 失败重试：retry_count+1；达到上限置 FAILED + 告警；否则回退 APPROVED 由下个轮询周期自动重试。 */
    private void failAndRetry(String id, String error) {
        int incremented = jdbc.update("update ds_sandbox_approval set retry_count=retry_count+1,last_error=?,updated_at=? "
                + "where id=? and status='EXECUTING' and deleted=0", error, now(), id);
        if (incremented != 1) {
            return; // 状态已被并发改变
        }
        Map<String, Object> approval = requireApproval(id);
        int retries = intValue(approval.get("retry_count"), 0);
        if (retries >= maxRetries) {
            jdbc.update("update ds_sandbox_approval set status='FAILED',current_stage='FAILED',completed_at=?,updated_at=? where id=?", now(), now(), id);
            history(id, "FAIL", "EXECUTING", "FAILED", error);
            service.auditAs("AUDIT", "ERROR", engineActor(), "SANDBOX_APPROVAL_FAILED", "SANDBOX_APPROVAL", id, error, false);
            service.dispatchWebhooks("sandbox.approval.failed",
                    Map.of("approvalId", id, "approvalType", approval.get("approval_type"), "error", error));
            service.raiseAlert("WARNING", "SANDBOX", "沙箱申请执行失败",
                    "申请单 " + id + " (" + approval.get("approval_type") + ") 执行失败：" + error,
                    "approval:" + id + ":failed");
        } else {
            jdbc.update("update ds_sandbox_approval set status='APPROVED',current_stage='APPROVED',updated_at=? where id=?", now(), id);
            history(id, "RETRY", "EXECUTING", "APPROVED", "自动重试：" + error);
            service.auditAs("AUDIT", "WARN", engineActor(), "SANDBOX_APPROVAL_RETRY", "SANDBOX_APPROVAL", id, error, true);
        }
    }

    /** JVM 崩溃/进程卡死的 EXECUTING 兜底：超过 10 分钟未更新 → 达上限 FAILED，否则回退 APPROVED 自动重试。 */
    public void reclaimStuckExecuting() {
        String threshold = LocalDateTime.now().minusMinutes(10).toString();
        for (Map<String, Object> row : jdbc.queryForList(
                "select id,retry_count from ds_sandbox_approval where status='EXECUTING' and updated_at<? and deleted=0", threshold)) {
            String id = string(row.get("id"));
            int retries = intValue(row.get("retry_count"), 0);
            if (retries >= maxRetries) {
                jdbc.update("update ds_sandbox_approval set status='FAILED',current_stage='FAILED',last_error=?,completed_at=?,updated_at=? where id=?",
                        "执行超时（10 分钟未完成）", now(), now(), id);
                history(id, "FAIL", "EXECUTING", "FAILED", "执行超时");
                service.auditAs("AUDIT", "ERROR", engineActor(), "SANDBOX_APPROVAL_STUCK", "SANDBOX_APPROVAL", id, "EXECUTING 超时转 FAILED", false);
            } else {
                jdbc.update("update ds_sandbox_approval set status='APPROVED',current_stage='APPROVED',updated_at=? where id=?", now(), id);
                history(id, "RETRY", "EXECUTING", "APPROVED", "卡死回退自动重试");
                service.auditAs("AUDIT", "WARN", engineActor(), "SANDBOX_APPROVAL_STUCK", "SANDBOX_APPROVAL", id, "EXECUTING 超时回退 APPROVED", true);
            }
        }
    }

    /* ------------------------------- 四类型执行流（幂等） ------------------------------- */

    private void execCreate(Map<String, Object> approval) {
        String sandboxId = string(approval.get("sandbox_id"));
        if (notBlank(sandboxId)) {
            // 重试路径：沙箱已建出，校验存在性后按需拉起
            Map<String, Object> sbx = requireSandbox(sandboxId);
            if (((Number) sbx.get("deleted")).intValue() == 1 || "DESTROYED".equals(string(sbx.get("status")))) {
                throw new IllegalStateException("沙箱已销毁，无法继续执行");
            }
            startIfNeeded(sbx);
            return;
        }
        // 首次执行：createSandbox 内部重新 assertCapacity + 镜像 enable 校验 + reserveAllocations；
        // 异步无 UserContext，必须显式传 ownerId
        Map<String, Object> req = new LinkedHashMap<>(parsePayload(approval));
        req.put("ownerId", string(approval.get("owner_id")));
        Map<String, Object> created = service.createSandbox(req);
        String id = string(created.get("id"));
        jdbc.update("update ds_sandbox_approval set sandbox_id=? where id=?", id, approval.get("id"));
        startIfNeeded(created);
    }

    private void startIfNeeded(Map<String, Object> sbx) {
        String status = string(sbx.get("status"));
        if ("RUNNING".equals(status) || "STARTING".equals(status)) {
            return; // 已运行/拉起中，交由 syncKusciaStatuses 推进
        }
        if ("STOPPED".equals(status) || "ERROR".equals(status)) {
            String id = string(sbx.get("id"));
            jdbc.update("update ds_sandbox set status='STARTING',intent='START',last_error='',updated_at=? where id=?", now(), id);
            service.reserveAllocations(sbx);
            String error = service.startKuscia(sbx);
            if (!error.isEmpty()) {
                jdbc.update("update ds_sandbox set status='ERROR',intent='',last_error=?,updated_at=? where id=?", error, now(), id);
                service.raiseSandboxErrorAlert(id, "启动失败：" + error);
                throw new IllegalStateException("沙箱启动失败: " + error);
            }
            return;
        }
        throw new IllegalStateException("沙箱状态不允许自动启动: " + status);
    }

    private void execRenew(Map<String, Object> approval) {
        String sandboxId = string(approval.get("sandbox_id"));
        Map<String, Object> sbx = requireSandbox(sandboxId);
        if (((Number) sbx.get("deleted")).intValue() == 1 || "DESTROYED".equals(string(sbx.get("status")))) {
            return; // 已回收，无可续，视为完成
        }
        Map<String, Object> payload = parsePayload(approval);
        int days = intValue(payload.get("days"), 7);
        service.sandboxAction(Map.of("id", sandboxId, "action", "RENEW", "days", days));
    }

    private void execSpecChange(Map<String, Object> approval) {
        String sandboxId = string(approval.get("sandbox_id"));
        Map<String, Object> sbx = requireSandbox(sandboxId);
        if (((Number) sbx.get("deleted")).intValue() == 1 || "DESTROYED".equals(string(sbx.get("status")))) {
            throw new IllegalStateException("沙箱已销毁，无法变更规格");
        }
        Map<String, Object> payload = parsePayload(approval);
        double cpu = positive(payload.get("cpuCores"), number(sbx.get("cpu_cores"), 1));
        double memory = positive(payload.get("memoryGb"), number(sbx.get("memory_gb"), 2));
        int gpu = intValue(payload.get("gpuCount"), (int) number(sbx.get("gpu_count"), 0));
        double storage = positive(payload.get("storageGb"), number(sbx.get("storage_gb"), 10));
        if (gpu < 0) {
            throw new IllegalStateException("gpuCount 不能小于 0");
        }
        // 1) 停旧任务（失败保留 job id 供重试再删）
        String oldJob = string(sbx.get("kuscia_job_id"));
        if (notBlank(oldJob)) {
            String stopError = service.stopKuscia(sbx, "SPEC_CHANGE 规格变更");
            if (!stopError.isEmpty()) {
                throw new IllegalStateException("停止旧任务失败: " + stopError);
            }
            String delError = service.deleteKuscia(sbx);
            if (!delError.isEmpty()) {
                throw new IllegalStateException("删除旧任务失败: " + delError);
            }
        }
        // 2) 落新规格 + 清空 job/endpoint/状态（job id 已清 → startKuscia 走 createJob 新规格）
        jdbc.update("update ds_sandbox set cpu_cores=?,memory_gb=?,gpu_count=?,storage_gb=?,kuscia_job_id='',endpoint='',kuscia_job_state='',status='STARTING',intent='START',last_error='',updated_at=? where id=?",
                cpu, memory, gpu, storage, now(), sandboxId);
        // 3) 释放旧分配 → 校验新容量 → 按新规格重预留（容量由引擎原子占用）
        Map<String, Object> fresh = service.sandbox(sandboxId);
        service.releaseAllocations(fresh, "SPEC_CHANGE");
        service.assertCapacity(string(fresh.get("owner_id")), cpu, memory, gpu, storage);
        service.reserveAllocations(fresh);
        // 4) 新规格启动
        String error = service.startKuscia(fresh);
        if (!error.isEmpty()) {
            jdbc.update("update ds_sandbox set status='ERROR',intent='',last_error=?,updated_at=? where id=?", error, now(), sandboxId);
            service.raiseSandboxErrorAlert(sandboxId, "规格变更启动失败：" + error);
            throw new IllegalStateException("规格变更启动失败: " + error);
        }
        // 5) 记录规格变更元数据
        service.appendRuntimeMeta(sandboxId, Map.of("spec_changed", true, "prev_job", oldJob,
                "spec", Map.of("cpu", cpu, "memory_gb", memory, "gpu", gpu, "storage_gb", storage)));
    }

    private void execRecycle(Map<String, Object> approval) {
        String sandboxId = string(approval.get("sandbox_id"));
        Map<String, Object> sbx = requireSandbox(sandboxId);
        if (((Number) sbx.get("deleted")).intValue() == 1 || "DESTROYED".equals(string(sbx.get("status")))) {
            return; // 已回收，视为完成
        }
        String stopError = service.stopKuscia(sbx, "RECYCLE 申请回收");
        if (!stopError.isEmpty()) {
            throw new IllegalStateException("停止任务失败: " + stopError);
        }
        String delError = service.deleteKuscia(sbx);
        if (!delError.isEmpty()) {
            throw new IllegalStateException("删除任务失败: " + delError);
        }
        jdbc.update("update ds_sandbox set status='DESTROYED',deleted=1,intent='',last_error='',updated_at=? where id=?", now(), sandboxId);
        service.releaseAllocations(sbx, "DESTROY");
    }

    /* ------------------------------- 内部工具 ------------------------------- */

    private int conditionalUpdate(String id, String from, String action, String to, String comment) {
        String now = now();
        return switch (action) {
            case "APPROVE" -> {
                if ("DATA_PROVIDER_REVIEW".equals(from)) {
                    yield jdbc.update("update ds_sandbox_approval set status='OPERATOR_REVIEW',current_stage='OPERATOR_REVIEW',reviewer=?,review_comment=?,updated_at=? "
                                    + "where id=? and status='DATA_PROVIDER_REVIEW' and deleted=0",
                            operator(), comment, now, id);
                }
                yield jdbc.update("update ds_sandbox_approval set status='APPROVED',current_stage='APPROVED',reviewer=?,review_comment=?,approved_at=?,updated_at=? "
                                + "where id=? and status='OPERATOR_REVIEW' and deleted=0",
                        operator(), comment, now, now, id);
            }
            case "REJECT" -> jdbc.update("update ds_sandbox_approval set status='REJECTED',current_stage='REJECTED',reviewer=?,review_comment=?,updated_at=? "
                            + "where id=? and status in ('DATA_PROVIDER_REVIEW','OPERATOR_REVIEW') and deleted=0",
                    operator(), comment, now, id);
            case "RESUBMIT" -> jdbc.update("update ds_sandbox_approval set status='DATA_PROVIDER_REVIEW',current_stage='DATA_PROVIDER_REVIEW',version=version+1,reviewer='',review_comment='',updated_at=? "
                            + "where id=? and status='REJECTED' and deleted=0",
                    now, id);
            case "CANCEL" -> jdbc.update("update ds_sandbox_approval set status='CANCELLED',current_stage='CANCELLED',updated_at=? "
                            + "where id=? and status in ('DATA_PROVIDER_REVIEW','OPERATOR_REVIEW','APPROVED') and deleted=0",
                    now, id);
            case "RETRY" -> jdbc.update("update ds_sandbox_approval set status='EXECUTING',current_stage='EXECUTING',executor=?,retry_count=0,last_error='',updated_at=? "
                            + "where id=? and status='FAILED' and deleted=0",
                    operator(), now, id);
            default -> throw new IllegalArgumentException("不支持的审批动作: " + action);
        };
    }

    private void history(String id, String action, String from, String to, String comment) {
        jdbc.update("insert into ds_sandbox_approval_history(approval_id,action,from_status,to_status,operator,comment,created_at) values(?,?,?,?,?,?,?)",
                id, action, from, to, operator(), comment, now());
    }

    private void assertNoOpenApproval(String type, String ownerId, String sandboxId) {
        long existing;
        if ("CREATE".equals(type)) {
            existing = count("select count(1) from ds_sandbox_approval where deleted=0 and approval_type='CREATE' and owner_id=? and status in ('DATA_PROVIDER_REVIEW','OPERATOR_REVIEW','APPROVED','EXECUTING')", ownerId);
        } else {
            existing = count("select count(1) from ds_sandbox_approval where deleted=0 and approval_type=? and sandbox_id=? and status in ('DATA_PROVIDER_REVIEW','OPERATOR_REVIEW','APPROVED','EXECUTING')", type, sandboxId);
        }
        if (existing > 0) {
            throw new IllegalStateException("已有同类型申请单处理中，请等待完成或取消");
        }
    }

    private void validateCreatePayload(Map<String, Object> request) {
        String imageId = required(request, "imageId");
        if (count("select count(1) from ds_sandbox_image where id=? and enabled=1", imageId) == 0) {
            throw new IllegalArgumentException("环境镜像不存在或未启用: " + imageId);
        }
        String networkPolicy = value(request, "networkPolicy", "INTERNAL_ONLY").toUpperCase(Locale.ROOT);
        if (!NETWORK_POLICIES.contains(networkPolicy)) {
            throw new IllegalArgumentException("不支持的网络策略: " + networkPolicy);
        }
        if (positive(request.get("cpuCores"), 1) <= 0) {
            throw new IllegalArgumentException("cpuCores 必须大于 0");
        }
        if (positive(request.get("memoryGb"), 2) <= 0) {
            throw new IllegalArgumentException("memoryGb 必须大于 0");
        }
        if (intValue(request.get("gpuCount"), 0) < 0) {
            throw new IllegalArgumentException("gpuCount 不能小于 0");
        }
        if (positive(request.get("storageGb"), 10) <= 0) {
            throw new IllegalArgumentException("storageGb 必须大于 0");
        }
        int days = intValue(request.get("validDays"), 7);
        if (days < 1 || days > 365) {
            throw new IllegalArgumentException("validDays 必须在 1-365 之间");
        }
    }

    private void ensureSandboxActive(String sandboxId) {
        Integer deleted = jdbc.queryForObject("select deleted from ds_sandbox where id=?", Integer.class, sandboxId);
        if (deleted == null) {
            throw new IllegalArgumentException("沙箱不存在: " + sandboxId);
        }
        if (deleted == 1) {
            throw new IllegalStateException("沙箱已删除，不能提交变更申请");
        }
    }

    private String sandboxOwner(String sandboxId) {
        try {
            return string(jdbc.queryForObject("select owner_id from ds_sandbox where id=?", String.class, sandboxId));
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("沙箱不存在: " + sandboxId);
        }
    }

    private Map<String, Object> requireSandbox(String sandboxId) {
        try {
            return service.sandbox(sandboxId);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("沙箱不存在: " + sandboxId);
        }
    }

    private Map<String, Object> requireApproval(String id) {
        try {
            return new LinkedHashMap<>(jdbc.queryForMap("select * from ds_sandbox_approval where id=? and deleted=0", id));
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("申请单不存在: " + id);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parsePayload(Map<String, Object> approval) {
        String raw = string(approval.get("payload_json"));
        try {
            Object parsed = objectMapper.readValue(raw, Object.class);
            return parsed instanceof Map<?, ?> map ? castMap(map) : new LinkedHashMap<>();
        } catch (Exception e) {
            return new LinkedHashMap<>();
        }
    }

    private String engineActor() {
        return "system:" + nodeId;
    }

    private String operator() {
        UserContextDTO user = UserContext.getUserOrNotExist();
        return user == null || !notBlank(user.getName()) ? engineActor() : user.getName();
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

    private String required(Map<String, Object> request, String key) {
        String value = string(request.get(key));
        if (!notBlank(value)) {
            throw new IllegalArgumentException(key + " 不能为空");
        }
        return value;
    }

    private String value(Map<String, Object> request, String key, String defaultValue) {
        String value = string(request.get(key));
        return notBlank(value) ? value : defaultValue;
    }

    private double positive(Object value, double defaultValue) {
        double number = number(value, defaultValue);
        if (number <= 0) {
            throw new IllegalArgumentException("规格必须大于 0");
        }
        return number;
    }

    private static double number(Object value, double defaultValue) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        try {
            return value == null ? defaultValue : Double.parseDouble(String.valueOf(value));
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static int intValue(Object value, int defaultValue) {
        return (int) Math.round(number(value, defaultValue));
    }

    private String json(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            return String.valueOf(value);
        }
    }

    private static String string(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }

    private static String now() {
        return LocalDateTime.now().truncatedTo(java.time.temporal.ChronoUnit.SECONDS).toString();
    }

    private static String shortId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    private static String truncate(String value, int max) {
        String safe = string(value);
        return safe.length() <= max ? safe : safe.substring(0, max);
    }

    private static Map<String, Object> castMap(Map<?, ?> value) {
        Map<String, Object> result = new LinkedHashMap<>();
        value.forEach((key, item) -> result.put(String.valueOf(key), item));
        return result;
    }
}
