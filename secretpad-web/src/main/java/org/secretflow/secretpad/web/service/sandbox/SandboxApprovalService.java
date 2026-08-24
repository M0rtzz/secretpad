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
import org.secretflow.secretpad.common.errorcode.DataErrorCode;
import org.secretflow.secretpad.common.errorcode.ProjectErrorCode;
import org.secretflow.secretpad.common.exception.SecretpadException;
import org.secretflow.secretpad.common.util.UserContext;
import org.secretflow.secretpad.persistence.entity.SandboxApprovalSyncDO;
import org.secretflow.secretpad.persistence.entity.ProjectAssetDO;
import org.secretflow.secretpad.persistence.entity.ProjectDatatableDO;
import org.secretflow.secretpad.persistence.repository.ProjectAssetRepository;
import org.secretflow.secretpad.persistence.repository.ProjectDatatableRepository;
import org.secretflow.secretpad.persistence.repository.SandboxApprovalSyncRepository;
import org.secretflow.secretpad.web.service.AssetTimeWindow;
import org.secretflow.secretpad.web.service.DataSandboxMvpService;
import org.secretflow.secretpad.web.service.MinioAssetStorage;
import org.secretflow.secretpad.web.service.storage.NodeDatasetStore;
import org.secretflow.secretpad.web.service.storage.SandboxDbService;
import org.secretflow.secretpad.web.service.sync.AssetSyncService;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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

    private static final Set<String> APPROVAL_TYPES = Set.of("CREATE", "RENEW", "SPEC_CHANGE", "DATA_CHANGE", "CONFIG_CHANGE", "RECYCLE", "ASSET_DELETE");
    /**
     * 执行引擎受理范围的 SQL in 子句。{@code ds_sandbox_approval} 被模型 API 供数方审批
     * （approval_type=MODEL_API，见 ModelApiApprovalService）等业务共用，轮询必须按类型隔离，
     * 否则会把他类申请单认领为 EXECUTING 并以「未知申请类型」置为 FAILED，阻断其自身落库流程。
     */
    private static final String EXECUTABLE_TYPES_SQL = APPROVAL_TYPES.stream()
            .sorted().map(type -> "'" + type + "'").collect(Collectors.joining(",", "(", ")"));
    private static final Set<String> APPROVAL_ACTIONS = Set.of("APPROVE", "REJECT", "RESUBMIT", "RETRY", "CANCEL");
    private static final Set<String> NETWORK_POLICIES = Set.of("INTERNAL_ONLY", "ALLOW_LIST", "NO_NETWORK");
    private static final Set<String> OPEN_STATUSES = Set.of("DATA_PROVIDER_REVIEW", "OPERATOR_REVIEW", "APPROVED", "EXECUTING");

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final DataSandboxMvpService service;
    private final SandboxApprovalGate gate;
    private final MinioAssetStorage assetStorage;
    private final SandboxApprovalSyncRepository approvalSyncRepository;
    private final ProjectAssetRepository projectAssetRepository;
    private final ProjectDatatableRepository projectDatatableRepository;
    private final AssetSyncService assetSyncService;
    private final SandboxDbService sandboxDbService;
    private final NodeDatasetStore nodeDatasetStore;
    private final Map<String, Integer> appliedSnapshotHashes = new ConcurrentHashMap<>();

    @Value("${secretpad.node-id:kuscia-system}")
    private String nodeId;

    /** Local node whose approved applications this process is allowed to execute. */
    @Value("${secretpad.data-sandbox.approval.executor-node-id:${secretpad.node-id:kuscia-system}}")
    private String executorNodeId;

    @Value("${secretpad.data-sandbox.approval.max-retries:3}")
    private int maxRetries;

    public SandboxApprovalService(
            @Qualifier("jdbcTemplate") JdbcTemplate jdbc,
            ObjectMapper objectMapper,
            DataSandboxMvpService service,
            SandboxApprovalGate gate,
            MinioAssetStorage assetStorage,
            SandboxApprovalSyncRepository approvalSyncRepository,
            ProjectAssetRepository projectAssetRepository,
            ProjectDatatableRepository projectDatatableRepository,
            AssetSyncService assetSyncService,
            SandboxDbService sandboxDbService,
            NodeDatasetStore nodeDatasetStore) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.service = service;
        this.gate = gate;
        this.assetStorage = assetStorage;
        this.approvalSyncRepository = approvalSyncRepository;
        this.projectAssetRepository = projectAssetRepository;
        this.projectDatatableRepository = projectDatatableRepository;
        this.assetSyncService = assetSyncService;
        this.sandboxDbService = sandboxDbService;
        this.nodeDatasetStore = nodeDatasetStore;
    }

    /* ------------------------------- 申请单查询 ------------------------------- */

    public List<Map<String, Object>> listApprovals(String status, String type, String keyword) {
        applySyncedApprovals();
        String current = operator();
        String currentNode = gate.effectiveOwner();
        boolean admin = gate.isAdmin(gate.currentUser());
        StringBuilder sql = new StringBuilder("select distinct a.*,coalesce(p.name,a.project_id) project_name "
                + "from ds_sandbox_approval a "
                + "left join project p on p.project_id=a.project_id and p.is_deleted=0 "
                + "left join ds_sandbox_approval_vote v on v.approval_id=a.id "
                + "where a.deleted=0 and (a.submitter=? or a.applicant_node_id=? or v.voter_node_id=?");
        List<Object> args = new ArrayList<>(List.of(current, currentNode, currentNode));
        if (admin) {
            sql.append(" or 1=1");
        }
        sql.append(")");
        if (notBlank(status)) {
            sql.append(" and a.status=?");
            args.add(status.toUpperCase(Locale.ROOT));
        }
        if (notBlank(type)) {
            sql.append(" and a.approval_type=?");
            args.add(type.toUpperCase(Locale.ROOT));
        }
        if (notBlank(keyword)) {
            sql.append(" and (lower(a.id) like ? or lower(a.submitter) like ?)");
            String q = "%" + keyword.toLowerCase(Locale.ROOT) + "%";
            args.add(q);
            args.add(q);
        }
        sql.append(" order by a.created_at desc");
        List<Map<String, Object>> rows = jdbc.queryForList(sql.toString(), args.toArray());
        rows.forEach(row -> row.put("direction", isApplicant(row) ? "OUTGOING" : "INCOMING"));
        return rows;
    }

    public Map<String, Object> approval(String id) {
        applySyncedApprovals();
        Map<String, Object> data = requireApproval(id);
        assertApprovalVisible(data);
        data.put("history", approvalHistory(id));
        data.put("votes", jdbc.queryForList("select * from ds_sandbox_approval_vote where approval_id=? order by voter_node_id", id));
        if ("ASSET_DELETE".equals(String.valueOf(data.get("approval_type")))) {
            data.put("asset_detail", assetDeletionDetail(data));
        }
        return data;
    }

    /** Enrich data deletion approvals with the catalog metadata users need to review. */
    @SuppressWarnings("unchecked")
    private Map<String, Object> assetDeletionDetail(Map<String, Object> approval) {
        Map<String, Object> detail = new LinkedHashMap<>();
        Map<String, Object> payload = new LinkedHashMap<>();
        try {
            Object raw = approval.get("payload_json");
            if (raw != null && !String.valueOf(raw).isBlank()) {
                payload.putAll(objectMapper.readValue(String.valueOf(raw), Map.class));
            }
        } catch (Exception ignored) {
            // Keep the approval visible even when a legacy payload is malformed.
        }
        String assetId = string(payload.get("assetId"));
        detail.put("asset_id", assetId);
        detail.put("name", string(payload.get("assetName")));
        detail.put("provider_node_id", "");
        detail.put("provider_node_name", "");
        detail.put("uploaded_at", "");
        detail.put("data_stage", "");
        detail.put("project_shared", false);
        detail.put("projects", List.of());
        if (assetId.isBlank()) return detail;

        List<Map<String, Object>> assets = jdbc.queryForList(
                "select a.name,a.provider_node_id,a.created_at,a.data_stage,n.name provider_node_name "
                        + "from ds_data_asset a left join node n on (n.node_id=a.provider_node_id or n.inst_id=a.provider_node_id) "
                        + "and n.is_deleted=0 where a.id=? and a.deleted=0", assetId);
        if (!assets.isEmpty()) {
            Map<String, Object> asset = assets.get(0);
            detail.put("name", string(asset.get("name")));
            detail.put("provider_node_id", string(asset.get("provider_node_id")));
            detail.put("provider_node_name", string(asset.get("provider_node_name")));
            detail.put("uploaded_at", string(asset.get("created_at")));
            detail.put("data_stage", string(asset.get("data_stage")));
        }
        List<Map<String, Object>> projects = jdbc.queryForList(
                "select distinct p.project_id,p.name from project p join ("
                        + "select project_id from ds_project_asset where asset_id=? and deleted=0 "
                        + "union select project_id from project_datatable where datatable_id=(select datatable_id from ds_data_asset where id=?) and is_deleted=0"
                        + ") mounted on mounted.project_id=p.project_id where p.is_deleted=0 order by p.name,p.project_id",
                assetId, assetId);
        detail.put("projects", projects);
        detail.put("project_shared", !projects.isEmpty());
        return detail;
    }

    public List<Map<String, Object>> approvalHistory(String id) {
        applySyncedApprovals();
        assertApprovalVisible(requireApproval(id));
        return jdbc.queryForList("select * from ds_sandbox_approval_history where approval_id=? order by id desc", id);
    }

    /** 门禁与重试配置，供前端感知 required 状态。 */
    public Map<String, Object> approvalConfig() {
        return Map.of("required", gate.isApprovalRequired(),
                "types", List.of("CREATE", "RENEW", "SPEC_CHANGE", "DATA_CHANGE", "CONFIG_CHANGE", "RECYCLE", "ASSET_DELETE"),
                "maxRetries", maxRetries);
    }

    /* ------------------------------- 提交申请 ------------------------------- */

    /**
     * 提交申请单：校验类型/沙箱/镜像/规格，幂等（同 owner 同类型进行中不重复），
     * 落库 DATA_PROVIDER_REVIEW 并写 SUBMIT 历史、审计与 webhook。
     */
    @Transactional
    public Map<String, Object> submit(Map<String, Object> request) {
        String type = required(request, "approvalType").toUpperCase(Locale.ROOT);
        if (!APPROVAL_TYPES.contains(type)) {
            throw new IllegalArgumentException("不支持的申请类型: " + type);
        }
        if ("ASSET_DELETE".equals(type)) {
            throw new IllegalArgumentException("数据删除申请必须从数据目录发起");
        }
        String sandboxId = "CREATE".equals(type) ? "" : required(request, "sandboxId");
        String applicantNodeId = gate.effectiveOwner();
        String ownerId = "CREATE".equals(type) ? applicantNodeId : sandboxOwner(sandboxId);
        // Check CREATE idempotency before project/payload validation so a retried
        // request reports the existing open approval instead of a missing field.
        if ("CREATE".equals(type)) {
            assertNoOpenApproval(type, ownerId, sandboxId);
        }
        if (!"CREATE".equals(type) && !gate.matchesCurrentNode(ownerId)) {
            throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "只能为当前所属节点提交沙箱申请");
        }
        String projectId = "CREATE".equals(type)
                ? required(request, "projectId")
                : string(requireSandbox(sandboxId).get("project_id"));
        // 兼容新版项目模型启用前创建的历史沙箱：这些记录的 project_id 为空或项目已不存在。
        // 仅 RECYCLE 可跳过项目审批，并仍由下方的节点归属与创建人校验保护；其他变更继续强制关联有效项目。
        boolean legacyRecycle = "RECYCLE".equals(type) && !projectExists(projectId);
        if (!legacyRecycle) {
            requireProjectMembership(projectId, applicantNodeId);
        }
        if (Set.of("CREATE", "DATA_CHANGE").contains(type)) {
            requireActiveProject(projectId, "CREATE".equals(type) ? "创建沙箱" : "挂载数据");
        }
        if ("CREATE".equals(type)) {
            validateCreatePayload(request);
            validateDatasetAssets(projectId, request.get("datasetAssetIds"));
        } else {
            ensureSandboxActive(sandboxId);
            assertSandboxCreator(sandboxId);
            if ("DATA_CHANGE".equals(type)) {
                validateDatasetAssets(projectId, request.get("datasetAssetIds"));
            }
        }
        assertNoOpenApproval(type, ownerId, sandboxId);
        Map<String, Object> payload = new LinkedHashMap<>(request);
        if (Set.of("CREATE", "DATA_CHANGE").contains(type)) {
            payload.put("datasetNames",
                    datasetAssetNames(projectId, request.get("datasetAssetIds"), request.get("datasetNames")));
        }
        if ("RECYCLE".equals(type)) {
            payload.put("sandboxName", string(requireSandbox(sandboxId).get("name")));
        }

        String id = "apr-" + shortId();
        String now = now();
        List<String> voters = legacyRecycle ? List.of() : projectVoters(projectId, applicantNodeId);
        String initialStatus = voters.isEmpty() ? "APPROVED" : "DATA_PROVIDER_REVIEW";
        jdbc.update("insert into ds_sandbox_approval(id,approval_type,sandbox_id,owner_id,submitter,payload_json,status,current_stage,version,executor,reviewer,review_comment,last_error,retry_count,submitted_at,approved_at,created_at,updated_at,deleted,project_id,applicant_node_id,project_snapshot_at) "
                        + "values(?,?,?,?,?,?,?,?,1,'','','','',0,?,?,?,?,0,?,?,?)",
                id, type, sandboxId, ownerId, operator(), json(payload), initialStatus, initialStatus,
                now, voters.isEmpty() ? now : "", now, now, projectId, applicantNodeId,
                legacyRecycle ? "" : projectSnapshot(projectId));
        voters.forEach(voter -> jdbc.update("insert into ds_sandbox_approval_vote(approval_id,voter_node_id,status,voter,comment,voted_at) values(?,?,'PENDING','','','')", id, voter));
        history(id, "SUBMIT", "", initialStatus, value(request, "reason", ""));
        service.audit("AUDIT", "SANDBOX_APPROVAL_SUBMIT", "SANDBOX_APPROVAL", id,
                type + " reason=" + value(request, "reason", ""), true);
        service.dispatchWebhooks("sandbox.approval.submitted",
                Map.of("approvalId", id, "approvalType", type, "ownerId", ownerId, "sandboxId", sandboxId));
        publishSnapshot(id);
        return approval(id);
    }

    /** Create one synchronized unanimous-consent request for every project using an asset. */
    @Transactional
    public List<String> submitAssetDeletion(String assetId, String assetName, List<String> projectIds) {
        String applicantNodeId = gate.effectiveOwner();
        if (count("select count(1) from ds_sandbox_approval where approval_type='ASSET_DELETE' and sandbox_id=? and status in ('DATA_PROVIDER_REVIEW','APPROVED','EXECUTING') and deleted=0", assetId) > 0) {
            throw SecretpadException.of(DataErrorCode.DATA_ASSET_DELETE_PENDING);
        }
        List<String> approvals = new ArrayList<>();
        String submittedAt = now();
        for (String projectId : projectIds.stream().distinct().sorted().toList()) {
            requireProjectMembership(projectId, applicantNodeId);
            String id = "apr-" + shortId();
            List<String> voters = projectVoters(projectId, applicantNodeId);
            String initialStatus = voters.isEmpty() ? "APPROVED" : "DATA_PROVIDER_REVIEW";
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("approvalType", "ASSET_DELETE");
            payload.put("assetId", assetId);
            payload.put("assetName", assetName);
            payload.put("projectId", projectId);
            payload.put("reason", "删除已挂载到项目的数据");
            jdbc.update("insert into ds_sandbox_approval(id,approval_type,sandbox_id,owner_id,submitter,payload_json,status,current_stage,version,executor,reviewer,review_comment,last_error,retry_count,submitted_at,approved_at,created_at,updated_at,deleted,project_id,applicant_node_id,project_snapshot_at) "
                            + "values(?,'ASSET_DELETE',?,?,?,?,?,?,1,'','','','',0,?,?,?,?,0,?,?,?)",
                    id, assetId, applicantNodeId, operator(), json(payload), initialStatus, initialStatus,
                    submittedAt, voters.isEmpty() ? submittedAt : "", submittedAt, submittedAt, projectId,
                    applicantNodeId, projectSnapshot(projectId));
            voters.forEach(voter -> jdbc.update("insert into ds_sandbox_approval_vote(approval_id,voter_node_id,status,voter,comment,voted_at) values(?,?,'PENDING','','','')", id, voter));
            history(id, "SUBMIT", "", initialStatus, "删除数据 " + assetName);
            service.audit("AUDIT", "ASSET_DELETE_SUBMIT", "DATA_ASSET", assetId,
                    "project=" + projectId + " approval=" + id, true);
            publishSnapshot(id);
            approvals.add(id);
        }
        return approvals;
    }

    /* ------------------------------- 审批动作 ------------------------------- */

    /**
     * 审批动作：状态机预检 → 角色校验 → 条件 UPDATE（affected==1 才成功，否则并发冲突）→
     * 历史/审计/webhook。RETRY 成功后同步执行（不等轮询）。
     */
    @Transactional
    public Map<String, Object> approvalAction(Map<String, Object> request) {
        applySyncedApprovals();
        String id = required(request, "id");
        String action = required(request, "action").toUpperCase(Locale.ROOT);
        if (!APPROVAL_ACTIONS.contains(action)) {
            throw new IllegalArgumentException("不支持的审批动作: " + action);
        }
        String comment = value(request, "comment", "");
        Map<String, Object> approval = requireApproval(id);
        String from = string(approval.get("status"));
        assertApprovalVisible(approval);
        switch (action) {
            case "APPROVE", "REJECT" -> {
                if ("DATA_PROVIDER_REVIEW".equals(from)) {
                    vote(id, action, comment);
                } else if ("OPERATOR_REVIEW".equals(from)) {
                    if (!gate.isAdminOrOperator(gate.currentUser(), string(approval.get("applicant_node_id")))) {
                        throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "仅运营方可处理运营审核");
                    }
                    if (conditionalUpdate(id, from, action, "", comment) != 1) {
                        throw new IllegalStateException("申请状态已被其他审核人更新，请刷新后重试");
                    }
                } else {
                    throw new IllegalStateException("当前申请不在审核中");
                }
            }
            case "RESUBMIT" -> {
                if (!isApplicant(approval)) {
                    throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "仅申请人可提交复审");
                }
                if (!"REJECTED".equals(from)) throw new IllegalStateException("只有已驳回申请可复审");
                jdbc.update("update ds_sandbox_approval_vote set status='PENDING',voter='',comment='',voted_at='' where approval_id=?", id);
                int voters = (int) count("select count(1) from ds_sandbox_approval_vote where approval_id=?", id);
                String target = voters == 0 ? "APPROVED" : "DATA_PROVIDER_REVIEW";
                jdbc.update("update ds_sandbox_approval set status=?,current_stage=?,version=version+1,reviewer='',review_comment='',approved_at=?,updated_at=? where id=? and status='REJECTED'", target, target, voters == 0 ? now() : "", now(), id);
            }
            case "CANCEL" -> {
                if (!isApplicant(approval)) {
                    throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "仅申请人可撤回申请");
                }
                if (!Set.of("DATA_PROVIDER_REVIEW", "APPROVED").contains(from)) throw new IllegalStateException("当前状态不可撤回");
                jdbc.update("update ds_sandbox_approval set status='CANCELLED',current_stage='CANCELLED',updated_at=? where id=? and status=?", now(), id, from);
            }
            case "RETRY" -> {
                boolean operator = gate.isAdminOrOperator(gate.currentUser(), string(approval.get("applicant_node_id")));
                if (!isApplicant(approval) && !operator) {
                    throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "仅申请人或运营方可重试");
                }
                if (!"FAILED".equals(from)) throw new IllegalStateException("只有执行失败申请可重试");
                jdbc.update("update ds_sandbox_approval set status='EXECUTING',current_stage='EXECUTING',executor=?,retry_count=0,last_error='',updated_at=? where id=? and status='FAILED'", operator(), now(), id);
            }
            default -> throw new IllegalArgumentException("不支持的审批动作: " + action);
        }
        String to = string(requireApproval(id).get("status"));
        history(id, action, from, to, comment);
        service.audit("AUDIT", "SANDBOX_APPROVAL_" + action, "SANDBOX_APPROVAL", id, comment, true);
        service.dispatchWebhooks("sandbox.approval." + action.toLowerCase(Locale.ROOT),
                Map.of("approvalId", id, "from", from, "to", to));
        if ("RETRY".equals(action)) {
            // 已认领为 EXECUTING，同步执行（结果由 executeOne 落库）
            executeOne(id);
        }
        publishSnapshot(id);
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
        applySyncedApprovals();
        for (Map<String, Object> row : jdbc.queryForList(
                "select id from ds_sandbox_approval where status='APPROVED' and deleted=0 "
                        + "and approval_type in " + EXECUTABLE_TYPES_SQL + " "
                        + "and (applicant_node_id=? or (coalesce(applicant_node_id,'')='' and owner_id=?)) "
                        + "order by approved_at asc limit 20", executorNodeId, executorNodeId)) {
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
            if (Set.of("CREATE", "DATA_CHANGE").contains(type)) {
                requireActiveProject(string(approval.get("project_id")),
                        "CREATE".equals(type) ? "创建沙箱" : "挂载数据");
            }
            if (!service.isKusciaEnabled() && ("CREATE".equals(type) || "SPEC_CHANGE".equals(type))) {
                throw new IllegalStateException("Kuscia 运行时未启用，无法执行沙箱拉起类申请");
            }
            boolean shouldComplete = switch (type) {
                case "CREATE" -> { execCreate(approval); yield true; }
                case "RENEW" -> { execRenew(approval); yield true; }
                case "SPEC_CHANGE" -> { execSpecChange(approval); yield true; }
                case "DATA_CHANGE" -> { execDataChange(approval); yield true; }
                case "CONFIG_CHANGE" -> { execConfigChange(approval); yield true; }
                case "RECYCLE" -> { execRecycle(approval); yield true; }
                case "ASSET_DELETE" -> execAssetDelete(approval);
                default -> throw new IllegalStateException("未知申请类型: " + type);
            };
            if (shouldComplete) {
                complete(id);
            }
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
        publishSnapshot(id);
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
            publishSnapshot(id);
        } else {
            jdbc.update("update ds_sandbox_approval set status='APPROVED',current_stage='APPROVED',updated_at=? where id=?", now(), id);
            history(id, "RETRY", "EXECUTING", "APPROVED", "自动重试：" + error);
            service.auditAs("AUDIT", "WARN", engineActor(), "SANDBOX_APPROVAL_RETRY", "SANDBOX_APPROVAL", id, error, true);
            publishSnapshot(id);
        }
    }

    /** JVM 崩溃/进程卡死的 EXECUTING 兜底：超过 10 分钟未更新 → 达上限 FAILED，否则回退 APPROVED 自动重试。 */
    public void reclaimStuckExecuting() {
        String threshold = LocalDateTime.now().minusMinutes(10).toString();
        for (Map<String, Object> row : jdbc.queryForList(
                "select id,retry_count from ds_sandbox_approval where status='EXECUTING' and updated_at<? and deleted=0 "
                        + "and approval_type in " + EXECUTABLE_TYPES_SQL, threshold)) {
            String id = string(row.get("id"));
            int retries = intValue(row.get("retry_count"), 0);
            if (retries >= maxRetries) {
                jdbc.update("update ds_sandbox_approval set status='FAILED',current_stage='FAILED',last_error=?,completed_at=?,updated_at=? where id=?",
                        "执行超时（10 分钟未完成）", now(), now(), id);
                history(id, "FAIL", "EXECUTING", "FAILED", "执行超时");
                service.auditAs("AUDIT", "ERROR", engineActor(), "SANDBOX_APPROVAL_STUCK", "SANDBOX_APPROVAL", id, "EXECUTING 超时转 FAILED", false);
                publishSnapshot(id);
            } else {
                jdbc.update("update ds_sandbox_approval set status='APPROVED',current_stage='APPROVED',updated_at=? where id=?", now(), id);
                history(id, "RETRY", "EXECUTING", "APPROVED", "卡死回退自动重试");
                service.auditAs("AUDIT", "WARN", engineActor(), "SANDBOX_APPROVAL_STUCK", "SANDBOX_APPROVAL", id, "EXECUTING 超时回退 APPROVED", true);
                publishSnapshot(id);
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
        req.put("createdBy", string(approval.get("submitter")));
        Map<String, Object> created = service.createSandbox(req);
        String id = string(created.get("id"));
        jdbc.update("update ds_sandbox_approval set sandbox_id=? where id=?", id, approval.get("id"));
        syncDatasetMounts(id, req);
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
        String expiresAt = required(payload, "expiresAt");
        service.sandboxAction(Map.of("id", sandboxId, "action", "RENEW", "expiresAt", expiresAt));
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
            String waitError = service.waitForKusciaJobDeletion(oldJob, Duration.ofSeconds(5));
            if (!waitError.isEmpty()) {
                throw new IllegalStateException("旧任务删除未完成: " + waitError);
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

    private void execDataChange(Map<String, Object> approval) {
        String sandboxId = string(approval.get("sandbox_id"));
        Map<String, Object> sbx = requireSandbox(sandboxId);
        recreateSandboxJob(sbx, () -> syncDatasetMounts(sandboxId, parsePayload(approval)), "DATA_CHANGE");
    }

    private void execConfigChange(Map<String, Object> approval) {
        String sandboxId = string(approval.get("sandbox_id"));
        Map<String, Object> sbx = requireSandbox(sandboxId);
        Map<String, Object> payload = parsePayload(approval);
        String imageId = value(payload, "imageId", string(sbx.get("image_id")));
        String networkPolicy = value(payload, "networkPolicy", string(sbx.get("network_policy"))).toUpperCase(Locale.ROOT);
        if (count("select count(1) from ds_sandbox_image where id=? and enabled=1", imageId) == 0) {
            throw new IllegalArgumentException("环境镜像不存在或未启用: " + imageId);
        }
        if (!NETWORK_POLICIES.contains(networkPolicy)) throw new IllegalArgumentException("不支持的网络策略: " + networkPolicy);
        recreateSandboxJob(sbx, () -> jdbc.update("update ds_sandbox set image_id=?,network_policy=?,updated_at=? where id=?", imageId, networkPolicy, now(), sandboxId), "CONFIG_CHANGE");
    }

    /** Delete only after every referenced project's request has reached unanimous approval. */
    private boolean execAssetDelete(Map<String, Object> approval) {
        String assetId = string(approval.get("sandbox_id"));
        long rejected = count("select count(1) from ds_sandbox_approval a join project p on p.project_id=a.project_id and p.status=1 and p.is_deleted=0 where a.approval_type='ASSET_DELETE' and a.sandbox_id=? and a.submitted_at=? and a.deleted=0 and a.status in ('REJECTED','CANCELLED','FAILED')",
                assetId, approval.get("submitted_at"));
        if (rejected > 0) {
            int changed = jdbc.update("update ds_sandbox_approval set status='REJECTED',current_stage='REJECTED',last_error=?,completed_at=?,updated_at=? where id=? and status='EXECUTING' and deleted=0",
                    "同批次项目审批未全部通过", now(), now(), approval.get("id"));
            if (changed == 1) {
                history(string(approval.get("id")), "GROUP_REJECT", "EXECUTING", "REJECTED", "同批次项目审批未全部通过");
            }
            return false;
        }
        long pending = count("select count(1) from ds_sandbox_approval a join project p on p.project_id=a.project_id and p.status=1 and p.is_deleted=0 where a.approval_type='ASSET_DELETE' and a.sandbox_id=? and a.submitted_at=? and a.deleted=0 and a.status not in ('APPROVED','EXECUTING','COMPLETED')",
                assetId, approval.get("submitted_at"));
        if (pending > 0) {
            jdbc.update("update ds_sandbox_approval set status='APPROVED',current_stage='APPROVED',last_error=?,updated_at=? where id=? and status='EXECUTING' and deleted=0",
                    "等待同批次项目审批", now(), approval.get("id"));
            return false;
        }
        List<Map<String, Object>> assets = jdbc.queryForList("select * from ds_data_asset where id=? and deleted=0", assetId);
        if (assets.isEmpty()) return true;
        Map<String, Object> asset = assets.get(0);
        String datatableId = string(asset.get("datatable_id"));
        Long children = jdbc.queryForObject("select count(1) from ds_data_asset where source_asset_id=? and deleted=0", Long.class, assetId);
        Long mounts = jdbc.queryForObject("select count(1) from ds_sandbox_dataset_mount where asset_id=? and deleted=0", Long.class, assetId);
        if (children != null && children > 0) {
            throw SecretpadException.of(DataErrorCode.DATA_ASSET_HAS_DERIVED_ASSET);
        }
        if (mounts != null && mounts > 0) {
            throw SecretpadException.of(DataErrorCode.DATA_ASSET_MOUNTED);
        }

        List<String> projects = jdbc.queryForList(
                "select distinct refs.project_id from (select project_id from ds_project_asset where asset_id=? and deleted=0 and coalesce(is_deleted,0)=0 union select project_id from project_datatable where datatable_id=? and is_deleted=0) refs join project p on p.project_id=refs.project_id and p.status=1 and p.is_deleted=0",
                String.class, assetId, datatableId);
        Set<String> approvedProjects = jdbc.queryForList(
                "select distinct a.project_id from ds_sandbox_approval a join project p on p.project_id=a.project_id and p.status=1 and p.is_deleted=0 where a.approval_type='ASSET_DELETE' and a.sandbox_id=? and a.submitted_at=? and a.deleted=0 and a.status in ('APPROVED','EXECUTING','COMPLETED')",
                String.class, assetId, approval.get("submitted_at")).stream().collect(Collectors.toSet());
        if (!projects.stream().collect(Collectors.toSet()).equals(approvedProjects)) {
            throw SecretpadException.of(DataErrorCode.DATA_ASSET_DELETE_CONFLICT);
        }
        for (String projectId : projects) {
            projectAssetRepository.findById(new ProjectAssetDO.UPK(projectId, assetId)).ifPresent(projectAsset -> {
                projectAsset.setIsDeleted(true);
                projectAsset.setGmtModified(LocalDateTime.now(java.time.ZoneOffset.UTC));
                projectAssetRepository.save(projectAsset);
            });
            for (ProjectDatatableDO datatable : projectDatatableRepository.findByDatableId(projectId, datatableId)) {
                datatable.setIsDeleted(true);
                datatable.setGmtModified(LocalDateTime.now(java.time.ZoneOffset.UTC));
                projectDatatableRepository.save(datatable);
            }
        }
        projectAssetRepository.flush();
        projectDatatableRepository.flush();
        assetStorage.delete(string(asset.get("storage_uri")));
        nodeDatasetStore.remove(assetId);
        int changed = jdbc.update("update ds_data_asset set deleted=1,status='DELETED',updated_at=? where id=? and deleted=0", now(), assetId);
        if (changed != 1) throw SecretpadException.of(DataErrorCode.DATA_ASSET_DELETE_CONFLICT);
        return true;
    }

    private void recreateSandboxJob(Map<String, Object> sandbox, Runnable mutation, String reason) {
        String sandboxId = string(sandbox.get("id"));
        if (!notBlank(sandboxId) || "DESTROYED".equals(string(sandbox.get("status")))) throw new IllegalStateException("沙箱不可变更");
        if (notBlank(string(sandbox.get("kuscia_job_id")))) {
            String oldJob = string(sandbox.get("kuscia_job_id"));
            String stopError = service.stopKuscia(sandbox, reason);
            if (!stopError.isEmpty()) throw new IllegalStateException("停止旧任务失败: " + stopError);
            String deleteError = service.deleteKuscia(sandbox);
            if (!deleteError.isEmpty()) throw new IllegalStateException("删除旧任务失败: " + deleteError);
            String waitError = service.waitForKusciaJobDeletion(oldJob, Duration.ofSeconds(5));
            if (!waitError.isEmpty()) throw new IllegalStateException("旧任务删除未完成: " + waitError);
        }
        mutation.run();
        jdbc.update("update ds_sandbox set kuscia_job_id='',endpoint='',kuscia_job_state='',status='STARTING',intent='START',last_error='',updated_at=? where id=?", now(), sandboxId);
        Map<String, Object> fresh = service.sandbox(sandboxId);
        String error = service.startKuscia(fresh);
        if (!error.isEmpty()) throw new IllegalStateException("重建沙箱任务失败: " + error);
    }

    private void syncDatasetMounts(String sandboxId, Map<String, Object> payload) {
        List<String> assetIds = stringList(payload.get("datasetAssetIds"));
        String sandboxNode = string(jdbc.queryForObject("select owner_id from ds_sandbox where id=?", String.class, sandboxId));
        String projectId = string(jdbc.queryForObject("select project_id from ds_sandbox where id=?", String.class, sandboxId));
        jdbc.update("update ds_sandbox_dataset_mount set deleted=1,status='DETACHED',updated_at=? where sandbox_id=? and deleted=0", now(), sandboxId);
        for (String assetId : assetIds) {
            Map<String, Object> asset = projectAsset(projectId, assetId);
            String mountId = "mnt-" + shortId();
            String provider = string(asset.get("provider_node_id"));
            String checksum = metadataChecksum(asset.get("metadata_json"));
            String stagingUri = string(asset.get("storage_uri"));
            if (!Objects.equals(provider, sandboxNode)) {
                // 授权自动同步：跨节点 PROCESSED 先在本地物化；本地权威库表作为 staging 源，不再 MinIO 加密快照
                assetSyncService.ensureSynced(projectId, assetId);
                String localTable = assetSyncService.localPhysicalTable(projectId, assetId);
                if (localTable != null) {
                    stagingUri = "node-data://" + localTable;
                } else {
                    stagingUri = assetStorage.encryptedSnapshot(stagingUri, "sandbox-staging/" + sandboxNode + "/" + sandboxId + "/" + assetId + "-v" + intValue(asset.get("version"), 1), checksum);
                }
            }
            String expiresAt = string(asset.get("control_valid_until"));
            if (!notBlank(expiresAt)) expiresAt = string(asset.get("valid_until"));
            jdbc.update("insert into ds_sandbox_dataset_mount(id,sandbox_id,asset_id,asset_version,provider_node_id,staging_uri,mount_path,checksum,status,expires_at,created_at,updated_at,deleted) values(?,?,?,?,?,?,?,?,?,?,?,?,0)",
                    mountId, sandboxId, assetId, intValue(asset.get("version"), 1), provider,
                    stagingUri, "/data/assets/" + assetId, checksum,
                    "READY", expiresAt, now(), now());
        }
        // 挂载即重建沙箱权威库（仅 PROCESSED 注入、RAW 禁入）；START 时亦会重建兜底
        try {
            sandboxDbService.rebuild(sandboxId);
        } catch (Exception e) {
            log.warn("沙箱 {} 权威库重建失败（不阻断启动，START 时重试）: {}", sandboxId, e.getMessage());
        }
    }

    /* ------------------------------- 内部工具 ------------------------------- */

    /**
     * Materialize project-scoped approval snapshots received through SecretPad's P2P data sync.
     * The JDBC approval tables remain the workflow store; this synchronized envelope makes the
     * same request, votes and history visible and actionable on every project participant.
     */
    /**
     * 将 P2P 同步的申请单快照应用到本节点（ModelApiApprovalService 亦复用）。
     */
    @SuppressWarnings("unchecked")
    public void applySyncedApprovals() {
        for (SandboxApprovalSyncDO sync : approvalSyncRepository.findAll()) {
            try {
                String approvalId = sync.getUpk() == null ? "" : sync.getUpk().getApprovalId();
                int snapshotHash = sync.getSnapshotJson().hashCode();
                if (Objects.equals(appliedSnapshotHashes.get(approvalId), snapshotHash)) continue;
                Map<String, Object> snapshot = objectMapper.readValue(sync.getSnapshotJson(), Map.class);
                Map<String, Object> approval = castMap((Map<?, ?>) snapshot.get("approval"));
                String id = string(approval.get("id"));
                if (!notBlank(id)) continue;
                List<Map<String, Object>> existing = jdbc.queryForList(
                        "select updated_at from ds_sandbox_approval where id=? and deleted=0", id);
                if (!existing.isEmpty()
                        && string(existing.get(0).get("updated_at")).compareTo(string(approval.get("updated_at"))) > 0) {
                    continue;
                }
                upsertSyncedApproval(approval);
                jdbc.update("delete from ds_sandbox_approval_vote where approval_id=?", id);
                for (Map<String, Object> vote : mapList(snapshot.get("votes"))) {
                    jdbc.update("insert into ds_sandbox_approval_vote(approval_id,voter_node_id,status,voter,comment,voted_at) values(?,?,?,?,?,?)",
                            id, vote.get("voter_node_id"), vote.get("status"), value(vote, "voter", ""),
                            value(vote, "comment", ""), value(vote, "voted_at", ""));
                }
                jdbc.update("delete from ds_sandbox_approval_history where approval_id=?", id);
                for (Map<String, Object> history : mapList(snapshot.get("history"))) {
                    jdbc.update("insert into ds_sandbox_approval_history(approval_id,action,from_status,to_status,operator,comment,created_at) values(?,?,?,?,?,?,?)",
                            id, history.get("action"), value(history, "from_status", ""), history.get("to_status"),
                            history.get("operator"), value(history, "comment", ""), history.get("created_at"));
                }
                appliedSnapshotHashes.put(id, snapshotHash);
            } catch (Exception e) {
                log.warn("Ignore malformed sandbox approval sync snapshot {}", sync.getUpk(), e);
            }
        }
    }

    private void upsertSyncedApproval(Map<String, Object> approval) {
        String id = string(approval.get("id"));
        int changed = jdbc.update("update ds_sandbox_approval set approval_type=?,sandbox_id=?,owner_id=?,submitter=?,payload_json=?,status=?,current_stage=?,version=?,executor=?,reviewer=?,review_comment=?,last_error=?,retry_count=?,submitted_at=?,approved_at=?,completed_at=?,created_at=?,updated_at=?,deleted=?,project_id=?,applicant_node_id=?,project_snapshot_at=? where id=?",
                approval.get("approval_type"), value(approval, "sandbox_id", ""), approval.get("owner_id"), approval.get("submitter"),
                value(approval, "payload_json", "{}"), approval.get("status"), approval.get("current_stage"), intValue(approval.get("version"), 1),
                value(approval, "executor", ""), value(approval, "reviewer", ""), value(approval, "review_comment", ""), value(approval, "last_error", ""),
                intValue(approval.get("retry_count"), 0), approval.get("submitted_at"), value(approval, "approved_at", ""), value(approval, "completed_at", ""),
                approval.get("created_at"), approval.get("updated_at"), intValue(approval.get("deleted"), 0), approval.get("project_id"),
                approval.get("applicant_node_id"), value(approval, "project_snapshot_at", ""), id);
        if (changed == 0) {
            jdbc.update("insert into ds_sandbox_approval(id,approval_type,sandbox_id,owner_id,submitter,payload_json,status,current_stage,version,executor,reviewer,review_comment,last_error,retry_count,submitted_at,approved_at,completed_at,created_at,updated_at,deleted,project_id,applicant_node_id,project_snapshot_at) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    id, approval.get("approval_type"), value(approval, "sandbox_id", ""), approval.get("owner_id"), approval.get("submitter"),
                    value(approval, "payload_json", "{}"), approval.get("status"), approval.get("current_stage"), intValue(approval.get("version"), 1),
                    value(approval, "executor", ""), value(approval, "reviewer", ""), value(approval, "review_comment", ""), value(approval, "last_error", ""),
                    intValue(approval.get("retry_count"), 0), approval.get("submitted_at"), value(approval, "approved_at", ""), value(approval, "completed_at", ""),
                    approval.get("created_at"), approval.get("updated_at"), intValue(approval.get("deleted"), 0), approval.get("project_id"),
                    approval.get("applicant_node_id"), value(approval, "project_snapshot_at", ""));
        }
    }

    private void publishSnapshot(String approvalId) {
        Map<String, Object> approval = requireApproval(approvalId);
        String projectId = string(approval.get("project_id"));
        if (!notBlank(projectId)) return;
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("approval", approval);
        snapshot.put("votes", jdbc.queryForList("select * from ds_sandbox_approval_vote where approval_id=? order by voter_node_id", approvalId));
        snapshot.put("history", jdbc.queryForList("select * from ds_sandbox_approval_history where approval_id=? order by id", approvalId));
        SandboxApprovalSyncDO.UPK upk = new SandboxApprovalSyncDO.UPK(approvalId);
        SandboxApprovalSyncDO sync = approvalSyncRepository.findById(upk).orElseGet(SandboxApprovalSyncDO::new);
        sync.setUpk(upk);
        sync.setProjectId(projectId);
        sync.setApplicantNodeId(string(approval.get("applicant_node_id")));
        String snapshotJson = json(snapshot);
        sync.setSnapshotJson(snapshotJson);
        sync.setGmtModified(LocalDateTime.now(java.time.ZoneOffset.UTC).truncatedTo(java.time.temporal.ChronoUnit.SECONDS));
        approvalSyncRepository.saveAndFlush(sync);
        appliedSnapshotHashes.put(approvalId, snapshotJson.hashCode());
    }

    private List<Map<String, Object>> mapList(Object value) {
        if (!(value instanceof Iterable<?> iterable)) return List.of();
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object item : iterable) {
            if (item instanceof Map<?, ?> map) result.add(castMap(map));
        }
        return result;
    }

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
        LocalDateTime expiresAt;
        try {
            expiresAt = LocalDateTime.parse(required(request, "expiresAt"));
        } catch (java.time.format.DateTimeParseException e) {
            throw new IllegalArgumentException("expiresAt 必须是有效的日期时间");
        }
        LocalDateTime current = LocalDateTime.now();
        if (!expiresAt.isAfter(current)) {
            throw new IllegalArgumentException("到期时间必须晚于当前时间");
        }
        if (expiresAt.isAfter(current.plusDays(365))) {
            throw new IllegalArgumentException("到期时间不能超过一年");
        }
    }

    private void requireProjectMembership(String projectId, String memberNodeId) {
        if (!projectExists(projectId)) {
            throw new IllegalArgumentException("项目不存在: " + projectId);
        }
        if (count("select count(1) from project_node where project_id=? and node_id=? and is_deleted=0", projectId, memberNodeId) == 0) {
            throw SecretpadException.of(ProjectErrorCode.PROJECT_NODE_NOT_EXISTS);
        }
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

    private boolean projectExists(String projectId) {
        return notBlank(projectId)
                && count("select count(1) from project where project_id=? and is_deleted=0", projectId) > 0;
    }

    private List<String> projectVoters(String projectId, String applicantNodeId) {
        return jdbc.queryForList("select distinct node_id from project_node where project_id=? and node_id<>? and is_deleted=0 order by node_id", String.class, projectId, applicantNodeId);
    }

    private String projectSnapshot(String projectId) {
        return string(jdbc.queryForObject("select gmt_modified from project where project_id=? and is_deleted=0", Object.class, projectId));
    }

    private void validateDatasetAssets(String projectId, Object selected) {
        for (String assetId : stringList(selected)) {
            Map<String, Object> asset = projectAsset(projectId, assetId);
            if (!"ACTIVE".equals(string(asset.get("status")))) {
                throw new IllegalArgumentException("数据不存在或不可用: " + assetId);
            }
            if (!"PROCESSED".equals(string(asset.get("data_stage")))) throw new IllegalArgumentException("沙箱只能挂载抽样脱敏后的数据: " + assetId);
            if (count("select count(1) from ds_project_asset where project_id=? and asset_id=? and deleted=0", projectId, assetId) == 0) {
                throw new IllegalArgumentException("数据未挂载到所选项目: " + assetId);
            }
            if (!AssetTimeWindow.within(asset.get("control_valid_from"),
                    asset.get("control_valid_until"))) {
                throw new IllegalArgumentException("数据不在使用有效期内: " + assetId);
            }
            if (!AssetTimeWindow.within(asset.get("access_start"), asset.get("access_end"))) {
                throw new IllegalArgumentException("数据不在访问有效期内: " + assetId);
            }
            // 授权自动同步：挂载前置校验即触发跨节点 PROCESSED 物理拉取（幂等；本节点资产为无操作）
            assetSyncService.ensureSynced(projectId, assetId);
        }
    }

    /** 申请详情保存可读名称，执行仍使用 datasetAssetIds 作为稳定标识。 */
    private List<String> datasetAssetNames(String projectId, Object selected, Object submittedNames) {
        List<String> assetIds = stringList(selected);
        List<String> fallbackNames = stringList(submittedNames);
        List<String> names = new ArrayList<>();
        for (int index = 0; index < assetIds.size(); index++) {
            String assetId = assetIds.get(index);
            String name = string(projectAsset(projectId, assetId).get("name"));
            String submittedName = index < fallbackNames.size() ? fallbackNames.get(index) : "";
            names.add(notBlank(name) && !assetId.equals(name)
                    ? name : notBlank(submittedName) ? submittedName : assetId);
        }
        return names;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> projectAsset(String projectId, String assetId) {
        List<Map<String, Object>> local = jdbc.queryForList(
                "select * from ds_data_asset where id=? and deleted=0", assetId);
        if (!local.isEmpty()) {
            Map<String, Object> asset = new LinkedHashMap<>(local.get(0));
            decorateUsageControl(asset, assetId);
            return asset;
        }
        List<Map<String, Object>> shared = jdbc.queryForList(
                "select asset_json,provider_node_id from ds_project_asset where project_id=? and asset_id=? and deleted=0 and coalesce(is_deleted,0)=0",
                projectId, assetId);
        if (shared.isEmpty()) throw new IllegalArgumentException("数据不存在: " + assetId);
        try {
            Map<String, Object> snapshot = objectMapper.readValue(string(shared.get(0).get("asset_json")), Map.class);
            snapshot.put("provider_node_id", shared.get(0).get("provider_node_id"));
            decorateUsageControl(snapshot, assetId);
            return snapshot;
        } catch (Exception e) {
            throw new IllegalStateException("项目数据元数据损坏: " + assetId, e);
        }
    }

    /** 本节点存在权威使用控制时覆盖项目附件快照。 */
    private void decorateUsageControl(Map<String, Object> asset, String assetId) {
        List<Map<String, Object>> controls = jdbc.queryForList(
                "select valid_from,valid_until,access_start,access_end "
                        + "from ds_asset_usage_control where asset_id=?", assetId);
        if (controls.isEmpty()) return;
        Map<String, Object> control = controls.get(0);
        asset.put("control_valid_from", control.get("valid_from"));
        asset.put("control_valid_until", control.get("valid_until"));
        asset.put("access_start", control.get("access_start"));
        asset.put("access_end", control.get("access_end"));
    }

    private void assertSandboxCreator(String sandboxId) {
        Map<String, Object> sandbox = requireRow("select created_by,owner_id from ds_sandbox where id=? and deleted=0", sandboxId);
        if (!Objects.equals(operator(), string(sandbox.get("created_by"))) || !gate.matchesCurrentNode(string(sandbox.get("owner_id")))) {
            throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "只有沙箱创建人可以提交变更申请");
        }
    }

    private void assertApprovalVisible(Map<String, Object> approval) {
        String currentNode = gate.effectiveOwner();
        boolean voter = count("select count(1) from ds_sandbox_approval_vote where approval_id=? and voter_node_id=?", approval.get("id"), currentNode) > 0;
        String applicantNodeId = string(approval.get("applicant_node_id"));
        boolean operator = gate.isAdminOrOperator(gate.currentUser(), applicantNodeId);
        if (!isApplicant(approval) && !voter && !operator) {
            throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "无权查看该项目申请");
        }
    }

    private boolean isApplicant(Map<String, Object> approval) {
        String applicantNodeId = string(approval.get("applicant_node_id"));
        boolean sameUser = Objects.equals(operator(), string(approval.get("submitter")));
        // Legacy non-project approvals did not persist an applicant node, so retain their
        // username-based behavior. Project approvals must also match the originating node.
        return sameUser && (!notBlank(applicantNodeId) || gate.matchesCurrentNode(applicantNodeId));
    }

    private void vote(String approvalId, String action, String comment) {
        String voterNode = gate.effectiveOwner();
        String voteStatus = "APPROVE".equals(action) ? "APPROVED" : "REJECTED";
        int changed = jdbc.update("update ds_sandbox_approval_vote set status=?,voter=?,comment=?,voted_at=? where approval_id=? and voter_node_id=? and status='PENDING'",
                voteStatus, operator(), comment, now(), approvalId, voterNode);
        if (changed != 1) throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "当前节点无待处理投票或已经完成投票");
        if ("REJECTED".equals(voteStatus)) {
            jdbc.update("update ds_sandbox_approval set status='REJECTED',current_stage='REJECTED',reviewer=?,review_comment=?,updated_at=? where id=? and status='DATA_PROVIDER_REVIEW'", operator(), comment, now(), approvalId);
        } else if (count("select count(1) from ds_sandbox_approval_vote where approval_id=? and status='PENDING'", approvalId) == 0) {
            jdbc.update("update ds_sandbox_approval set status='APPROVED',current_stage='APPROVED',reviewer=?,review_comment=?,approved_at=?,updated_at=? where id=? and status='DATA_PROVIDER_REVIEW'", operator(), comment, now(), now(), approvalId);
        }
    }

    private List<String> stringList(Object value) {
        if (value == null) return List.of();
        if (value instanceof Iterable<?> iterable) {
            List<String> result = new ArrayList<>();
            iterable.forEach(item -> { if (notBlank(string(item))) result.add(string(item)); });
            return result.stream().distinct().toList();
        }
        String raw = string(value);
        if (!notBlank(raw)) return List.of();
        return java.util.Arrays.stream(raw.split(",")).map(String::trim).filter(SandboxApprovalService::notBlank).distinct().toList();
    }

    private String metadataChecksum(Object metadataJson) {
        try {
            Object value = objectMapper.readValue(string(metadataJson), Map.class).get("sha256");
            return string(value);
        } catch (Exception ignored) {
            return "";
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
            return new LinkedHashMap<>(jdbc.queryForMap(
                    "select a.*,coalesce(p.name,a.project_id) project_name from ds_sandbox_approval a "
                            + "left join project p on p.project_id=a.project_id and p.is_deleted=0 "
                            + "where a.id=? and a.deleted=0",
                    id));
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
