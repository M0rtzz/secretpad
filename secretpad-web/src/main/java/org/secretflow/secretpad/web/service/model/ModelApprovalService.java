/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.service.model;

import org.secretflow.secretpad.common.dto.UserContextDTO;
import org.secretflow.secretpad.common.util.UserContext;
import org.secretflow.secretpad.web.service.DataSandboxMvpService;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Z-06 模型审批服务：模型注册（绑定 Z-05 制品/版本/项目）+ 审批单（扩展 V6 ds_model_approval，
 * 绑定制品/版本）+ 两级审批 + 强制测试门禁 + 发布。
 *
 * <p>复用现有 {@code MODEL_REVIEW → RESOURCE_REVIEW → APPROVED → PUBLISHED} 状态流与
 * {@code ds_model_approval_history}；新流程行 {@code model_id = ds_model.id（dm-）}，与旧流程行
 * （legacy modelId 字符串）并存，legacy {@code DataSandboxMvpService.submitModel/approvalAction/
 * assertModelApproved} 保持兼容不动。</p>
 *
 * <p>审批权限：模型创建人 / 审批提交人 / 平台管理员可操作（对 Z-05 创建人独享的明确放宽，
 * 报告中注明）。资源审批通过（APPROVED）前强制 ≥1 次成功的模型测试且保存评估指标。</p>
 */
@Slf4j
@Service
public class ModelApprovalService {

    private static final Set<String> MODEL_TYPES = Set.of("JAR", "PYTHON");
    /** 自动注册（画布训练 / 制品一键发布 API）放行全部可执行制品类型，SQL/函数走进程内/包装器执行。 */
    private static final Set<String> AUTO_MODEL_TYPES = Set.of("JAR", "PYTHON", "SQL", "FUNCTION");
    private static final Set<String> NON_TERMINAL_STATUSES = Set.of("DRAFT", "APPROVING", "APPROVED", "PUBLISHED");
    private static final Set<String> EDITABLE_STATUSES = Set.of("DRAFT", "REJECTED");
    private static final Set<String> APPROVAL_ACTIONS = Set.of("APPROVE", "REJECT", "RESUBMIT", "PUBLISH");
    private static final Set<String> TESTABLE_APPROVAL_STATUSES = Set.of("MODEL_REVIEW", "RESOURCE_REVIEW", "APPROVED");

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final DataSandboxMvpService mvp;
    private final ModelTestService modelTestService;

    @Value("${secretpad.node-id:kuscia-system}")
    private String nodeId;

    public ModelApprovalService(
            @Qualifier("jdbcTemplate") JdbcTemplate jdbc,
            ObjectMapper objectMapper,
            DataSandboxMvpService mvp,
            ModelTestService modelTestService) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.mvp = mvp;
        this.modelTestService = modelTestService;
    }

    /* ============================== 模型注册 ============================== */

    /**
     * 注册模型：校验制品为 JAR/PYTHON（SQL 非模型，注册即拒）+ 版本存在 + 项目存在；
     * 同项目同制品存在非终结态模型 → {@code MODEL_ALREADY_EXISTS}，否则 version 自增。
     */
    @Transactional
    public Map<String, Object> registerModel(Map<String, Object> request) {
        String name = required(request, "name");
        String projectId = required(request, "projectId");
        String artifactId = required(request, "artifactId");
        String artifactVersionId = required(request, "artifactVersionId");
        String sandboxId = string(request.get("sandboxId"));
        String description = string(request.get("description"));

        Map<String, Object> artifact = requireArtifact(artifactId);
        String artifactType = string(artifact.get("type"));
        if (!MODEL_TYPES.contains(artifactType)) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID
                    + ": 仅 JAR/PYTHON 制品可作为模型（当前 " + artifactType + "）");
        }
        Map<String, Object> version = requireVersion(artifactId, artifactVersionId);
        requireProject(projectId);
        if (notBlank(sandboxId)) {
            Map<String, Object> sandbox = requireRow("select * from ds_sandbox where id=? and project_id=? and deleted=0", sandboxId, projectId);
            if (!Objects.equals(actor(), string(sandbox.get("created_by")))) {
                throw new IllegalArgumentException(ModelErrors.MODEL_NO_PERMISSION + ": 沙箱仅创建人可注册算法");
            }
            if (!sandboxId.equals(string(artifact.get("sandbox_id")))) {
                throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": 制品不属于当前沙箱");
            }
        }

        Long duplicate = count("select count(1) from ds_model where project_id=? and artifact_id=? and deleted=0 and status in (?,?,?,?)",
                projectId, artifactId, "DRAFT", "APPROVING", "APPROVED", "PUBLISHED");
        if (duplicate > 0) {
            throw new IllegalArgumentException(ModelErrors.MODEL_ALREADY_EXISTS
                    + ": 同项目同制品已存在未终结模型，不可重复注册");
        }
        Integer maxVersion = jdbc.queryForObject(
                "select max(version) from ds_model where project_id=? and artifact_id=? and deleted=0",
                Integer.class, projectId, artifactId);
        int modelVersion = (maxVersion == null ? 0 : maxVersion) + 1;
        String id = "dm-" + shortId();
        String createdBy = actor();
        String createdByOwner = currentOwner();
        String now = now();
        jdbc.update("insert into ds_model(id,name,description,project_id,artifact_id,artifact_version_id,node_id,version,status,"
                        + "created_by,created_by_owner,created_at,updated_at,approved_at,published_at,deleted,sandbox_id,input_schema,output_schema)"
                        + " values(?,?,?,?,?,?,?,?,?,?,?,?,?,'','',0,?,?,?)",
                id, name, description, projectId, artifactId, artifactVersionId, nodeIdOf(projectId, createdByOwner),
                modelVersion, "DRAFT", createdBy, createdByOwner, now, now, sandboxId,
                string(request.getOrDefault("inputSchema", "[]")), string(request.getOrDefault("outputSchema", "[]")));
        audit("MODEL_REGISTER", "MODEL", id,
                "artifact=" + artifactId + " v" + version.get("version") + " project=" + projectId, true);
        dispatch("model.registered", Map.of("id", id, "name", name, "artifactId", artifactId, "version", modelVersion));
        return modelDetail(id);
    }

    /**
     * 画布训练 / 制品一键发布用：自动注册模型且直接 APPROVED（绕过两段审批；评估由画布内
     * ml.binary_classification / ml.regression_evaluation 等节点承担）。
     *
     * <p>幂等：同项目同制品同版本已存在 APPROVED/PUBLISHED 模型时直接复用——画布重复训练/重复发布
     * 不重复建模型，仅返回既有模型（调用方需注意此时不轮换 API 凭证）。</p>
     */
    @Transactional
    public Map<String, Object> registerModelAutoApproved(String name, String projectId, String artifactId,
            String artifactVersionId, String sandboxId, String description) {
        Map<String, Object> artifact = requireArtifact(artifactId);
        String artifactType = string(artifact.get("type"));
        if (!AUTO_MODEL_TYPES.contains(artifactType)) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID
                    + ": 仅 JAR/PYTHON/SQL/FUNCTION 制品可作为自动注册模型（当前 " + artifactType + "）");
        }
        requireVersion(artifactId, artifactVersionId);
        requireProject(projectId);
        if (notBlank(sandboxId)) {
            Map<String, Object> sandbox = requireRow("select * from ds_sandbox where id=? and project_id=? and deleted=0", sandboxId, projectId);
            if (!Objects.equals(actor(), string(sandbox.get("created_by")))) {
                throw new IllegalArgumentException(ModelErrors.MODEL_NO_PERMISSION + ": 沙箱仅创建人可注册算法");
            }
            if (!sandboxId.equals(string(artifact.get("sandbox_id")))) {
                throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": 制品不属于当前沙箱");
            }
        }
        List<Map<String, Object>> existing = jdbc.queryForList(
                "select * from ds_model where project_id=? and artifact_id=? and artifact_version_id=? and deleted=0 and status in ('APPROVED','PUBLISHED') order by version desc limit 1",
                projectId, artifactId, artifactVersionId);
        if (!existing.isEmpty()) {
            return enrichModel(new LinkedHashMap<>(existing.get(0)));
        }
        Integer maxVersion = jdbc.queryForObject(
                "select max(version) from ds_model where project_id=? and artifact_id=? and deleted=0",
                Integer.class, projectId, artifactId);
        int modelVersion = (maxVersion == null ? 0 : maxVersion) + 1;
        String id = "dm-" + shortId();
        String createdBy = actor();
        String createdByOwner = currentOwner();
        String now = now();
        jdbc.update("insert into ds_model(id,name,description,project_id,artifact_id,artifact_version_id,node_id,version,status,"
                        + "created_by,created_by_owner,created_at,updated_at,approved_at,published_at,deleted,sandbox_id,input_schema,output_schema)"
                        + " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?,?,?)",
                id, name, description, projectId, artifactId, artifactVersionId, nodeIdOf(projectId, createdByOwner),
                modelVersion, "APPROVED", createdBy, createdByOwner, now, now, now, "", sandboxId, "[]", "[]");
        audit("MODEL_AUTO_APPROVED", "MODEL", id,
                "artifact=" + artifactId + " v" + artifactVersionId + " project=" + projectId, true);
        dispatch("model.registered", Map.of("id", id, "name", name, "artifactId", artifactId,
                "version", modelVersion, "autoApproved", true));
        return modelDetail(id);
    }

    @Transactional
    public Map<String, Object> updateModel(String id, String name, String description) {
        Map<String, Object> model = requireModel(id);
        requireCreator(model);
        String status = string(model.get("status"));
        if (!EDITABLE_STATUSES.contains(status)) {
            throw new IllegalArgumentException(ModelErrors.MODEL_STATE_CONFLICT + ": 仅 DRAFT/REJECTED 模型可编辑（当前 " + status + "）");
        }
        jdbc.update("update ds_model set name=?,description=?,updated_at=? where id=? and deleted=0",
                notBlank(name) ? name : string(model.get("name")),
                description == null ? string(model.get("description")) : description, now(), id);
        audit("MODEL_UPDATE", "MODEL", id, "", true);
        dispatch("model.updated", Map.of("id", id));
        return modelDetail(id);
    }

    @Transactional
    public void deleteModel(String id) {
        Map<String, Object> model = requireModel(id);
        requireCreator(model);
        String status = string(model.get("status"));
        if (!Set.of("DRAFT", "REJECTED", "OFFLINE").contains(status)) {
            throw new IllegalArgumentException(ModelErrors.MODEL_STATE_CONFLICT + ": 仅 DRAFT/REJECTED/OFFLINE 模型可删除（当前 " + status + "）");
        }
        jdbc.update("update ds_model set deleted=1,updated_at=? where id=?", now(), id);
        audit("MODEL_DELETE", "MODEL", id, "", true);
        dispatch("model.deleted", Map.of("id", id));
    }

    public List<Map<String, Object>> listModels(String status, String keyword, String sandboxId) {
        StringBuilder sql = new StringBuilder(
                "select m.*, a.name artifact_name, a.type artifact_type, v.version artifact_version_no "
                        + "from ds_model m left join ds_dev_artifact a on a.id=m.artifact_id "
                        + "left join ds_dev_artifact_version v on v.id=m.artifact_version_id where m.deleted=0");
        List<Object> args = new ArrayList<>();
        if (notBlank(sandboxId)) {
            sql.append(" and m.sandbox_id=?");
            args.add(sandboxId);
        }
        if (notBlank(status)) {
            sql.append(" and m.status=?");
            args.add(status.trim().toUpperCase(Locale.ROOT));
        }
        if (notBlank(keyword)) {
            sql.append(" and (m.name like ? or m.id like ?)");
            String like = "%" + keyword.trim() + "%";
            args.add(like);
            args.add(like);
        }
        sql.append(" order by m.updated_at desc limit 500");
        List<Map<String, Object>> rows = jdbc.queryForList(sql.toString(), args.toArray());
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            result.add(enrichModel(row));
        }
        return result;
    }

    public Map<String, Object> modelDetail(String id) {
        Map<String, Object> model = requireModel(id);
        // 惰性收官该模型下 RUNNING 的测试（调度器只收官 ds_dev_task，测试记录读时收官）
        modelTestService.finalizeAllForModel(id);
        return enrichModel(model);
    }

    /** 列表/详情富化：附加当前审批单 + 测试数 + API 数。 */
    private Map<String, Object> enrichModel(Map<String, Object> model) {
        Map<String, Object> result = new LinkedHashMap<>(model);
        String id = string(model.get("id"));
        List<Map<String, Object>> approvals = jdbc.queryForList(
                "select * from ds_model_approval where model_id=? order by submitted_at desc limit 1", id);
        result.put("currentApproval", approvals.isEmpty() ? null : approvals.get(0));
        result.put("testCount", count("select count(1) from ds_model_test where model_id=? and deleted=0", id));
        result.put("apiCount", count("select count(1) from ds_model_api where model_id=? and deleted=0", id));
        long canvasModelCount = count(
                "select count(1) from ds_compute_canvas_model where model_id=? and status='READY' and deleted=0", id);
        result.put("canvasModelSaved", canvasModelCount > 0);
        result.put("canvasModelCount", canvasModelCount);
        return result;
    }

    /* ============================== 审批 ============================== */

    /** 提交审批：模型 DRAFT/REJECTED → APPROVING；写审批单（绑定制品/版本/项目）+ 历史。 */
    @Transactional
    public Map<String, Object> submitApproval(String modelId, String comment) {
        Map<String, Object> model = requireModel(modelId);
        String modelStatus = string(model.get("status"));
        if (!Set.of("DRAFT", "REJECTED").contains(modelStatus)) {
            throw new IllegalArgumentException(ModelErrors.MODEL_STATE_CONFLICT + ": 仅 DRAFT/REJECTED 模型可提交审批（当前 " + modelStatus + "）");
        }
        Long active = count("select count(1) from ds_model_approval where model_id=? and status in (?,?,?)",
                modelId, "MODEL_REVIEW", "RESOURCE_REVIEW", "APPROVED");
        if (active > 0) {
            throw new IllegalArgumentException(ModelErrors.MODEL_STATE_CONFLICT + ": 已存在进行中的审批单");
        }
        String id = "apr-" + shortId();
        String now = now();
        jdbc.update("insert into ds_model_approval(id,model_id,model_name,project_id,version,status,current_stage,description,"
                        + "submitter,submitted_at,updated_at,artifact_id,artifact_version_id,test_evidence)"
                        + " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                id, modelId, string(model.get("name")), string(model.get("project_id")), intValue(model.get("version"), 1),
                "MODEL_REVIEW", "MODEL_REVIEW", string(model.get("description")), actor(), now, now,
                string(model.get("artifact_id")), string(model.get("artifact_version_id")), "");
        approvalHistory(id, "SUBMIT", "", "MODEL_REVIEW", comment == null ? "" : comment);
        jdbc.update("update ds_model set status='APPROVING',updated_at=? where id=? and deleted=0", now, modelId);
        audit("MODEL_SUBMIT", "MODEL_APPROVAL", id, "model=" + modelId, true);
        dispatch("model.submitted", Map.of("approvalId", id, "modelId", modelId));
        return approvalDetail(id);
    }

    /**
     * 审批动作：APPROVE（两段）/REJECT/RESUBMIT/PUBLISH。
     * <b>RESOURCE_REVIEW → APPROVED 前强制测试门禁</b>：先收官该审批下所有 RUNNING 测试，
     * 再统计成功且有指标的测试 ≥1，否则 {@code MODEL_TEST_REQUIRED}。
     */
    @Transactional
    public Map<String, Object> approvalAction(String id, String action, String comment) {
        String act = requiredAction(action);
        Map<String, Object> approval = requireApproval(id);
        String from = string(approval.get("status"));
        if (!APPROVAL_ACTIONS.contains(act)) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": action 必须是 APPROVE/REJECT/RESUBMIT/PUBLISH");
        }
        String modelId = string(approval.get("model_id"));
        Map<String, Object> model = requireModel(modelId);
        requireApprovalActor(model, approval);
        ModelApprovalStateMachine.Action enumAction = ModelApprovalStateMachine.Action.valueOf(act);
        ModelApprovalStateMachine.Transition transition;
        try {
            transition = ModelApprovalStateMachine.next(from, enumAction);
        } catch (IllegalStateException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
        String to = transition.to();
        String stage = transition.stage();

        if ("APPROVE".equals(act) && "APPROVED".equals(to)) {
            modelTestService.finalizeAllForApproval(id);
            Long succeeded = count("select count(1) from ds_model_test where approval_id=? and status='SUCCEEDED' and metrics not in ('','{}')",
                    id);
            if (succeeded == 0) {
                throw new IllegalArgumentException(ModelErrors.MODEL_TEST_REQUIRED
                        + ": 审批通过前需至少一次成功的模型测试并保存评估指标");
            }
        }
        String now = now();
        int versionBump = transition.versionBump() ? 1 : 0;
        jdbc.update("update ds_model_approval set status=?,current_stage=?,version=version+?,reviewer=?,review_comment=?,updated_at=?,"
                        + "published_at=case when ?='PUBLISHED' then ? else published_at end where id=?",
                to, stage, versionBump, actor(), comment == null ? "" : comment, now, to, now, id);
        approvalHistory(id, act, from, to, comment == null ? "" : comment);
        updateModelStatusOnApproval(modelId, act, to, now);
        audit("MODEL_" + act, "MODEL_APPROVAL", id, "from=" + from + " to=" + to + (versionBump == 1 ? " version+1" : ""), true);
        dispatch("model." + act.toLowerCase(Locale.ROOT), Map.of("approvalId", id, "from", from, "to", to, "modelId", modelId));
        return approvalDetail(id);
    }

    /** 审批动作 → ds_model 状态联动：提交保持 APPROVING；终 APPROVED → APPROVED+approved_at；REJECT → REJECTED；PUBLISH → PUBLISHED+published_at。 */
    private void updateModelStatusOnApproval(String modelId, String action, String to, String now) {
        switch (action) {
            case "APPROVE" -> {
                if ("APPROVED".equals(to)) {
                    jdbc.update("update ds_model set status='APPROVED',approved_at=?,updated_at=? where id=? and deleted=0", now, now, modelId);
                }
            }
            case "REJECT" -> jdbc.update("update ds_model set status='REJECTED',updated_at=? where id=? and deleted=0", now, modelId);
            case "RESUBMIT" -> jdbc.update("update ds_model set status='APPROVING',updated_at=? where id=? and deleted=0", now, modelId);
            case "PUBLISH" -> jdbc.update("update ds_model set status='PUBLISHED',published_at=?,updated_at=? where id=? and deleted=0", now, now, modelId);
            default -> {
            }
        }
    }

    public List<Map<String, Object>> listApprovals(String status, String keyword) {
        StringBuilder sql = new StringBuilder(
                "select a.*, m.name model_display_name, a2.name artifact_name from ds_model_approval a "
                        + "left join ds_model m on m.id=a.model_id "
                        + "left join ds_dev_artifact a2 on a2.id=a.artifact_id where 1=1");
        List<Object> args = new ArrayList<>();
        if (notBlank(status)) {
            sql.append(" and a.status=?");
            args.add(status.trim().toUpperCase(Locale.ROOT));
        }
        if (notBlank(keyword)) {
            sql.append(" and (lower(a.model_name) like ? or lower(a.model_id) like ? or lower(a.id) like ?)");
            String like = "%" + keyword.toLowerCase(Locale.ROOT) + "%";
            args.add(like);
            args.add(like);
            args.add(like);
        }
        sql.append(" order by a.updated_at desc limit 500");
        return jdbc.queryForList(sql.toString(), args.toArray());
    }

    public Map<String, Object> approvalDetail(String id) {
        Map<String, Object> approval = requireApproval(id);
        Map<String, Object> result = new LinkedHashMap<>(approval);
        result.put("history", approvalHistory(id));
        String modelId = string(approval.get("model_id"));
        try {
            result.put("model", modelDetail(modelId));
        } catch (IllegalArgumentException e) {
            result.put("model", null);
        }
        List<Map<String, Object>> tests = jdbc.queryForList(
                "select * from ds_model_test where approval_id=? and deleted=0 order by created_at desc", id);
        List<Map<String, Object>> enrichedTests = new ArrayList<>();
        for (Map<String, Object> test : tests) {
            enrichedTests.add(modelTestService.finalizeIfNeededAndGet(string(test.get("id"))));
        }
        result.put("tests", enrichedTests);
        return result;
    }

    public List<Map<String, Object>> approvalHistory(String id) {
        return jdbc.queryForList("select * from ds_model_approval_history where approval_id=? order by id desc", id);
    }

    /** 供 ModelTestService 查询当前进行中审批单（MODEL_REVIEW/RESOURCE_REVIEW/APPROVED）。 */
    public Map<String, Object> currentTestableApproval(String modelId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select * from ds_model_approval where model_id=? and status in (?,?,?) order by submitted_at desc limit 1",
                modelId, "MODEL_REVIEW", "RESOURCE_REVIEW", "APPROVED");
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(ModelErrors.MODEL_STATE_CONFLICT
                    + ": 模型无进行中的审批单，无法测试（先提交审批）");
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    public boolean isApprovalTestable(String status) {
        return TESTABLE_APPROVAL_STATUSES.contains(status);
    }

    /* ============================== 权限 ============================== */

    /** 模型管理权限：创建人 / 平台管理员。 */
    public void requireCreator(Map<String, Object> model) {
        if (isAdmin()) {
            return;
        }
        String createdBy = string(model.get("created_by"));
        if (notBlank(createdBy) && !createdBy.equals(actor())) {
            throw new IllegalArgumentException(ModelErrors.MODEL_NO_PERMISSION + ": 仅创建人可操作该模型");
        }
    }

    /** 审批动作权限：模型创建人 / 审批提交人 / 平台管理员（对 Z-05 owner-only 的明确放宽）。 */
    private void requireApprovalActor(Map<String, Object> model, Map<String, Object> approval) {
        if (isAdmin()) {
            return;
        }
        String actorName = actor();
        boolean creator = actorName.equals(string(model.get("created_by")));
        boolean submitter = actorName.equals(string(approval.get("submitter")));
        if (!creator && !submitter) {
            throw new IllegalArgumentException(ModelErrors.MODEL_NO_PERMISSION + ": 仅模型创建人或审批提交人可执行审批动作");
        }
    }

    /* ============================== 内部 ============================== */

    private Map<String, Object> requireModel(String id) {
        List<Map<String, Object>> rows = jdbc.queryForList("select * from ds_model where id=? and deleted=0", id);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(ModelErrors.MODEL_NOT_FOUND + ": 模型不存在: " + id);
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    private Map<String, Object> requireApproval(String id) {
        List<Map<String, Object>> rows = jdbc.queryForList("select * from ds_model_approval where id=?", id);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(ModelErrors.MODEL_NOT_FOUND + ": 审批单不存在: " + id);
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    private Map<String, Object> requireArtifact(String id) {
        List<Map<String, Object>> rows = jdbc.queryForList("select * from ds_dev_artifact where id=? and deleted=0", id);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(ModelErrors.MODEL_NOT_FOUND + ": 制品不存在: " + id);
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    private Map<String, Object> requireRow(String sql, Object... args) {
        List<Map<String, Object>> rows = jdbc.queryForList(sql, args);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(ModelErrors.MODEL_NOT_FOUND + ": 记录不存在");
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    /** 版本必须存在且属于该制品（防跨制品引用）。 */
    private Map<String, Object> requireVersion(String artifactId, String versionId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select * from ds_dev_artifact_version where id=? and artifact_id=? and deleted=0", versionId, artifactId);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(ModelErrors.MODEL_NOT_FOUND + ": 制品版本不存在: " + artifactId + "/" + versionId);
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    private void requireProject(String projectId) {
        Long exists = count("select count(1) from project where project_id=? and is_deleted=0", projectId);
        if (exists == 0) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": 项目不存在: " + projectId);
        }
    }

    /** 模型执行/调用运行节点：项目首个节点；项目无节点回退创建人平台节点。 */
    private String nodeIdOf(String projectId, String fallback) {
        List<Map<String, Object>> nodes = jdbc.queryForList(
                "select node_id from project_node where project_id=? and is_deleted=0 order by id asc limit 1", projectId);
        return nodes.isEmpty() ? fallback : string(nodes.get(0).get("node_id"));
    }

    private void approvalHistory(String id, String action, String from, String to, String comment) {
        jdbc.update("insert into ds_model_approval_history(approval_id,action,from_status,to_status,operator,comment,created_at) "
                        + "values(?,?,?,?,?,?,?)",
                id, action, from, to, actor(), comment, now());
    }

    private String requiredAction(String action) {
        String act = action == null ? "" : action.trim().toUpperCase(Locale.ROOT);
        if (!APPROVAL_ACTIONS.contains(act)) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": action 必须是 APPROVE/REJECT/RESUBMIT/PUBLISH");
        }
        return act;
    }

    private boolean isAdmin() {
        UserContextDTO user = UserContext.getUserOrNotExist();
        return user != null && "kuscia-system".equals(user.getOwnerId()) && "admin".equals(user.getName());
    }

    private String actor() {
        UserContextDTO user = UserContext.getUserOrNotExist();
        return user == null || !notBlank(user.getName()) ? "system" : user.getName();
    }

    private String currentOwner() {
        UserContextDTO user = UserContext.getUserOrNotExist();
        return user == null || !notBlank(user.getOwnerId()) ? nodeId : user.getOwnerId();
    }

    private void audit(String action, String resourceType, String resourceId, String detail, boolean success) {
        mvp.auditAs("OPERATION", success ? "INFO" : "ERROR", actor(), action, resourceType, resourceId, detail, success);
    }

    private void dispatch(String event, Map<String, Object> payload) {
        mvp.dispatchWebhooks(event, payload);
    }

    private long count(String sql, Object... args) {
        Long value = jdbc.queryForObject(sql, Long.class, args);
        return value == null ? 0 : value;
    }

    private static String required(Map<String, Object> request, String key) {
        String value = string(request.get(key));
        if (!notBlank(value)) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": " + key + " 不能为空");
        }
        return value;
    }

    private String json(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            return String.valueOf(value);
        }
    }

    private static String string(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }

    private static int intValue(Object value, int defaultValue) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value == null || String.valueOf(value).isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static String now() {
        return LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS).toString();
    }

    private static String shortId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }
}
