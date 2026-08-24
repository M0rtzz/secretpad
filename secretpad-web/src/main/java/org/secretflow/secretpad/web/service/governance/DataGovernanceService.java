/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.service.governance;

import org.secretflow.secretpad.common.constant.DomainDataConstants;
import org.secretflow.secretpad.common.constant.DomainDatasourceConstants;
import org.secretflow.secretpad.common.dto.UserContextDTO;
import org.secretflow.secretpad.common.util.UUIDUtils;
import org.secretflow.secretpad.common.util.UserContext;
import org.secretflow.secretpad.kuscia.v1alpha1.service.impl.KusciaGrpcClientAdapter;
import org.secretflow.secretpad.manager.integration.datatable.DatatableManager;
import org.secretflow.secretpad.manager.integration.model.DatatableDTO;
import org.secretflow.secretpad.persistence.entity.NodeDO;
import org.secretflow.secretpad.persistence.repository.NodeRepository;
import org.secretflow.secretpad.web.service.DataSandboxMvpService;
import org.secretflow.secretpad.web.service.DataAssetService;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.secretflow.v1alpha1.common.Common;
import org.secretflow.v1alpha1.kusciaapi.Domaindata;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Z-04 数据治理服务：抽样/脱敏策略、内置执行引擎、任务记录、血缘与结果数据集注册。
 *
 * <p>与 {@code DataSandboxMvpService} 同构：JdbcTemplate + 条件 UPDATE（affected==1）做并发控制，
 * 审计/告警/webhook 复用 {@link DataSandboxMvpService#auditAs} / {@code raiseAlert} / {@code dispatchWebhooks}。
 * 内置执行全部在进程内完成（读 CSV → 抽样 → 脱敏 → 写结果 → 注册 Kuscia DomainData），
 * 输出只含策略允许的列；自定义代码执行（CUSTOM）由 {@link #submitCustomTask} 委托执行组件。</p>
 *
 * <p>权限前置校验 {@link #checkSourcePermission}：抽样、脱敏和源数据预览仅允许处理当前节点
 * 自己的源数据；项目共享的其他节点数据不能作为治理输入。</p>
 */
@Slf4j
@Service
public class DataGovernanceService {

    public static final String GOV_NO_PERMISSION = "GOV_NO_PERMISSION";
    public static final String GOV_INPUT_TOO_LARGE = "GOV_INPUT_TOO_LARGE";
    public static final String GOV_NOT_FOUND = "GOV_NOT_FOUND";
    public static final String GOV_STATE_CONFLICT = "GOV_STATE_CONFLICT";
    public static final String GOV_PARAM_INVALID = "GOV_PARAM_INVALID";

    private static final String STATUS_PENDING = "PENDING";
    private static final String STATUS_RUNNING = "RUNNING";
    private static final String STATUS_SUCCEEDED = "SUCCEEDED";
    private static final String STATUS_FAILED = "FAILED";
    private static final String STATUS_CANCELLED = "CANCELLED";

    private static final String ATTR_DATASOURCE_TYPE = "DatasourceType";
    private static final String ATTR_DATASOURCE_NAME = "DatasourceName";
    private static final String ATTR_DESC = "description";

    private static final Set<String> POLICY_TYPES = Set.of("SAMPLING", "MASKING", "SAMPLING_MASKING");
    private static final Set<String> SAMPLING_KEYS = Set.of(
            "method", "count", "ratio", "strataColumns", "clusterColumn", "blockSize", "seed", "limit");

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final KusciaGrpcClientAdapter kuscia;
    private final DatatableManager datatableManager;
    private final NodeRepository nodeRepository;
    private final DataSandboxMvpService mvp;
    private final GovernanceCustomExecutor customExecutor;
    private final DataAssetService dataAssetService;

    @Value("${secretpad.data.dir-path:/app/data/}")
    private String storeDir;

    @Value("${secretpad.data-sandbox.governance.input-rows:5000}")
    private long maxInputRows;

    @Value("${secretpad.data-sandbox.governance.input-bytes:262144}")
    private long maxInputBytes;

    @Value("${secretpad.data-sandbox.governance.max-retries:3}")
    private int maxRetries;

    public DataGovernanceService(
            @Qualifier("jdbcTemplate") JdbcTemplate jdbc,
            ObjectMapper objectMapper,
            KusciaGrpcClientAdapter kuscia,
            DatatableManager datatableManager,
            NodeRepository nodeRepository,
            DataSandboxMvpService mvp,
            GovernanceCustomExecutor customExecutor,
            DataAssetService dataAssetService) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.kuscia = kuscia;
        this.datatableManager = datatableManager;
        this.nodeRepository = nodeRepository;
        this.mvp = mvp;
        this.customExecutor = customExecutor;
        this.dataAssetService = dataAssetService;
    }

    /* ============================== 权限 ============================== */

    /** 源数据权限前置校验：抽样与脱敏只能处理当前节点自己的源数据。 */
    public void checkSourcePermission(UserContextDTO user, String nodeId, String datatableId) {
        if (user == null || !notBlank(nodeId) || !notBlank(datatableId)) {
            throw noPermission();
        }
        String currentNodeId = notBlank(user.getPlatformNodeId()) ? user.getPlatformNodeId() : user.getOwnerId();
        NodeDO node = nodeRepository.findByNodeId(nodeId);
        if (nodeId.equals(currentNodeId)
                || nodeId.equals(user.getOwnerId())
                || (node != null && user.getOwnerId().equals(node.getInstId()))) {
            Long raw = count("select count(1) from ds_data_asset where provider_node_id=? and data_stage='RAW' and deleted=0 and (datatable_id=? or (coalesce(datatable_id,'')='' and id=?))",
                    nodeId, datatableId, datatableId);
            if (raw > 0) {
                return;
            }
            throw new IllegalArgumentException(GOV_NO_PERMISSION + ": 只能处理本节点源数据");
        }
        throw noPermission();
    }

    private IllegalArgumentException noPermission() {
        return new IllegalArgumentException(GOV_NO_PERMISSION + ": 无权访问该数据表");
    }

    private void requireRawSourceAsset(String assetId, String nodeId, String datatableId) {
        Long matches = count("select count(1) from ds_data_asset where id=? and provider_node_id=? and data_stage='RAW' and deleted=0 and (datatable_id=? or (coalesce(datatable_id,'')='' and id=?))",
                assetId, nodeId, datatableId, datatableId);
        if (matches == 0) {
            throw new IllegalArgumentException(GOV_NO_PERMISSION + ": 只能选择本节点源数据作为策略参考");
        }
    }

    /* ============================== 策略 ============================== */

    public Map<String, Object> createPolicy(Map<String, Object> request) {
        String name = required(request, "name");
        String policyType = required(request, "policyType").trim().toUpperCase(Locale.ROOT);
        if (!POLICY_TYPES.contains(policyType)) {
            throw new IllegalArgumentException(GOV_PARAM_INVALID + ": policyType 必须是 SAMPLING/MASKING/SAMPLING_MASKING");
        }
        String samplingMethod = string(request.get("samplingMethod"));
        if (notBlank(samplingMethod)) {
            GovernanceSamplingMethod.from(samplingMethod);
        }
        String samplingParams = jsonOr(request.get("samplingParams"), "{}");
        String maskingColumns = jsonOr(request.get("maskingColumns"), "[]");
        String sourceAssetId = required(request, "sourceAssetId");
        String sourceNodeId = required(request, "sourceNodeId");
        String sourceDatatableId = required(request, "sourceDatatableId");
        checkSourcePermission(currentUser(), sourceNodeId, sourceDatatableId);
        requireRawSourceAsset(sourceAssetId, sourceNodeId, sourceDatatableId);
        Long dup = count("select count(1) from ds_governance_policy where name=? and deleted=0", name);
        if (dup > 0) {
            throw new IllegalArgumentException(GOV_STATE_CONFLICT + ": 策略名称已存在: " + name);
        }
        String id = "gp-" + shortId();
        String createdBy = actor();
        String now = now();
        jdbc.update("insert into ds_governance_policy(id,name,description,policy_type,sampling_method,sampling_params,masking_columns,source_asset_id,source_node_id,source_datatable_id,created_by,created_at,updated_at,deleted)"
                        + " values(?,?,?,?,?,?,?,?,?,?,?,?,?,0)",
                id, name, string(request.get("description")), policyType, samplingMethod,
                samplingParams, maskingColumns, sourceAssetId, sourceNodeId, sourceDatatableId,
                createdBy, now, now);
        audit("GOVERNANCE_POLICY_CREATE", "GOVERNANCE_POLICY", id, "type=" + policyType, true);
        dispatch("governance.policy.created", Map.of("id", id, "name", name));
        return policyDetail(id);
    }

    public Map<String, Object> updatePolicy(Map<String, Object> request) {
        String id = required(request, "id");
        Map<String, Object> policy = requirePolicy(id);
        requireCreator(policy, "策略");
        String policyType = value(request, "policyType", string(policy.get("policy_type"))).trim().toUpperCase(Locale.ROOT);
        if (!POLICY_TYPES.contains(policyType)) {
            throw new IllegalArgumentException(GOV_PARAM_INVALID + ": policyType 必须是 SAMPLING/MASKING/SAMPLING_MASKING");
        }
        String samplingMethod = value(request, "samplingMethod", string(policy.get("sampling_method")));
        if (notBlank(samplingMethod)) {
            GovernanceSamplingMethod.from(samplingMethod);
        }
        String samplingParams = jsonOr(request.get("samplingParams"), string(policy.get("sampling_params")));
        String maskingColumns = jsonOr(request.get("maskingColumns"), string(policy.get("masking_columns")));
        String sourceAssetId = required(request, "sourceAssetId");
        String sourceNodeId = required(request, "sourceNodeId");
        String sourceDatatableId = required(request, "sourceDatatableId");
        checkSourcePermission(currentUser(), sourceNodeId, sourceDatatableId);
        requireRawSourceAsset(sourceAssetId, sourceNodeId, sourceDatatableId);
        jdbc.update("update ds_governance_policy set description=?,policy_type=?,sampling_method=?,sampling_params=?,masking_columns=?,source_asset_id=?,source_node_id=?,source_datatable_id=?,updated_at=? where id=? and deleted=0",
                value(request, "description", string(policy.get("description"))), policyType, samplingMethod, samplingParams,
                maskingColumns, sourceAssetId, sourceNodeId, sourceDatatableId, now(), id);
        audit("GOVERNANCE_POLICY_UPDATE", "GOVERNANCE_POLICY", id, "", true);
        dispatch("governance.policy.updated", Map.of("id", id));
        return policyDetail(id);
    }

    public void deletePolicy(String id) {
        Map<String, Object> policy = requirePolicy(id);
        requireCreator(policy, "策略");
        jdbc.update("update ds_governance_policy set deleted=1,updated_at=? where id=?", now(), id);
        audit("GOVERNANCE_POLICY_DELETE", "GOVERNANCE_POLICY", id, "", true);
        dispatch("governance.policy.deleted", Map.of("id", id));
    }

    public List<Map<String, Object>> listPolicies(String type, String keyword) {
        StringBuilder sql = new StringBuilder(
                "select * from ds_governance_policy where deleted=0");
        List<Object> args = new ArrayList<>();
        if (notBlank(type)) {
            sql.append(" and policy_type=?");
            args.add(type.trim().toUpperCase(Locale.ROOT));
        }
        if (notBlank(keyword)) {
            sql.append(" and (name like ? or description like ?)");
            String like = "%" + keyword.trim() + "%";
            args.add(like);
            args.add(like);
        }
        sql.append(" order by updated_at desc limit 500");
        return jdbc.queryForList(sql.toString(), args.toArray());
    }

    public Map<String, Object> policyDetail(String id) {
        Map<String, Object> policy = requirePolicy(id);
        List<Map<String, Object>> tasks = jdbc.queryForList(
                "select id,name,status,created_by,created_at from ds_governance_task where policy_id=? and deleted=0 order by created_at desc limit 100",
                id);
        Map<String, Object> result = new LinkedHashMap<>(policy);
        result.put("tasks", tasks);
        return result;
    }

    /* ============================== 任务 ============================== */

    /** 提交治理任务：按 execMode 分发到内置引擎（BUILTIN）或自定义代码执行组件（CUSTOM）。 */
    public Map<String, Object> submitTask(Map<String, Object> request) {
        String execMode = value(request, "execMode", "BUILTIN").trim().toUpperCase(Locale.ROOT);
        if ("CUSTOM".equals(execMode)) {
            return submitCustomTask(request);
        }
        if (!"BUILTIN".equals(execMode)) {
            throw new IllegalArgumentException(GOV_PARAM_INVALID + ": execMode 必须是 BUILTIN/CUSTOM");
        }
        return submitBuiltinTask(request);
    }

    /**
     * 自定义代码执行（Z-04 受控能力）：输入子集（行数/字节均受限）随 task_input_config 进入
     * 一次性 Kuscia Job，由 {@link GovernanceCustomExecutor} 提交并在轮询中取回结果。
     */
    public Map<String, Object> submitCustomTask(Map<String, Object> request) {
        String nodeId = required(request, "nodeId");
        String datatableId = required(request, "datatableId");
        String sourceAssetId = string(request.get("sourceAssetId"));
        String script = required(request, "script");
        checkSourcePermission(currentUser(), nodeId, datatableId);
        if (notBlank(sourceAssetId)) requireRawSourceAsset(sourceAssetId, nodeId, datatableId);

        DatatableDTO source = resolveSource(nodeId, datatableId);
        String relativeUri = source.getRelativeUri();
        if (!notBlank(relativeUri)) {
            throw new IllegalArgumentException(GOV_NOT_FOUND + ": 源数据表缺少 relativeUri");
        }
        Map<String, Object> policy = resolvePolicyMap(request);
        Map<String, Object> sampling = resolveSampling(request, policy);
        List<Map<String, Object>> masking = resolveMasking(request, policy);

        // 读源 + 行数/字节校验（超限在任务创建前拒绝，不产生任务记录）；物理目录 = 源表属主（kuscia 域）
        List<List<String>> parsed = readCsv(source.getNodeId(), relativeUri);
        List<String> header = parsed.isEmpty() ? new ArrayList<>() : new ArrayList<>(parsed.get(0));
        List<List<String>> data = parsed.size() > 1 ? new ArrayList<>(parsed.subList(1, parsed.size())) : new ArrayList<>();
        if (data.size() > maxInputRows) {
            throw new IllegalArgumentException(GOV_INPUT_TOO_LARGE + ": 源数据行数 " + data.size() + " 超过上限 " + maxInputRows);
        }
        if (header.isEmpty()) {
            throw new IllegalArgumentException(GOV_PARAM_INVALID + ": 源 CSV 表头为空");
        }
        String inputB64 = Base64.getEncoder().encodeToString(CsvUtil.toCsv(header, data).getBytes(StandardCharsets.UTF_8));
        if (inputB64.length() > maxInputBytes) {
            throw new IllegalArgumentException(GOV_INPUT_TOO_LARGE + ": 输入数据超过 " + maxInputBytes + " 字节上限");
        }
        Map<String, Object> params = new LinkedHashMap<>();
        if (request.get("params") instanceof Map<?, ?> paramsMap) {
            params.putAll(castMap(paramsMap));
        }

        String taskId = createTask(request, "CUSTOM", nodeId, datatableId, relativeUri, policy, sampling, masking);
        // 自定义任务快照：脚本全文入 script_content，params/输入行数入 exec_params，输入行数入 source_rows
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("sampling", sampling == null ? new LinkedHashMap<>() : sampling);
        snapshot.put("masking", masking);
        snapshot.put("custom", Map.of("params", params, "inputRows", data.size()));
        jdbc.update("update ds_governance_task set script_content=?,source_rows=?,exec_params=? where id=?",
                script, data.size(), json(snapshot), taskId);
        audit("GOVERNANCE_TASK_SUBMIT", "GOVERNANCE_TASK", taskId, "mode=CUSTOM source=" + nodeId + "/" + datatableId, true);
        dispatch("governance.task.submitted", Map.of("id", taskId, "mode", "CUSTOM"));
        try {
            claimTask(taskId);
            customExecutor.submit(taskId, nodeId, inputB64, script, params);
        } catch (Exception e) {
            log.warn("Governance custom task {} submit failed: {}", taskId, e.getMessage(), e);
            failTask(taskId, e);
        }
        return taskDetail(taskId);
    }

    /** 内置抽样/脱敏执行流（同步，任务级状态机 PENDING→RUNNING→SUCCEEDED/FAILED）。 */
    public Map<String, Object> submitBuiltinTask(Map<String, Object> request) {
        String nodeId = required(request, "nodeId");
        String datatableId = required(request, "datatableId");
        String sourceAssetId = string(request.get("sourceAssetId"));
        checkSourcePermission(currentUser(), nodeId, datatableId);
        if (notBlank(sourceAssetId)) requireRawSourceAsset(sourceAssetId, nodeId, datatableId);

        DatatableDTO source = resolveSource(nodeId, datatableId);
        String relativeUri = source.getRelativeUri();
        if (!notBlank(relativeUri)) {
            throw new IllegalArgumentException(GOV_NOT_FOUND + ": 源数据表缺少 relativeUri");
        }
        Map<String, Object> policy = resolvePolicyMap(request);
        Map<String, Object> sampling = resolveSampling(request, policy);
        List<Map<String, Object>> masking = resolveMasking(request, policy);

        // 读源 + 行数校验（超限在任务创建前拒绝，不产生任务记录）；物理目录 = 源表属主（kuscia 域）
        List<List<String>> parsed = readCsv(source.getNodeId(), relativeUri);
        List<String> header = parsed.isEmpty() ? new ArrayList<>() : new ArrayList<>(parsed.get(0));
        List<List<String>> data = parsed.size() > 1 ? new ArrayList<>(parsed.subList(1, parsed.size())) : new ArrayList<>();
        if (data.size() > maxInputRows) {
            throw new IllegalArgumentException(GOV_INPUT_TOO_LARGE + ": 源数据行数 " + data.size() + " 超过上限 " + maxInputRows);
        }
        if (header.isEmpty()) {
            throw new IllegalArgumentException(GOV_PARAM_INVALID + ": 源 CSV 表头为空");
        }

        String taskId = createTask(request, "BUILTIN", nodeId, datatableId, relativeUri, policy, sampling, masking);
        audit("GOVERNANCE_TASK_SUBMIT", "GOVERNANCE_TASK", taskId, "mode=BUILTIN source=" + nodeId + "/" + datatableId, true);
        dispatch("governance.task.submitted", Map.of("id", taskId, "mode", "BUILTIN"));
        try {
            claimTask(taskId);
            runBuiltin(taskId, source, header, data, sampling, masking);
        } catch (Exception e) {
            log.warn("Governance builtin task {} failed: {}", taskId, e.getMessage(), e);
            failTask(taskId, e);
        }
        return taskDetail(taskId);
    }

    public List<Map<String, Object>> listTasks(String status, String execMode, String keyword) {
        StringBuilder sql = new StringBuilder("select * from ds_governance_task where deleted=0");
        List<Object> args = new ArrayList<>();
        if (notBlank(status)) {
            sql.append(" and status=?");
            args.add(status.trim().toUpperCase(Locale.ROOT));
        }
        if (notBlank(execMode)) {
            sql.append(" and exec_mode=?");
            args.add(execMode.trim().toUpperCase(Locale.ROOT));
        }
        if (notBlank(keyword)) {
            sql.append(" and (name like ? or id like ? or description like ?)");
            String like = "%" + keyword.trim() + "%";
            args.add(like);
            args.add(like);
            args.add(like);
        }
        sql.append(" order by created_at desc limit 500");
        return jdbc.queryForList(sql.toString(), args.toArray());
    }

    public Map<String, Object> taskDetail(String id) {
        Map<String, Object> task = requireTask(id);
        List<Map<String, Object>> lineage = jdbc.queryForList(
                "select * from ds_governance_lineage where task_id=? and deleted=0 order by id", id);
        Map<String, Object> result = new LinkedHashMap<>(task);
        result.put("lineage", lineage);
        return result;
    }

    public void cancelTask(String id) {
        Map<String, Object> task = requireTask(id);
        requireCreator(task, "任务");
        String status = string(task.get("status"));
        String jobId = string(task.get("kuscia_job_id"));
        if (STATUS_PENDING.equals(status)) {
            jdbc.update("update ds_governance_task set status=?,finished_at=?,updated_at=? where id=? and status=?",
                    STATUS_CANCELLED, now(), now(), id, STATUS_PENDING);
        } else if (STATUS_RUNNING.equals(status)) {
            if (notBlank(jobId)) {
                mvp.stopKuscia(Map.of("kuscia_job_id", jobId), "Governance task cancelled");
            }
            jdbc.update("update ds_governance_task set status=?,finished_at=?,updated_at=? where id=? and status=?",
                    STATUS_CANCELLED, now(), now(), id, STATUS_RUNNING);
        } else {
            throw new IllegalStateException(GOV_STATE_CONFLICT + ": 当前状态不可取消: " + status);
        }
        audit("GOVERNANCE_TASK_CANCEL", "GOVERNANCE_TASK", id, "", true);
        dispatch("governance.task.cancelled", Map.of("id", id));
    }

    public Map<String, Object> retryTask(String id) {
        Map<String, Object> task = requireTask(id);
        requireCreator(task, "任务");
        if (!STATUS_FAILED.equals(string(task.get("status")))) {
            throw new IllegalStateException(GOV_STATE_CONFLICT + ": 仅 FAILED 任务可重试");
        }
        int retries = ((Number) task.get("retry_count")).intValue();
        if (retries >= maxRetries) {
            throw new IllegalStateException(GOV_STATE_CONFLICT + ": 重试次数已达上限 " + maxRetries);
        }
        String nodeId = string(task.get("source_node_id"));
        String datatableId = string(task.get("source_datatable_id"));
        String relativeUri = string(task.get("source_relative_uri"));
        checkSourcePermission(currentUser(), nodeId, datatableId);
        DatatableDTO source = resolveSource(nodeId, datatableId);

        Map<String, Object> snapshot = parseJsonMap(string(task.get("exec_params")));
        Map<String, Object> sampling = snapshot.get("sampling") instanceof Map<?, ?> sMap ? castMap(sMap) : new LinkedHashMap<>();
        List<Map<String, Object>> masking = castList(snapshot.get("masking"));

        List<List<String>> parsed = readCsv(source.getNodeId(), relativeUri);
        List<String> header = parsed.isEmpty() ? new ArrayList<>() : new ArrayList<>(parsed.get(0));
        List<List<String>> data = parsed.size() > 1 ? new ArrayList<>(parsed.subList(1, parsed.size())) : new ArrayList<>();

        jdbc.update("update ds_governance_task set retry_count=retry_count+1,error_message='',kuscia_job_id='',started_at=?,status=?,updated_at=? where id=? and status=?",
                now(), STATUS_RUNNING, now(), id, STATUS_FAILED);
        audit("GOVERNANCE_TASK_RETRY", "GOVERNANCE_TASK", id, "retry=" + (retries + 1), true);
        try {
            if ("CUSTOM".equals(string(task.get("exec_mode")))) {
                String script = string(task.get("script_content"));
                String inputB64 = Base64.getEncoder().encodeToString(CsvUtil.toCsv(header, data).getBytes(StandardCharsets.UTF_8));
                Map<String, Object> params = new LinkedHashMap<>();
                if (snapshot.get("custom") instanceof Map<?, ?> custom) {
                    Object customParams = castMap(custom).get("params");
                    if (customParams instanceof Map<?, ?> p) {
                        params.putAll(castMap(p));
                    }
                }
                customExecutor.submit(id, nodeId, inputB64, script, params);
            } else {
                runBuiltin(id, source, header, data, sampling, masking);
            }
        } catch (Exception e) {
            log.warn("Governance retry {} failed: {}", id, e.getMessage(), e);
            failTask(id, e);
        }
        return taskDetail(id);
    }

    public List<Map<String, Object>> listResults(String nodeId) {
        StringBuilder sql = new StringBuilder(
                "select * from ds_governance_task where deleted=0 and status='SUCCEEDED' and result_datatable_id<>''");
        List<Object> args = new ArrayList<>();
        if (notBlank(nodeId)) {
            sql.append(" and result_node_id=?");
            args.add(nodeId);
        }
        sql.append(" order by finished_at desc limit 500");
        return jdbc.queryForList(sql.toString(), args.toArray());
    }

    /** 结果数据集挂载项目（source=IMPORTED），复用 project_datatable 授权表。
     *  source 须为 IMPORTED，否则项目数据集树（仅按 IMPORTED 查询）不展示挂载结果。 */
    @Transactional
    public Map<String, Object> mountResult(Map<String, Object> request) {
        String taskId = required(request, "taskId");
        String projectId = required(request, "projectId");
        Map<String, Object> task = requireTask(taskId);
        if (!STATUS_SUCCEEDED.equals(string(task.get("status"))) || !notBlank(string(task.get("result_datatable_id")))) {
            throw new IllegalStateException(GOV_STATE_CONFLICT + ": 仅 SUCCEEDED 且含结果数据集的任务可挂载");
        }
        String nodeId = string(task.get("result_node_id"));
        String datatableId = string(task.get("result_datatable_id"));
        Map<String, Object> resultAsset = dataAssetService.registerGovernedResult(taskId, nodeId, datatableId);
        String assetId = string(resultAsset.get("id"));
        try {
            dataAssetService.attachGovernedResult(projectId, assetId);
        } catch (IllegalStateException e) {
            if ("结果已挂载到该项目".equals(e.getMessage())) {
                throw new IllegalStateException(GOV_STATE_CONFLICT + ": " + e.getMessage(), e);
            }
            throw e;
        }
        Long dup = count("select count(1) from project_datatable where project_id=? and node_id=? and datatable_id=? and is_deleted=0",
                projectId, nodeId, datatableId);
        if (dup == 0) {
            String tableConfigs = buildTableConfigs(nodeId, datatableId);
            jdbc.update("insert into project_datatable(project_id,node_id,datatable_id,table_configs,source,is_deleted) values(?,?,?,?,?,0)",
                    projectId, nodeId, datatableId, tableConfigs, "IMPORTED");
        }
        audit("GOVERNANCE_RESULT_MOUNT", "GOVERNANCE_TASK", taskId, "project=" + projectId + " result=" + datatableId, true);
        dispatch("governance.result.mounted", Map.of("taskId", taskId, "projectId", projectId, "datatableId", datatableId));
        return taskDetail(taskId);
    }

    /* ============================== 血缘 / 预览 ============================== */

    public List<Map<String, Object>> lineage(String nodeId, String datatableId) {
        StringBuilder sql = new StringBuilder("select * from ds_governance_lineage where deleted=0");
        List<Object> args = new ArrayList<>();
        if (notBlank(nodeId) || notBlank(datatableId)) {
            sql.append(" and (source_node_id=? or target_node_id=?");
            args.add(nodeId);
            args.add(nodeId);
            if (notBlank(datatableId)) {
                sql.append(" or source_datatable_id=? or target_datatable_id=?");
                args.add(datatableId);
                args.add(datatableId);
            }
            sql.append(")");
        }
        sql.append(" order by id desc limit 500");
        return jdbc.queryForList(sql.toString(), args.toArray());
    }

    /** 源数据预览：强制权限校验，仅返回前 limit 行 + schema + 行数，绝不返回全量。 */
    public Map<String, Object> previewSource(Map<String, Object> request) {
        String nodeId = required(request, "nodeId");
        String datatableId = required(request, "datatableId");
        int limit = Math.max(1, Math.min(intValue(request.get("limit"), 20), 100));
        checkSourcePermission(currentUser(), nodeId, datatableId);
        DatatableDTO source = resolveSource(nodeId, datatableId);
        String relativeUri = source.getRelativeUri();
        if (!notBlank(relativeUri)) {
            throw new IllegalArgumentException(GOV_NOT_FOUND + ": 源数据表缺少 relativeUri");
        }
        List<List<String>> parsed = readCsv(source.getNodeId(), relativeUri);
        List<String> header = parsed.isEmpty() ? new ArrayList<>() : new ArrayList<>(parsed.get(0));
        List<List<String>> data = parsed.size() > 1 ? new ArrayList<>(parsed.subList(1, parsed.size())) : new ArrayList<>();
        List<List<String>> previewRows = new ArrayList<>();
        for (int i = 0; i < Math.min(limit, data.size()); i++) {
            previewRows.add(new ArrayList<>(data.get(i)));
        }
        List<Map<String, Object>> schema = new ArrayList<>();
        if (source.getSchema() != null) {
            for (DatatableDTO.TableColumnDTO column : source.getSchema()) {
                schema.add(Map.of("colName", string(column.getColName()), "colType", string(column.getColType()),
                        "colComment", string(column.getColComment())));
            }
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("nodeId", nodeId);
        result.put("datatableId", datatableId);
        result.put("name", source.getDatatableName());
        result.put("relativeUri", relativeUri);
        result.put("header", header);
        result.put("schema", schema);
        result.put("sourceRows", data.size());
        result.put("rows", previewRows);
        return result;
    }

    /**
     * 查看任务结果数据：仅脱敏后的结果可返回行数据（masked=true），表头携带数据源/结果表信息；
     * 未脱敏（纯抽样或自定义代码输出）不返回行，仅返回 masked=false 元信息——保证不暴露未经授权的真实数据。
     */
    public Map<String, Object> viewResult(String taskId) {
        Map<String, Object> task = requireTask(taskId);
        UserContextDTO user = currentUser();
        if (user == null || !notBlank(user.getOwnerId())) {
            throw noPermission();
        }
        requireCreator(task, "结果");
        if (!STATUS_SUCCEEDED.equals(string(task.get("status"))) || !notBlank(string(task.get("result_datatable_id")))) {
            throw new IllegalStateException(GOV_STATE_CONFLICT + ": 仅 SUCCEEDED 且含结果数据集的任务可查看结果");
        }
        Map<String, Object> snapshot = parseJsonMap(string(task.get("exec_params")));
        Object samplingObj = snapshot.get("sampling");
        Map<String, Object> sampling = samplingObj instanceof Map<?, ?> m ? castMap(m) : new LinkedHashMap<>();
        List<Map<String, Object>> masking = castList(snapshot.get("masking"));
        boolean masked = masking != null && !masking.isEmpty();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("taskId", taskId);
        result.put("execMode", string(task.get("exec_mode")));
        result.put("samplingMethod", string(sampling.get("method")));
        result.put("masked", masked);
        result.put("sourceNodeId", string(task.get("source_node_id")));
        result.put("sourceDatatableId", string(task.get("source_datatable_id")));
        result.put("resultNodeId", string(task.get("result_node_id")));
        result.put("resultDatatableId", string(task.get("result_datatable_id")));
        result.put("sourceRows", longValue(task.get("source_rows")));
        result.put("resultRows", longValue(task.get("result_rows")));
        result.put("sourceName", tableName(string(task.get("source_node_id")), string(task.get("source_datatable_id"))));
        result.put("resultName", tableName(string(task.get("result_node_id")), string(task.get("result_datatable_id"))));
        if (!masked) {
            result.put("message", "该结果未经脱敏（纯抽样或自定义代码输出），含真实数据，不予展示");
            return result;
        }
        // 任务创建人可查看已脱敏结果；源数据 RAW 权限校验不适用于治理结果表。
        DatatableDTO dst = resolveSource(string(task.get("result_node_id")), string(task.get("result_datatable_id")));
        List<List<String>> parsed = readCsv(dst.getNodeId(), dst.getRelativeUri());
        List<String> header = parsed.isEmpty() ? new ArrayList<>() : new ArrayList<>(parsed.get(0));
        List<List<String>> data = parsed.size() > 1 ? new ArrayList<>(parsed.subList(1, parsed.size())) : new ArrayList<>();
        List<List<String>> rows = new ArrayList<>();
        for (int i = 0; i < Math.min(100, data.size()); i++) {
            rows.add(new ArrayList<>(data.get(i)));
        }
        result.put("header", header);
        result.put("rows", rows);
        return result;
    }

    /** 表名兜底：取不到元数据时退回 datatableId。 */
    private String tableName(String nodeId, String datatableId) {
        if (!notBlank(nodeId) || !notBlank(datatableId)) {
            return "";
        }
        try {
            return string(resolveSource(nodeId, datatableId).getDatatableName());
        } catch (Exception e) {
            return datatableId;
        }
    }

    /* ============================== 内置执行 ============================== */

    private void runBuiltin(String taskId, DatatableDTO source, List<String> header, List<List<String>> data,
            Map<String, Object> sampling, List<Map<String, Object>> masking) {
        long sourceRows = data.size();
        List<List<String>> sampled = sampleRows(header, data, sampling);
        GovernanceMaskingExecutor.MaskResult masked = maskRows(header, sampled, masking);
        Map<String,Object> resultAsset=dataAssetService.registerGovernedResult(taskId,source.getNodeId(),
                CsvUtil.toCsv(masked.header(),masked.rows()).getBytes(StandardCharsets.UTF_8));
        String domainDataId=string(resultAsset.get("datatable_id"));
        long resultRows = masked.rows().size();
        jdbc.update("update ds_governance_task set status=?,result_node_id=?,result_datatable_id=?,source_rows=?,result_rows=?,finished_at=?,updated_at=? where id=? and status=?",
                STATUS_SUCCEEDED, source.getNodeId(), domainDataId, sourceRows, resultRows, now(), now(), taskId, STATUS_RUNNING);
        insertLineage(taskId, source.getNodeId(), source.getDatatableId(), source.getNodeId(), domainDataId);
        audit("GOVERNANCE_TASK_SUCCEEDED", "GOVERNANCE_TASK", taskId,
                "rows=" + sourceRows + "->" + resultRows + " result=" + domainDataId, true);
        dispatch("governance.task.succeeded", Map.of("id", taskId, "sourceRows", sourceRows,
                "resultRows", resultRows, "resultDatatableId", domainDataId));
    }

    private List<List<String>> sampleRows(List<String> header, List<List<String>> data,
            Map<String, Object> sampling) {
        GovernanceSamplingExecutor.SamplingParams params = samplingParams(sampling);
        if (params == null || !notBlank(params.method())) {
            return data;
        }
        return GovernanceSamplingExecutor.sample(header, data, params);
    }

    private GovernanceMaskingExecutor.MaskResult maskRows(List<String> header, List<List<String>> rows,
            List<Map<String, Object>> masking) {
        List<GovernanceMaskingExecutor.MaskRule> rules = maskRules(masking);
        if (rules.isEmpty()) {
            return new GovernanceMaskingExecutor.MaskResult(new ArrayList<>(header),
                    rows.stream().map(r -> (List<String>) new ArrayList<>(r)).toList());
        }
        return GovernanceMaskingExecutor.apply(header, rows, rules);
    }

    private void insertLineage(String taskId, String sourceNodeId, String sourceDatatableId,
            String targetNodeId, String targetDatatableId) {
        jdbc.update("insert into ds_governance_lineage(task_id,source_node_id,source_datatable_id,target_node_id,target_datatable_id,op_type,created_by,created_at,deleted)"
                        + " values(?,?,?,?,?,?,?,?,0)",
                taskId, sourceNodeId, sourceDatatableId, targetNodeId, targetDatatableId,
                "SAMPLE_MASK", actor(), now());
    }

    /** 认领 PENDING 任务为 RUNNING（仿 Z-03 条件 UPDATE + affected==1 并发控制）。 */
    private void claimTask(String taskId) {
        int affected = jdbc.update("update ds_governance_task set status=?,started_at=?,updated_at=? where id=? and status=?",
                STATUS_RUNNING, now(), now(), taskId, STATUS_PENDING);
        if (affected != 1) {
            throw new IllegalStateException(GOV_STATE_CONFLICT + ": 任务状态已变更，无法开始执行: " + taskId);
        }
    }

    private void failTask(String taskId, Exception e) {
        jdbc.update("update ds_governance_task set status=?,error_message=?,finished_at=?,updated_at=? where id=? and status=?",
                STATUS_FAILED, truncate(e.getMessage(), 1900), now(), now(), taskId, STATUS_RUNNING);
        mvp.raiseAlert("WARNING", "GOVERNANCE", "数据治理任务执行失败",
                "任务 " + taskId + "：" + truncate(e.getMessage(), 900), "gov:" + taskId + ":failed");
        audit("GOVERNANCE_TASK_FAILED", "GOVERNANCE_TASK", taskId, truncate(e.getMessage(), 1500), false);
        dispatch("governance.task.failed", Map.of("id", taskId, "error", truncate(e.getMessage(), 500)));
    }

    private String createTask(Map<String, Object> request, String execMode, String nodeId, String datatableId,
            String relativeUri, Map<String, Object> policy, Map<String, Object> sampling, List<Map<String, Object>> masking) {
        String taskId = "gt-" + shortId();
        String name = value(request, "name", "治理任务-" + taskId);
        String policyId = string(request.get("policyId"));
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("sampling", sampling == null ? new LinkedHashMap<>() : sampling);
        snapshot.put("masking", masking == null ? new ArrayList<>() : masking);
        String execParams = json(snapshot);
        String now = now();
        jdbc.update("insert into ds_governance_task(id,name,description,policy_id,exec_mode,source_node_id,source_datatable_id,"
                        + "source_relative_uri,exec_params,script_content,status,result_node_id,result_datatable_id,source_rows,result_rows,"
                        + "error_message,kuscia_job_id,retry_count,created_by,created_at,updated_at,started_at,finished_at,deleted)"
                        + " values(?,?,?,?,?,?,?,?,?,?,'PENDING','','',0,0,'','',0,?,?,?,?,'',0)",
                taskId, name, string(request.get("description")), policyId, execMode, nodeId, datatableId,
                relativeUri, execParams, "", actor(), now, now, now);
        return taskId;
    }

    /* ============================== 数据 / 注册 ============================== */

    private DatatableDTO resolveSource(String nodeId, String datatableId) {
        var catalogTable = dataAssetService.processingTable(nodeId, datatableId);
        if (catalogTable.isPresent()) {
            return catalogTable.get();
        }
        return datatableManager.findById(DatatableDTO.NodeDatatableId.from(nodeId, datatableId))
                .orElseThrow(() -> new IllegalArgumentException(GOV_NOT_FOUND + ": 数据表不存在: " + nodeId + "/" + datatableId));
    }

    /** 读源 CSV（复用 DataServiceImpl 的 storeDir+nodeId+relativeUri 解析 + canonical 安全校验 + BOM 剥离）。 */
    private List<List<String>> readCsv(String nodeId, String relativeUri) {
        if (relativeUri.startsWith("s3://")) {
            try (InputStream input = dataAssetService.openStored(relativeUri)) {
                return CsvUtil.parse(new String(input.readAllBytes(), StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new IllegalStateException(GOV_NOT_FOUND + ": 读取 MinIO 源 CSV 失败: " + e.getMessage(), e);
            }
        }
        if (relativeUri.contains("..")) {
            throw new IllegalArgumentException(GOV_PARAM_INVALID + ": 非法路径");
        }
        Path base = Path.of(storeDir, nodeId).toAbsolutePath().normalize();
        Path target = base.resolve(relativeUri).normalize();
        if (!target.startsWith(base)) {
            throw new IllegalArgumentException(GOV_PARAM_INVALID + ": 非法路径");
        }
        try {
            String content = Files.readString(target, StandardCharsets.UTF_8);
            return CsvUtil.parse(content);
        } catch (IOException e) {
            throw new IllegalStateException(GOV_NOT_FOUND + ": 读取源 CSV 失败: " + e.getMessage(), e);
        }
    }

    private String writeResultCsv(String nodeId, String taskId, List<String> header, List<List<String>> rows) {
        String resultUri = taskId + "-" + shortId() + ".csv";
        Path base = Path.of(storeDir, nodeId).toAbsolutePath().normalize();
        try {
            Files.createDirectories(base);
        } catch (IOException e) {
            throw new IllegalStateException("无法创建结果目录: " + e.getMessage(), e);
        }
        Path target = base.resolve(resultUri).normalize();
        if (!target.startsWith(base)) {
            throw new IllegalStateException(GOV_PARAM_INVALID + ": 非法结果路径");
        }
        try {
            Files.writeString(target, CsvUtil.toCsv(header, rows), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("写入结果 CSV 失败: " + e.getMessage(), e);
        }
        return resultUri;
    }

    /** 结果数据集注册为 Kuscia DomainData（type=table, CSV），columns 由输出表头 + 源 schema 推导。 */
    private String registerResultDomainData(String nodeId, String taskId, String relativeUri,
            List<String> header, List<DatatableDTO.TableColumnDTO> sourceSchema) {
        String domainDataId = UUIDUtils.random(8);
        List<Common.DataColumn> columns = new ArrayList<>();
        for (String col : header) {
            String type = "str";
            String comment = "";
            if (sourceSchema != null) {
                for (DatatableDTO.TableColumnDTO column : sourceSchema) {
                    if (col.equals(column.getColName())) {
                        type = string(column.getColType());
                        comment = string(column.getColComment());
                        break;
                    }
                }
            }
            columns.add(Common.DataColumn.newBuilder().setName(col).setType(type).setComment(comment).build());
        }
        Domaindata.CreateDomainDataRequest request = Domaindata.CreateDomainDataRequest.newBuilder()
                .setDomaindataId(domainDataId)
                .setDomainId(nodeId)
                .setName("gov-" + taskId)
                .setType("table")
                .setFileFormat(Common.FileFormat.CSV)
                .setDatasourceId(DomainDatasourceConstants.DEFAULT_DATASOURCE)
                .putAttributes(ATTR_DATASOURCE_TYPE, DomainDataConstants.DEFAULT_LOCAL_DATASOURCE_TYPE)
                .putAttributes(ATTR_DATASOURCE_NAME, DomainDataConstants.DEFAULT_LOCAL_DATASOURCE_NAME)
                .putAttributes(DomainDataConstants.NULL_STRS, "[]")
                .putAttributes(ATTR_DESC, "数据治理结果：" + taskId)
                .setRelativeUri(relativeUri)
                .addAllColumns(columns)
                .build();
        Domaindata.CreateDomainDataResponse response = kuscia.createDomainData(request);
        if (response.getStatus().getCode() != 0) {
            throw new IllegalStateException("注册结果数据集失败: " + response.getStatus().getMessage());
        }
        return domainDataId;
    }

    private String buildTableConfigs(String nodeId, String datatableId) {
        // 结果表头 ⊆ 源列（CLEAR-drop 只删列不新增），故从任务源 schema 推导 colType 并保留类型
        Map<String, Object> task = requireRow("select * from ds_governance_task where result_node_id=? and result_datatable_id=? and deleted=0",
                nodeId, datatableId);
        String sourceNodeId = string(task.get("source_node_id"));
        String sourceDatatableId = string(task.get("source_datatable_id"));
        List<Map<String, Object>> sourceSchema = new ArrayList<>();
        try {
            DatatableDTO source = resolveSource(sourceNodeId, sourceDatatableId);
            if (source.getSchema() != null) {
                for (DatatableDTO.TableColumnDTO column : source.getSchema()) {
                    sourceSchema.add(Map.of("colName", string(column.getColName()), "colType", string(column.getColType()),
                            "colComment", string(column.getColComment())));
                }
            }
        } catch (Exception e) {
            log.warn("Unable to resolve source schema for mount, using str types: {}", e.getMessage());
        }
        List<Map<String, Object>> configs = new ArrayList<>();
        for (Map<String, Object> col : sourceSchema) {
            Map<String, Object> config = new LinkedHashMap<>();
            config.put("colName", string(col.get("colName")));
            config.put("colType", string(col.get("colType")));
            config.put("colComment", string(col.get("colComment")));
            config.put("isAssociateKey", false);
            config.put("isGroupKey", false);
            configs.add(config);
        }
        return json(configs);
    }

    /* ============================== 策略/参数解析 ============================== */

    private Map<String, Object> resolvePolicyMap(Map<String, Object> request) {
        String policyId = string(request.get("policyId"));
        if (!notBlank(policyId)) {
            return new LinkedHashMap<>();
        }
        Map<String, Object> policy = requirePolicy(policyId);
        requireCreator(policy, "策略");
        return new LinkedHashMap<>(policy);
    }

    private Map<String, Object> resolveSampling(Map<String, Object> request, Map<String, Object> policy) {
        Object inline = request.get("sampling");
        if (inline instanceof Map<?, ?> inlineMap) {
            Map<String, Object> result = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : inlineMap.entrySet()) {
                if (SAMPLING_KEYS.contains(String.valueOf(entry.getKey()))) {
                    result.put(String.valueOf(entry.getKey()), entry.getValue());
                }
            }
            return result;
        }
        String method = string(policy.get("sampling_method"));
        if (!notBlank(method)) {
            return new LinkedHashMap<>();
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("method", method);
        Map<String, Object> params = parseJsonMap(string(policy.get("sampling_params")));
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            if (SAMPLING_KEYS.contains(entry.getKey())) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    private List<Map<String, Object>> resolveMasking(Map<String, Object> request, Map<String, Object> policy) {
        Object inline = request.get("masking");
        if (inline instanceof List<?> inlineList) {
            List<Map<String, Object>> result = new ArrayList<>();
            for (Object item : inlineList) {
                if (item instanceof Map<?, ?> map) {
                    result.add(castMap(map));
                }
            }
            return result;
        }
        return parseJsonList(string(policy.get("masking_columns")));
    }

    private GovernanceSamplingExecutor.SamplingParams samplingParams(Map<String, Object> sampling) {
        if (sampling == null || sampling.isEmpty() || !notBlank(string(sampling.get("method")))) {
            return null;
        }
        return new GovernanceSamplingExecutor.SamplingParams(
                string(sampling.get("method")),
                longValue(sampling.get("count")),
                doubleValue(sampling.get("ratio")),
                stringList(sampling.get("strataColumns")),
                string(sampling.get("clusterColumn")),
                intValue(sampling.get("blockSize")),
                longValue(sampling.get("seed")),
                intValue(sampling.get("limit")));
    }

    private List<GovernanceMaskingExecutor.MaskRule> maskRules(List<Map<String, Object>> masking) {
        List<GovernanceMaskingExecutor.MaskRule> rules = new ArrayList<>();
        if (masking == null) {
            return rules;
        }
        for (Map<String, Object> item : masking) {
            String column = string(item.get("column"));
            String method = string(item.get("method"));
            if (!notBlank(column) || !notBlank(method)) {
                throw new IllegalArgumentException(GOV_PARAM_INVALID + ": 脱敏规则缺少 column/method");
            }
            Map<String, String> params = stringParams(item.get("params"));
            rules.add(new GovernanceMaskingExecutor.MaskRule(column, method, params));
        }
        return rules;
    }

    /* ============================== 审计 / 辅助 ============================== */

    private void audit(String action, String resourceType, String resourceId, String detail, boolean success) {
        mvp.auditAs("OPERATION", success ? "INFO" : "ERROR", actor(), action, resourceType, resourceId, detail, success);
    }

    private void dispatch(String event, Map<String, Object> payload) {
        mvp.dispatchWebhooks(event, payload);
    }

    private Map<String, Object> requirePolicy(String id) {
        return requireRow("select * from ds_governance_policy where id=? and deleted=0", id);
    }

    private Map<String, Object> requireTask(String id) {
        return requireRow("select * from ds_governance_task where id=? and deleted=0", id);
    }

    private void requireCreator(Map<String, Object> row, String what) {
        String createdBy = string(row.get("created_by"));
        if (notBlank(createdBy) && !createdBy.equals(actor())) {
            throw new IllegalStateException(GOV_NO_PERMISSION + ": 仅创建人可操作该" + what);
        }
    }

    private UserContextDTO currentUser() {
        return UserContext.getUserOrNotExist();
    }

    private String actor() {
        UserContextDTO user = UserContext.getUserOrNotExist();
        return user == null || !notBlank(user.getName()) ? "system" : user.getName();
    }

    private Map<String, Object> requireRow(String sql, Object... args) {
        List<Map<String, Object>> rows = jdbc.queryForList(sql, args);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(GOV_NOT_FOUND + ": 记录不存在");
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    private long count(String sql, Object... args) {
        Long value = jdbc.queryForObject(sql, Long.class, args);
        return value == null ? 0 : value;
    }

    private Map<String, Object> parseJsonMap(String json) {
        if (!notBlank(json) || "{}".equals(json)) {
            return new LinkedHashMap<>();
        }
        try {
            Map<?, ?> map = objectMapper.readValue(json, Map.class);
            return map == null ? new LinkedHashMap<>() : castMap(map);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(GOV_PARAM_INVALID + ": 非法 JSON 参数: " + json, e);
        }
    }

    private List<Map<String, Object>> parseJsonList(String json) {
        if (!notBlank(json) || "[]".equals(json)) {
            return new ArrayList<>();
        }
        try {
            List<?> list = objectMapper.readValue(json, List.class);
            List<Map<String, Object>> result = new ArrayList<>();
            if (list != null) {
                for (Object item : list) {
                    if (item instanceof Map<?, ?> map) {
                        result.add(castMap(map));
                    }
                }
            }
            return result;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(GOV_PARAM_INVALID + ": 非法 JSON 参数: " + json, e);
        }
    }

    private Map<String, String> stringParams(Object value) {
        Map<String, String> result = new LinkedHashMap<>();
        if (value instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                result.put(String.valueOf(entry.getKey()), string(entry.getValue()));
            }
        }
        return result;
    }

    private List<String> stringList(Object value) {
        List<String> result = new ArrayList<>();
        if (value instanceof List<?> list) {
            for (Object item : list) {
                result.add(string(item));
            }
        }
        return result;
    }

    private static String jsonOr(Object value, String defaultValue) {
        return value == null ? defaultValue : String.valueOf(value);
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

    private static String required(Map<String, Object> request, String key) {
        String value = string(request.get(key));
        if (!notBlank(value)) {
            throw new IllegalArgumentException(GOV_PARAM_INVALID + ": " + key + " 不能为空");
        }
        return value;
    }

    private static String value(Map<String, Object> request, String key, String defaultValue) {
        String value = string(request.get(key));
        return notBlank(value) ? value : defaultValue;
    }

    private static Long longValue(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value == null || String.valueOf(value).isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Double doubleValue(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (value == null || String.valueOf(value).isBlank()) {
            return null;
        }
        try {
            return Double.parseDouble(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Integer intValue(Object value) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value == null || String.valueOf(value).isBlank()) {
            return null;
        }
        try {
            return Integer.parseInt(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static int intValue(Object value, int defaultValue) {
        Integer parsed = intValue(value);
        return parsed == null ? defaultValue : parsed;
    }

    private static String now() {
        return LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS).toString();
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
        if (value != null) {
            value.forEach((key, item) -> result.put(String.valueOf(key), item));
        }
        return result;
    }

    private static List<Map<String, Object>> castList(Object value) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (value instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof Map<?, ?> map) {
                    result.add(castMap(map));
                }
            }
        }
        return result;
    }
}
