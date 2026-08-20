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
import org.secretflow.secretpad.manager.integration.datatable.DatatableManager;
import org.secretflow.secretpad.manager.integration.model.DatatableDTO;
import org.secretflow.secretpad.persistence.entity.NodeDO;
import org.secretflow.secretpad.persistence.repository.NodeRepository;
import org.secretflow.secretpad.web.service.DataSandboxMvpService;
import org.secretflow.secretpad.web.service.dev.DevDependencyChecker;
import org.secretflow.secretpad.web.service.dev.DevJobExecutor;
import org.secretflow.secretpad.web.service.dev.ModelMetricsEvaluator;
import org.secretflow.secretpad.web.service.governance.CsvUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
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
 * Z-06 模型测试执行服务：审批人配置测试参数、选择测试数据并执行模型；保存测试日志、输入/输出摘要
 * 与评估指标；读取时惰性收官（镜像 ds_dev_task 状态 + 计算指标）。
 *
 * <p>执行链路复用 Z-05 一次性 Kuscia Job：{@code ds_model_test} 关联 {@code ds_dev_task}
 * （{@code channel='model'}，由现有 {@code @Scheduled pollDevTasks} 收尾日志/结果），本服务在
 * 读取/审批门禁时对 RUNNING 测试做惰性收官——SUCCEEDED 时读 {@code result_uri} 结果 CSV，
 * 按 {@code label_column（输入 CSV） × prediction_column（结果 CSV）} 行级 1:1 对齐调用
 * {@link ModelMetricsEvaluator}。</p>
 *
 * <p>测试权限（对 Z-05 owner-only 的明确放宽，报告注明）：模型创建人 或 审批提交人 或 平台管理员；
 * 测试数据集必须属于模型项目（{@code project_datatable}），回退创建人节点/机构判定。</p>
 */
@Slf4j
@Service
public class ModelTestService {

    private static final String STATUS_RUNNING = "RUNNING";
    private static final String STATUS_SUCCEEDED = "SUCCEEDED";
    private static final String STATUS_FAILED = "FAILED";
    private static final String STATUS_CANCELLED = "CANCELLED";
    private static final Set<String> RUN_MODES = Set.of("DEV", "PROD");
    private static final Set<String> METRIC_TYPES = Set.of("auto", "classification", "regression");
    private static final Set<String> TESTABLE_APPROVAL_STATUSES = Set.of("MODEL_REVIEW", "RESOURCE_REVIEW", "APPROVED");

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final DataSandboxMvpService mvp;
    private final DevJobExecutor devJobExecutor;
    private final DatatableManager datatableManager;
    private final NodeRepository nodeRepository;

    @Value("${secretpad.data.dir-path:/app/data/}")
    private String storeDir;

    @Value("${secretpad.data-sandbox.model.test.input-rows:5000}")
    private long maxInputRows;

    @Value("${secretpad.data-sandbox.model.test.input-bytes:262144}")
    private long maxInputBytes;

    @Value("${secretpad.data-sandbox.model.test.max-retries:3}")
    private int maxRetries;

    @Value("${secretpad.data-sandbox.model.test.result-preview-rows:50}")
    private int resultPreviewRows;

    @Value("${secretpad.data-sandbox.model.metrics.classification-distinct-threshold:20}")
    private int classificationDistinctThreshold;

    @Value("${secretpad.data-sandbox.dev.jar-bytes:50331648}")
    private long maxJarBytes;

    public ModelTestService(
            @Qualifier("jdbcTemplate") JdbcTemplate jdbc,
            ObjectMapper objectMapper,
            DataSandboxMvpService mvp,
            DevJobExecutor devJobExecutor,
            DatatableManager datatableManager,
            NodeRepository nodeRepository) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.mvp = mvp;
        this.devJobExecutor = devJobExecutor;
        this.datatableManager = datatableManager;
        this.nodeRepository = nodeRepository;
    }

    /* ============================== 权限 ============================== */

    /**
     * 测试权限：模型创建人 / 审批提交人 / 平台管理员；测试数据集须属于模型项目
     * （{@code project_datatable}），回退创建人平台节点或所属机构判定。
     */
    public void checkTestPermission(UserContextDTO user, Map<String, Object> model, Map<String, Object> approval,
            String nodeId, String datatableId) {
        if (isAdmin(user)) {
            return;
        }
        String actorName = actor(user);
        boolean creator = actorName.equals(string(model.get("created_by")));
        boolean submitter = actorName.equals(string(approval.get("submitter")));
        if (!creator && !submitter) {
            throw new IllegalArgumentException(ModelErrors.MODEL_NO_PERMISSION + ": 仅模型创建人或审批人可执行测试");
        }
        String projectId = string(model.get("project_id"));
        Long inProject = count("select count(1) from project_datatable where project_id=? and node_id=? and datatable_id=? and is_deleted=0",
                projectId, nodeId, datatableId);
        if (inProject > 0) {
            return;
        }
        // 回退：nodeId == 创建人平台节点，或节点所属机构 == 创建人 ownerId
        String owner = string(model.get("created_by_owner"));
        NodeDO node = nodeRepository.findByNodeId(nodeId);
        if (notBlank(owner) && (owner.equals(nodeId) || (node != null && owner.equals(node.getInstId())))) {
            return;
        }
        throw new IllegalArgumentException(ModelErrors.MODEL_NO_PERMISSION
                + ": 测试数据必须属于模型项目或创建人节点");
    }

    /** 查看测试（日志/详情/取消/重试）：测试执行人 / 模型创建人 / 审批提交人 / 管理员。 */
    private void requireTestAccess(Map<String, Object> test, Map<String, Object> model, Map<String, Object> approval) {
        if (isAdmin(null)) {
            return;
        }
        String actorName = actor(null);
        boolean actorIs = actorName.equals(string(test.get("created_by")))
                || actorName.equals(string(model.get("created_by")))
                || actorName.equals(string(approval.get("submitter")));
        if (!actorIs) {
            throw new IllegalArgumentException(ModelErrors.MODEL_NO_PERMISSION + ": 无权访问该测试");
        }
    }

    /* ============================== 执行 ============================== */

    /**
     * 执行模型测试：审批处于 MODEL_REVIEW/RESOURCE_REVIEW/APPROVED；读取测试 CSV 子集（行/字节上限）→
     * 建 ds_dev_task（channel='model'）+ ds_model_test（RUNNING，approval_id 绑定）→ 认领 →
     * {@link DevJobExecutor#submit}（JAR 读盘 base64 / PYTHON 脚本 + 白名单）。
     */
    @Transactional
    public Map<String, Object> executeTest(Map<String, Object> request) {
        String modelId = required(request, "modelId");
        String nodeId = required(request, "nodeId");
        String datatableId = required(request, "datatableId");
        String labelColumn = required(request, "labelColumn");
        String predictionColumn = required(request, "predictionColumn");
        String metricType = string(request.get("metricType"));
        if (!notBlank(metricType)) {
            metricType = "auto";
        }
        if (!METRIC_TYPES.contains(metricType)) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": metricType 必须是 auto/classification/regression");
        }
        String runMode = string(request.get("runMode"));
        if (!notBlank(runMode)) {
            runMode = "DEV";
        }
        if (!RUN_MODES.contains(runMode)) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": runMode 必须是 DEV/PROD");
        }

        Map<String, Object> model = requireModel(modelId);
        Map<String, Object> approval = currentTestableApproval(modelId);
        checkTestPermission(currentUser(), model, approval, nodeId, datatableId);

        Map<String, Object> artifact = requireArtifact(string(model.get("artifact_id")));
        Map<String, Object> version = requireVersion(string(model.get("artifact_id")), string(model.get("artifact_version_id")));
        String execType = string(artifact.get("type"));
        if (!"JAR".equals(execType) && !"PYTHON".equals(execType)) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": 仅 JAR/PYTHON 模型可执行测试");
        }
        DatatableDTO source = resolveSource(nodeId, datatableId);
        String relativeUri = source.getRelativeUri();
        if (!notBlank(relativeUri)) {
            throw new IllegalArgumentException(ModelErrors.MODEL_NOT_FOUND + ": 源数据表缺少 relativeUri");
        }
        List<List<String>> parsed = readInputCsv(source.getNodeId(), relativeUri);
        List<String> header = parsed.isEmpty() ? new ArrayList<>() : new ArrayList<>(parsed.get(0));
        List<List<String>> data = parsed.size() > 1 ? new ArrayList<>(parsed.subList(1, parsed.size())) : new ArrayList<>();
        if (header.isEmpty()) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": 测试 CSV 表头为空");
        }
        if (data.size() > maxInputRows) {
            throw new IllegalArgumentException(ModelErrors.MODEL_INPUT_TOO_LARGE + ": 测试数据行数 " + data.size() + " 超过上限 " + maxInputRows);
        }
        int labelIndex = indexOfColumn(header, labelColumn);
        if (labelIndex < 0) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": 输入 CSV 不含真实列 labelColumn=" + labelColumn);
        }
        Map<String, Object> params = mergedParams(version, request.get("params"));
        String inputB64 = Base64.getEncoder().encodeToString(CsvUtil.toCsv(header, data).getBytes(StandardCharsets.UTF_8));
        if (inputB64.length() > maxInputBytes) {
            throw new IllegalArgumentException(ModelErrors.MODEL_INPUT_TOO_LARGE + ": 测试输入超过 " + maxInputBytes + " 字节上限");
        }

        String jarB64OrScript;
        List<String> allowedImports = List.of();
        if ("JAR".equals(execType)) {
            String filePath = string(version.get("file_path"));
            if (!notBlank(filePath)) {
                throw new IllegalArgumentException(ModelErrors.MODEL_NOT_FOUND + ": 该版本无 JAR 文件");
            }
            byte[] jarBytes = readJar(filePath);
            jarB64OrScript = Base64.getEncoder().encodeToString(jarBytes);
        } else {
            String script = string(version.get("content_text"));
            if (!notBlank(script)) {
                throw new IllegalArgumentException(ModelErrors.MODEL_NOT_FOUND + ": 该版本无脚本内容");
            }
            validatePython(script);
            allowedImports = new ArrayList<>(enabledWhitelist());
            jarB64OrScript = script;
        }
        String taskId = createModelTask(model, version, runMode, execType, nodeId, datatableId, relativeUri, params,
                jarB64OrScript, allowedImports, data.size());
        String testId = createModelTest(modelId, string(approval.get("id")), taskId, runMode, execType, nodeId,
                datatableId, relativeUri, params, labelColumn, predictionColumn, metricType);
        audit("MODEL_TEST_SUBMIT", "MODEL_TEST", testId, "model=" + modelId + " task=" + taskId + " source=" + nodeId + "/" + datatableId, true);
        dispatch("model.test.submitted", Map.of("id", testId, "modelId", modelId, "taskId", taskId, "mode", runMode));
        try {
            claimTask(taskId);
            devJobExecutor.submit(taskId, nodeId, inputB64, execType, jarB64OrScript, params, allowedImports, "model");
        } catch (Exception e) {
            log.warn("Model test {} failed to start: {}", testId, e.getMessage(), e);
            failTask(taskId, e);
            jdbc.update("update ds_model_test set status=?,error_message=?,finished_at=?,updated_at=? where id=? and status=?",
                    STATUS_FAILED, truncate(e.getMessage(), 1900), now(), now(), testId, STATUS_RUNNING);
        }
        return testDetail(testId);
    }

    /* ============================== 惰性收官 ============================== */

    /**
     * 读时收官：测试仍 RUNNING 时镜像关联 ds_dev_task 状态。SUCCEEDED → 读结果 CSV 计算
     * 输入/输出摘要 + 指标（条件 UPDATE 单写者）；FAILED/CANCELLED → 镜像状态与错误。
     */
    public Map<String, Object> finalizeIfNeededAndGet(String testId) {
        Map<String, Object> test = requireTest(testId);
        if (!STATUS_RUNNING.equals(string(test.get("status")))) {
            return enrichTest(test);
        }
        Map<String, Object> task = requireTask(string(test.get("task_id")));
        String taskStatus = string(task.get("status"));
        if (STATUS_SUCCEEDED.equals(taskStatus)) {
            finalizeSuccess(test, task);
        } else if (STATUS_FAILED.equals(taskStatus)) {
            jdbc.update("update ds_model_test set status=?,error_message=?,finished_at=?,updated_at=? where id=? and status=?",
                    STATUS_FAILED, truncate(string(task.get("error_message")), 1900), now(), now(), testId, STATUS_RUNNING);
            audit("MODEL_TEST_FAILED", "MODEL_TEST", testId, truncate(string(task.get("error_message")), 1500), false);
            dispatch("model.test.failed", Map.of("id", testId, "error", truncate(string(task.get("error_message")), 500)));
        } else if (STATUS_CANCELLED.equals(taskStatus)) {
            jdbc.update("update ds_model_test set status=?,finished_at=?,updated_at=? where id=? and status=?",
                    STATUS_CANCELLED, now(), now(), testId, STATUS_RUNNING);
        }
        return enrichTest(requireTest(testId));
    }

    /** 收官某审批下全部 RUNNING 测试（审批门禁调用）。 */
    public void finalizeAllForApproval(String approvalId) {
        List<Map<String, Object>> tests = jdbc.queryForList(
                "select id from ds_model_test where approval_id=? and status=? and deleted=0", approvalId, STATUS_RUNNING);
        for (Map<String, Object> test : tests) {
            try {
                finalizeIfNeededAndGet(string(test.get("id")));
            } catch (Exception e) {
                log.warn("Finalize model test {} failed: {}", test.get("id"), e.getMessage());
            }
        }
    }

    /** 收官某模型下全部 RUNNING 测试（模型详情读时调用）。 */
    public void finalizeAllForModel(String modelId) {
        List<Map<String, Object>> tests = jdbc.queryForList(
                "select id from ds_model_test where model_id=? and status=? and deleted=0", modelId, STATUS_RUNNING);
        for (Map<String, Object> test : tests) {
            try {
                finalizeIfNeededAndGet(string(test.get("id")));
            } catch (Exception e) {
                log.warn("Finalize model test {} failed: {}", test.get("id"), e.getMessage());
            }
        }
    }

    /** 收官成功：读结果 CSV，label_column（输入）× prediction_column（结果）行级 1:1 → 摘要 + 指标。 */
    private void finalizeSuccess(Map<String, Object> test, Map<String, Object> task) {
        String testId = string(test.get("id"));
        String taskId = string(task.get("id"));
        String labelColumn = string(test.get("label_column"));
        String predictionColumn = string(test.get("prediction_column"));
        String metricType = string(test.get("metric_type"));
        try {
            List<List<String>> input = readInputCsv(string(test.get("source_node_id")), string(test.get("source_relative_uri")));
            List<List<String>> output = readResultCsv(task);
            if (output.isEmpty()) {
                throw new IllegalArgumentException(ModelErrors.MODEL_NOT_FOUND + ": 无结果 CSV（result_uri 缺失或不可读）");
            }
            List<String> inputHeader = input.isEmpty() ? new ArrayList<>() : new ArrayList<>(input.get(0));
            List<List<String>> inputData = input.size() > 1 ? new ArrayList<>(input.subList(1, input.size())) : new ArrayList<>();
            List<String> outputHeader = output.isEmpty() ? new ArrayList<>() : new ArrayList<>(output.get(0));
            List<List<String>> outputData = output.size() > 1 ? new ArrayList<>(output.subList(1, output.size())) : new ArrayList<>();

            int labelIndex = indexOfColumn(inputHeader, labelColumn);
            if (labelIndex < 0) {
                throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": 输入 CSV 不含真实列 labelColumn=" + labelColumn);
            }
            int predictionIndex = indexOfColumn(outputHeader, predictionColumn);
            if (predictionIndex < 0) {
                throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": 结果 CSV 不含预测列 predictionColumn=" + predictionColumn);
            }
            List<String> labels = new ArrayList<>();
            for (List<String> row : inputData) {
                labels.add(row.size() > labelIndex ? row.get(labelIndex) : "");
            }
            List<String> predictions = new ArrayList<>();
            for (List<String> row : outputData) {
                predictions.add(row.size() > predictionIndex ? row.get(predictionIndex) : "");
            }
            Map<String, Object> metrics = ModelMetricsEvaluator.evaluate(labels, predictions, metricType, classificationDistinctThreshold);
            String inputSummary = json(Map.of("header", inputHeader, "rowCount", inputData.size(), "columnCount", inputHeader.size()));
            String outputSummary = json(Map.of("header", outputHeader, "rowCount", outputData.size(),
                    "columnCount", outputHeader.size(), "previewRows", previewRows(outputHeader, outputData)));
            String resultPreview = json(Map.of("header", outputHeader, "rows", previewRows(outputHeader, outputData),
                    "resultRows", outputData.size()));
            String now = now();
            int affected = jdbc.update("update ds_model_test set status=?,input_summary=?,output_summary=?,metrics=?,result_preview=?,"
                            + "finished_at=?,updated_at=? where id=? and status=?",
                    STATUS_SUCCEEDED, inputSummary, outputSummary, json(metrics), resultPreview, now, now, testId, STATUS_RUNNING);
            if (affected == 1) {
                String approvalId = string(test.get("approval_id"));
                if (notBlank(approvalId)) {
                    String evidence = json(Map.of("testId", testId, "metrics", metrics, "ranAt", now));
                    jdbc.update("update ds_model_approval set test_evidence=?,updated_at=? where id=?",
                            evidence, now, approvalId);
                }
                audit("MODEL_TEST_SUCCEEDED", "MODEL_TEST", testId,
                        "metricType=" + metrics.get("metricType") + " rows=" + labels.size(), true);
                dispatch("model.test.succeeded", Map.of("id", testId, "taskId", taskId,
                        "metricType", string(metrics.get("metricType")), "rows", labels.size()));
            }
        } catch (Exception e) {
            log.warn("Model test {} finalize failed: {}", testId, e.getMessage(), e);
            jdbc.update("update ds_model_test set status=?,error_message=?,finished_at=?,updated_at=? where id=? and status=?",
                    STATUS_FAILED, truncate(e.getMessage(), 1900), now(), now(), testId, STATUS_RUNNING);
            audit("MODEL_TEST_FAILED", "MODEL_TEST", testId, truncate(e.getMessage(), 1500), false);
            dispatch("model.test.failed", Map.of("id", testId, "error", truncate(e.getMessage(), 500)));
        }
    }

    /* ============================== 查询 / 操作 ============================== */

    public List<Map<String, Object>> listTests(String modelId, String status) {
        StringBuilder sql = new StringBuilder("select * from ds_model_test where deleted=0");
        List<Object> args = new ArrayList<>();
        if (notBlank(modelId)) {
            sql.append(" and model_id=?");
            args.add(modelId);
        }
        if (notBlank(status)) {
            sql.append(" and status=?");
            args.add(status.trim().toUpperCase(Locale.ROOT));
        }
        sql.append(" order by created_at desc limit 500");
        List<Map<String, Object>> rows = jdbc.queryForList(sql.toString(), args.toArray());
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            result.add(finalizeIfNeededAndGet(string(row.get("id"))));
        }
        return result;
    }

    public Map<String, Object> testDetail(String testId) {
        Map<String, Object> test = finalizeIfNeededAndGet(testId);
        Map<String, Object> result = enrichTest(test);
        Map<String, Object> task = requireTask(string(test.get("task_id")));
        List<Map<String, Object>> runLogs = jdbc.queryForList(
                "select id,attempt,length(log_text) as log_len,created_at from ds_dev_run_log where task_id=? order by attempt asc",
                string(task.get("id")));
        result.put("task", Map.of("id", string(task.get("id")), "status", string(task.get("status")),
                "kusciaJobId", string(task.get("kuscia_job_id")), "runMode", string(task.get("run_mode"))));
        result.put("runLogs", runLogs);
        return result;
    }

    /** 调试日志：指定 attempt 返回该次全文；未指定返回全部 attempt 摘要。 */
    public Map<String, Object> testLog(String testId, Integer attempt) {
        Map<String, Object> test = requireTest(testId);
        Map<String, Object> model = requireModel(string(test.get("model_id")));
        Map<String, Object> approval = approvalOf(string(test.get("approval_id")));
        requireTestAccess(test, model, approval);
        String taskId = string(test.get("task_id"));
        if (attempt != null) {
            List<Map<String, Object>> rows = jdbc.queryForList(
                    "select id,attempt,log_text,created_at from ds_dev_run_log where task_id=? and attempt=?",
                    taskId, attempt);
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("taskId", taskId);
            result.put("testId", testId);
            result.put("attempt", attempt);
            if (!rows.isEmpty()) {
                result.put("logText", string(rows.get(0).get("log_text")));
                result.put("createdAt", string(rows.get(0).get("created_at")));
            } else {
                result.put("logText", "");
            }
            return result;
        }
        List<Map<String, Object>> logs = jdbc.queryForList(
                "select id,attempt,log_text,created_at from ds_dev_run_log where task_id=? order by attempt asc", taskId);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("taskId", taskId);
        result.put("testId", testId);
        result.put("attempts", logs);
        return result;
    }

    /** 取消测试：RUNNING 时 stop+delete Job，任务与测试置 CANCELLED。 */
    @Transactional
    public Map<String, Object> cancelTest(String testId) {
        Map<String, Object> test = requireTest(testId);
        Map<String, Object> model = requireModel(string(test.get("model_id")));
        Map<String, Object> approval = approvalOf(string(test.get("approval_id")));
        requireTestAccess(test, model, approval);
        if (!STATUS_RUNNING.equals(string(test.get("status")))) {
            throw new IllegalArgumentException(ModelErrors.MODEL_STATE_CONFLICT + ": 仅 RUNNING 测试可取消");
        }
        String taskId = string(test.get("task_id"));
        Map<String, Object> task = requireTask(taskId);
        String status = string(task.get("status"));
        String jobId = string(task.get("kuscia_job_id"));
        if ("PENDING".equals(status)) {
            jdbc.update("update ds_dev_task set status=?,finished_at=?,updated_at=? where id=? and status=?",
                    STATUS_CANCELLED, now(), now(), taskId, "PENDING");
        } else if (STATUS_RUNNING.equals(status)) {
            if (notBlank(jobId)) {
                devJobExecutor.stop(jobId, "Model test cancelled");
                devJobExecutor.delete(jobId);
            }
            jdbc.update("update ds_dev_task set status=?,finished_at=?,updated_at=? where id=? and status=?",
                    STATUS_CANCELLED, now(), now(), taskId, STATUS_RUNNING);
        } else {
            throw new IllegalArgumentException(ModelErrors.MODEL_STATE_CONFLICT + ": 任务当前状态不可取消: " + status);
        }
        jdbc.update("update ds_model_test set status=?,finished_at=?,updated_at=? where id=? and status=?",
                STATUS_CANCELLED, now(), now(), testId, STATUS_RUNNING);
        audit("MODEL_TEST_CANCEL", "MODEL_TEST", testId, "", true);
        dispatch("model.test.cancelled", Map.of("id", testId));
        return testDetail(testId);
    }

    /** 重试：FAILED 且 retry_count<max → 重建运行（复用 task 行，attempt=retry_count）。 */
    @Transactional
    public Map<String, Object> retryTest(String testId) {
        Map<String, Object> test = requireTest(testId);
        Map<String, Object> model = requireModel(string(test.get("model_id")));
        Map<String, Object> approval = approvalOf(string(test.get("approval_id")));
        requireTestAccess(test, model, approval);
        if (!STATUS_FAILED.equals(string(test.get("status")))) {
            throw new IllegalArgumentException(ModelErrors.MODEL_STATE_CONFLICT + ": 仅 FAILED 测试可重试");
        }
        String taskId = string(test.get("task_id"));
        Map<String, Object> task = requireTask(taskId);
        int retries = intValue(task.get("retry_count"), 0);
        if (retries >= maxRetries) {
            throw new IllegalArgumentException(ModelErrors.MODEL_STATE_CONFLICT + ": 重试次数已达上限 " + maxRetries);
        }
        String nodeId = string(task.get("source_node_id"));
        String datatableId = string(task.get("source_datatable_id"));
        String relativeUri = string(task.get("source_relative_uri"));
        checkTestPermission(currentUser(), model, approval, nodeId, datatableId);
        List<List<String>> parsed = readInputCsv(nodeId, relativeUri);
        List<String> header = parsed.isEmpty() ? new ArrayList<>() : new ArrayList<>(parsed.get(0));
        List<List<String>> data = parsed.size() > 1 ? new ArrayList<>(parsed.subList(1, parsed.size())) : new ArrayList<>();
        if (data.size() > maxInputRows) {
            throw new IllegalArgumentException(ModelErrors.MODEL_INPUT_TOO_LARGE + ": 测试数据行数 " + data.size() + " 超过上限 " + maxInputRows);
        }
        Map<String, Object> params = parseJsonMap(string(task.get("params")));
        String execType = string(task.get("exec_type"));
        String inputB64 = Base64.getEncoder().encodeToString(CsvUtil.toCsv(header, data).getBytes(StandardCharsets.UTF_8));
        if (inputB64.length() > maxInputBytes) {
            throw new IllegalArgumentException(ModelErrors.MODEL_INPUT_TOO_LARGE + ": 测试输入超过 " + maxInputBytes + " 字节上限");
        }
        Map<String, Object> artifact = requireArtifact(string(model.get("artifact_id")));
        Map<String, Object> version = requireVersion(string(model.get("artifact_id")), string(model.get("artifact_version_id")));
        String jarB64OrScript;
        List<String> allowedImports = List.of();
        if ("JAR".equals(execType)) {
            jarB64OrScript = Base64.getEncoder().encodeToString(readJar(string(version.get("file_path"))));
        } else {
            String script = string(version.get("content_text"));
            validatePython(script);
            allowedImports = new ArrayList<>(enabledWhitelist());
            jarB64OrScript = script;
        }
        jdbc.update("update ds_dev_task set retry_count=retry_count+1,error_message='',kuscia_job_id='',started_at=?,status=?,updated_at=? where id=? and status=?",
                now(), STATUS_RUNNING, now(), taskId, STATUS_FAILED);
        jdbc.update("update ds_model_test set status=?,error_message='',started_at=?,finished_at='',updated_at=? where id=? and status=?",
                STATUS_RUNNING, now(), now(), testId, STATUS_FAILED);
        audit("MODEL_TEST_RETRY", "MODEL_TEST", testId, "retry=" + (retries + 1), true);
        dispatch("model.test.retried", Map.of("id", testId, "retry", retries + 1));
        try {
            devJobExecutor.submit(taskId, nodeId, inputB64, execType, jarB64OrScript, params, allowedImports, "model");
        } catch (Exception e) {
            log.warn("Model test {} retry failed: {}", testId, e.getMessage(), e);
            failTask(taskId, e);
            jdbc.update("update ds_model_test set status=?,error_message=?,finished_at=?,updated_at=? where id=? and status=?",
                    STATUS_FAILED, truncate(e.getMessage(), 1900), now(), now(), testId, STATUS_RUNNING);
        }
        return testDetail(testId);
    }

    /* ============================== 内部 ============================== */

    private String createModelTask(Map<String, Object> model, Map<String, Object> version, String runMode,
            String execType, String nodeId, String datatableId, String relativeUri, Map<String, Object> params,
            String contentSnapshot, List<String> dependencyNames, int sourceRows) {
        String taskId = "dt-" + shortId();
        String now = now();
        jdbc.update("insert into ds_dev_task(id,name,description,artifact_id,version,run_mode,exec_type,source_node_id,"
                        + "source_datatable_id,source_relative_uri,params,content_snapshot,dependency_names,channel,status,result_node_id,"
                        + "result_datatable_id,result_preview,result_uri,source_rows,result_rows,error_message,kuscia_job_id,retry_count,"
                        + "created_by,created_at,updated_at,started_at,finished_at,deleted)"
                        + " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,'PENDING','','','','',?,0,'','',0,?,?,?,?,'',0)",
                taskId, "模型测试-" + taskId, "", string(model.get("artifact_id")), intValue(model.get("version"), 1),
                runMode, execType, nodeId, datatableId, relativeUri, json(params), contentSnapshot,
                json(dependencyNames), "model", sourceRows, actor(null), now, now, now);
        return taskId;
    }

    private String createModelTest(String modelId, String approvalId, String taskId, String runMode, String execType,
            String nodeId, String datatableId, String relativeUri, Map<String, Object> params, String labelColumn,
            String predictionColumn, String metricType) {
        String testId = "mt-" + shortId();
        String now = now();
        jdbc.update("insert into ds_model_test(id,model_id,approval_id,task_id,run_mode,exec_type,source_node_id,"
                        + "source_datatable_id,source_relative_uri,params,label_column,prediction_column,metric_type,status,"
                        + "input_summary,output_summary,metrics,result_preview,error_message,created_by,created_at,updated_at,"
                        + "started_at,finished_at,deleted)"
                        + " values(?,?,?,?,?,?,?,?,?,?,?,?,?,'RUNNING','{}','{}','{}','','',?,?,?,?,'',0)",
                testId, modelId, approvalId, taskId, runMode, execType, nodeId, datatableId, relativeUri,
                json(params), labelColumn, predictionColumn, metricType, actor(null), now, now, now);
        return testId;
    }

    private void claimTask(String taskId) {
        int affected = jdbc.update("update ds_dev_task set status=?,started_at=?,updated_at=? where id=? and status=?",
                STATUS_RUNNING, now(), now(), taskId, "PENDING");
        if (affected != 1) {
            throw new IllegalStateException(ModelErrors.MODEL_STATE_CONFLICT + ": 任务状态已变更，无法开始执行: " + taskId);
        }
    }

    private void failTask(String taskId, Exception e) {
        jdbc.update("update ds_dev_task set status=?,error_message=?,finished_at=?,updated_at=? where id=? and status=?",
                STATUS_FAILED, truncate(e.getMessage(), 1900), now(), now(), taskId, STATUS_RUNNING);
        mvp.raiseAlert("WARNING", "DATA_MODEL", "模型测试执行失败",
                "任务 " + taskId + "：" + truncate(e.getMessage(), 900), "model:" + taskId + ":failed");
    }

    /** 当前进行中的审批单（MODEL_REVIEW/RESOURCE_REVIEW/APPROVED 才可测试）。 */
    Map<String, Object> currentTestableApproval(String modelId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select * from ds_model_approval where model_id=? and status in (?,?,?) order by submitted_at desc limit 1",
                modelId, "MODEL_REVIEW", "RESOURCE_REVIEW", "APPROVED");
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(ModelErrors.MODEL_STATE_CONFLICT
                    + ": 模型无进行中的审批单，无法测试（先提交审批）");
        }
        if (!TESTABLE_APPROVAL_STATUSES.contains(string(rows.get(0).get("status")))) {
            throw new IllegalArgumentException(ModelErrors.MODEL_STATE_CONFLICT
                    + ": 审批状态 " + string(rows.get(0).get("status")) + " 不可测试");
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    private Map<String, Object> approvalOf(String approvalId) {
        if (!notBlank(approvalId)) {
            return Map.of();
        }
        List<Map<String, Object>> rows = jdbc.queryForList("select * from ds_model_approval where id=?", approvalId);
        return rows.isEmpty() ? Map.of() : new LinkedHashMap<>(rows.get(0));
    }

    private Map<String, Object> requireModel(String id) {
        List<Map<String, Object>> rows = jdbc.queryForList("select * from ds_model where id=? and deleted=0", id);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(ModelErrors.MODEL_NOT_FOUND + ": 模型不存在: " + id);
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

    private Map<String, Object> requireVersion(String artifactId, String versionId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select * from ds_dev_artifact_version where id=? and artifact_id=? and deleted=0", versionId, artifactId);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(ModelErrors.MODEL_NOT_FOUND + ": 制品版本不存在: " + artifactId + "/" + versionId);
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    private Map<String, Object> requireTest(String id) {
        List<Map<String, Object>> rows = jdbc.queryForList("select * from ds_model_test where id=? and deleted=0", id);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(ModelErrors.MODEL_NOT_FOUND + ": 测试记录不存在: " + id);
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    private Map<String, Object> requireTask(String id) {
        List<Map<String, Object>> rows = jdbc.queryForList("select * from ds_dev_task where id=? and deleted=0", id);
        if (rows.isEmpty()) {
            throw new IllegalStateException(ModelErrors.MODEL_NOT_FOUND + ": 任务不存在: " + id);
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    /** 富化：metrics/摘要 解析为对象便于前端直接渲染。 */
    private Map<String, Object> enrichTest(Map<String, Object> test) {
        Map<String, Object> result = new LinkedHashMap<>(test);
        Object metricsObj = parseJsonValue(string(test.get("metrics")));
        if (metricsObj instanceof Map<?, ?> m && !m.isEmpty()) {
            result.put("metrics", metricsObj);
        }
        Object inputObj = parseJsonValue(string(test.get("input_summary")));
        if (inputObj instanceof Map<?, ?> m && !m.isEmpty()) {
            result.put("inputSummary", m);
        }
        Object outputObj = parseJsonValue(string(test.get("output_summary")));
        if (outputObj instanceof Map<?, ?> m && !m.isEmpty()) {
            result.put("outputSummary", m);
        }
        return result;
    }

    private List<List<String>> readInputCsv(String nodeId, String relativeUri) {
        if (relativeUri.contains("..")) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": 非法路径");
        }
        Path base = Path.of(storeDir, nodeId).toAbsolutePath().normalize();
        Path target = base.resolve(relativeUri).normalize();
        if (!target.startsWith(base)) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": 非法路径");
        }
        try {
            return CsvUtil.parse(Files.readString(target, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new IllegalStateException(ModelErrors.MODEL_NOT_FOUND + ": 读取源 CSV 失败: " + e.getMessage(), e);
        }
    }

    private List<List<String>> readResultCsv(Map<String, Object> task) {
        String resultUri = string(task.get("result_uri"));
        String nodeId = string(task.get("result_node_id"));
        if (!notBlank(resultUri) || !notBlank(nodeId) || resultUri.contains("..")) {
            return new ArrayList<>();
        }
        Path base = Path.of(storeDir, nodeId).toAbsolutePath().normalize();
        Path target = base.resolve(resultUri).normalize();
        if (!target.startsWith(base)) {
            return new ArrayList<>();
        }
        try {
            return CsvUtil.parse(Files.readString(target, StandardCharsets.UTF_8));
        } catch (IOException e) {
            log.warn("Model result read failed for {}: {}", task.get("id"), e.getMessage());
            return new ArrayList<>();
        }
    }

    byte[] readJar(String filePath) {
        if (!notBlank(filePath) || filePath.contains("..")) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": 非法 JAR 路径");
        }
        Path base = Path.of(storeDir).toAbsolutePath().normalize();
        Path target = base.resolve(filePath).normalize();
        if (!target.startsWith(base)) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": 非法 JAR 路径");
        }
        try {
            byte[] bytes = Files.readAllBytes(target);
            if (bytes.length > maxJarBytes) {
                throw new IllegalArgumentException(ModelErrors.MODEL_INPUT_TOO_LARGE + ": JAR " + bytes.length + " 字节超过上限 " + maxJarBytes);
            }
            return bytes;
        } catch (IOException e) {
            throw new IllegalStateException(ModelErrors.MODEL_NOT_FOUND + ": 读取 JAR 文件失败: " + e.getMessage(), e);
        }
    }

    void validatePython(String script) {
        try {
            DevDependencyChecker.validate(script, enabledWhitelist());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(ModelErrors.MODEL_DEPENDENCY_REJECTED
                    + e.getMessage().replaceFirst("^" + java.util.regex.Pattern.quote("DEV_DEPENDENCY_REJECTED"), ""));
        }
    }

    Set<String> enabledWhitelist() {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select name from ds_dev_dependency where deleted=0 and enabled=1");
        Set<String> result = new java.util.HashSet<>();
        for (Map<String, Object> row : rows) {
            result.add(string(row.get("name")).toLowerCase(Locale.ROOT));
        }
        return result;
    }

    /** 请求参数覆盖版本 default_params 后作为执行参数。 */
    Map<String, Object> mergedParams(Map<String, Object> version, Object requestParams) {
        Map<String, Object> params = new LinkedHashMap<>();
        Map<String, Object> defaults = parseJsonMap(string(version.get("default_params")));
        params.putAll(defaults);
        if (requestParams instanceof Map<?, ?> map) {
            params.putAll(castMap(map));
        }
        return params;
    }

    private DatatableDTO resolveSource(String nodeId, String datatableId) {
        return datatableManager.findById(DatatableDTO.NodeDatatableId.from(nodeId, datatableId))
                .orElseThrow(() -> new IllegalArgumentException(ModelErrors.MODEL_NOT_FOUND + ": 数据表不存在: " + nodeId + "/" + datatableId));
    }

    private int indexOfColumn(List<String> header, String column) {
        for (int i = 0; i < header.size(); i++) {
            if (header.get(i).trim().equals(column.trim())) {
                return i;
            }
        }
        return -1;
    }

    private List<List<String>> previewRows(List<String> header, List<List<String>> rows) {
        List<List<String>> preview = new ArrayList<>();
        for (int i = 0; i < Math.min(resultPreviewRows, rows.size()); i++) {
            preview.add(new ArrayList<>(rows.get(i)));
        }
        return preview;
    }

    private Object parseJsonValue(String json) {
        if (!notBlank(json) || "{}".equals(json) || "[]".equals(json)) {
            return null;
        }
        try {
            return objectMapper.readValue(json, Object.class);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    /* ============================== 审计 / 辅助 ============================== */

    private void audit(String action, String resourceType, String resourceId, String detail, boolean success) {
        mvp.auditAs("OPERATION", success ? "INFO" : "ERROR", actor(null), action, resourceType, resourceId, detail, success);
    }

    private void dispatch(String event, Map<String, Object> payload) {
        mvp.dispatchWebhooks(event, payload);
    }

    private UserContextDTO currentUser() {
        return UserContext.getUserOrNotExist();
    }

    private static boolean isAdmin(UserContextDTO user) {
        return user != null && "kuscia-system".equals(user.getOwnerId()) && "admin".equals(user.getName());
    }

    private static String actor(UserContextDTO user) {
        UserContextDTO current = user != null ? user : UserContext.getUserOrNotExist();
        return current == null || !notBlank(current.getName()) ? "system" : current.getName();
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

    private Map<String, Object> parseJsonMap(String json) {
        if (!notBlank(json) || "{}".equals(json)) {
            return new LinkedHashMap<>();
        }
        try {
            Map<?, ?> map = objectMapper.readValue(json, Map.class);
            return map == null ? new LinkedHashMap<>() : castMap(map);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(ModelErrors.MODEL_PARAM_INVALID + ": 非法 JSON 参数: " + json, e);
        }
    }

    private static Map<String, Object> castMap(Map<?, ?> value) {
        Map<String, Object> result = new LinkedHashMap<>();
        if (value != null) {
            value.forEach((key, item) -> result.put(String.valueOf(key), item));
        }
        return result;
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

    private static String truncate(String value, int max) {
        String safe = string(value);
        return safe.length() <= max ? safe : safe.substring(0, max);
    }
}
