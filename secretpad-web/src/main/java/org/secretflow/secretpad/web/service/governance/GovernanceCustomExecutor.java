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
import org.secretflow.secretpad.web.service.DataSandboxMvpService;
import org.secretflow.secretpad.web.service.DataAssetService;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.secretflow.v1alpha1.common.Common;
import org.secretflow.v1alpha1.kusciaapi.Domaindata;
import org.secretflow.v1alpha1.kusciaapi.Job;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Z-04 自定义代码执行组件：把授权的输入子集以 base64 内联进 {@code task_input_config}，
 * 在一次性 Kuscia Job（AppImage {@code data-sandbox-sampler}，scope=Cluster 结果端口）中运行用户脚本，
 * 轮询 {@code queryJob} 取回容器输出 CSV，注册为结果数据集后删除 Job。
 *
 * <p>执行隔离（E2E 可验证）：一次性 Job、无卷无密钥、仅 task_input_config 入参、CPU/内存限额、
 * 超时 kill（stopJob）+ 跑完即删（deleteJob）、网络策略 GOVERNANCE（无 Cluster 端口时容器不可达）。</p>
 *
 * <p>结果取回：scope=Cluster 端点为 {@code *.svc} 时经 {@code secretpad.gateway}（Kuscia envoy :80，
 * Host 头路由）转发；测试/本机端点（形如 {@code host:port}）直连。查询/拉取必须在 deleteJob 之前。</p>
 */
@Slf4j
@Component
public class GovernanceCustomExecutor {

    private static final String STATUS_RUNNING = "RUNNING";
    private static final String STATUS_SUCCEEDED = "SUCCEEDED";
    private static final String STATUS_FAILED = "FAILED";
    private static final String ENDPOINT_PORT = "sampler";
    private static final String NETWORK_POLICY = "GOVERNANCE";
    private static final long MAX_RESULT_BYTES = 8 * 1024 * 1024;

    private static final String ATTR_DATASOURCE_TYPE = "DatasourceType";
    private static final String ATTR_DATASOURCE_NAME = "DatasourceName";
    private static final String ATTR_DESC = "description";

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final KusciaGrpcClientAdapter kuscia;
    private final DataSandboxMvpService mvp;
    private final DataAssetService dataAssetService;

    @Value("${secretpad.data.dir-path:/app/data/}")
    private String storeDir;

    @Value("${secretpad.gateway:127.0.0.1:80}")
    private String gateway;

    @Value("${secretpad.data-sandbox.governance.input-bytes:262144}")
    private long maxInputBytes;

    @Value("${secretpad.data-sandbox.governance.timeout-seconds:300}")
    private long timeoutSeconds;

    @Value("${secretpad.data-sandbox.governance.cpu:0.5}")
    private String cpu;

    @Value("${secretpad.data-sandbox.governance.memory:512Mi}")
    private String memory;

    @Value("${secretpad.data-sandbox.governance.app-image:data-sandbox-sampler}")
    private String appImage;

    @Value("${secretpad.data-sandbox.kuscia.enabled:false}")
    private boolean kusciaEnabled;

    public GovernanceCustomExecutor(
            @Qualifier("jdbcTemplate") JdbcTemplate jdbc,
            ObjectMapper objectMapper,
            KusciaGrpcClientAdapter kuscia,
            DataSandboxMvpService mvp,
            DataAssetService dataAssetService) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.kuscia = kuscia;
        this.mvp = mvp;
        this.dataAssetService = dataAssetService;
    }

    /* ------------------------------- 提交 ------------------------------- */

    /**
     * 提交自定义代码任务：输入子集 base64 内联进 task_input_config 后拉起一次性 Kuscia Job。
     * 失败抛异常，由调用方（DataGovernanceService）置 FAILED。
     */
    public void submit(String taskId, String nodeId, String inputB64,
            String script, Map<String, Object> params) {
        if (!kusciaEnabled) {
            throw new IllegalStateException(DataGovernanceService.GOV_PARAM_INVALID + ": Kuscia 运行时未启用，无法执行自定义代码");
        }
        if (!notBlank(script)) {
            throw new IllegalStateException(DataGovernanceService.GOV_PARAM_INVALID + ": 自定义代码任务缺少 script");
        }
        if (!notBlank(inputB64) || inputB64.length() > maxInputBytes) {
            throw new IllegalStateException(DataGovernanceService.GOV_INPUT_TOO_LARGE
                    + ": 输入数据超过 " + maxInputBytes + " 字节上限");
        }
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("script", script);
        config.put("input_csv_b64", inputB64);
        config.put("params", params == null ? new LinkedHashMap<>() : params);
        String taskInputConfig = json(config);

        String jobId = "gov-" + taskId;
        String kusciaTaskId = jobId + "-task";
        Job.Party party = Job.Party.newBuilder().setDomainId(nodeId).setRole("server")
                .setResources(Job.JobResource.newBuilder().setCpu(cpu).setMemory(memory)).build();
        Job.Task task = Job.Task.newBuilder().setTaskId(kusciaTaskId).setAlias("governance")
                .setAppImage(appImage).addParties(party).setTaskInputConfig(taskInputConfig).build();
        Job.CreateJobResponse response;
        try {
            response = kuscia.createJob(Job.CreateJobRequest.newBuilder().setJobId(jobId).setInitiator(nodeId)
                    .setMaxParallelism(1).addTasks(task)
                    .putCustomFields("task_id", taskId)
                    .putCustomFields("network_policy", NETWORK_POLICY)
                    .build());
        } catch (Exception e) {
            throw new IllegalStateException("创建治理执行容器失败: " + truncate(e.getMessage(), 900), e);
        }
        if (response.getStatus().getCode() != 0) {
            throw new IllegalStateException("创建治理执行容器失败: " + response.getStatus().getMessage());
        }
        jdbc.update("update ds_governance_task set kuscia_job_id=?,updated_at=? where id=? and status=?",
                jobId, now(), taskId, STATUS_RUNNING);
        log.info("Governance custom task {} submitted as Kuscia job {}", taskId, jobId);
    }

    /** 停止治理 Job（幂等，jobId 为空返回 ""）。取消/超时终止复用。 */
    public String stop(String jobId, String reason) {
        if (!notBlank(jobId) || !kusciaEnabled) {
            return "";
        }
        try {
            Job.StopJobResponse response = kuscia.stopJob(Job.StopJobRequest.newBuilder().setJobId(jobId).setReason(reason).build());
            return response.getStatus().getCode() == 0 ? "" : response.getStatus().getMessage();
        } catch (Exception e) {
            return truncate(e.getMessage(), 900);
        }
    }

    private String delete(String jobId) {
        if (!notBlank(jobId)) {
            return "";
        }
        try {
            Job.DeleteJobResponse response = kuscia.deleteJob(Job.DeleteJobRequest.newBuilder().setJobId(jobId).build());
            return response.getStatus().getCode() == 0 ? "" : response.getStatus().getMessage();
        } catch (Exception e) {
            return truncate(e.getMessage(), 900);
        }
    }

    /* ------------------------------- 轮询取回 ------------------------------- */

    @Scheduled(fixedDelayString = "${secretpad.data-sandbox.governance.poll-interval-ms:10000}")
    public void pollCustomTasks() {
        if (!kusciaEnabled) {
            return;
        }
        List<Map<String, Object>> tasks = jdbc.queryForList(
                "select * from ds_governance_task where deleted=0 and exec_mode='CUSTOM' and status=? and kuscia_job_id<>''",
                STATUS_RUNNING);
        for (Map<String, Object> task : tasks) {
            try {
                pollCustomTask(task);
            } catch (Exception e) {
                log.warn("Governance custom poll failed for task {}: {}", task.get("id"), e.getMessage(), e);
            }
        }
    }

    private void pollCustomTask(Map<String, Object> task) {
        String taskId = string(task.get("id"));
        String jobId = string(task.get("kuscia_job_id"));
        Job.QueryJobResponse response;
        try {
            response = kuscia.queryJob(Job.QueryJobRequest.newBuilder().setJobId(jobId).build());
        } catch (Exception e) {
            fail(taskId, "查询治理 Job 失败: " + truncate(e.getMessage(), 900), "gov:" + taskId + ":failed");
            delete(jobId);
            return;
        }
        if (response.getStatus().getCode() != 0) {
            fail(taskId, "Kuscia Job 已不存在: " + response.getStatus().getMessage(), "gov:" + taskId + ":failed");
            delete(jobId);
            return;
        }
        String state = effectiveKusciaState(response);
        if (state.contains("SUCCEED")) {
            finalizeSuccess(task, jobId, response);
        } else if (state.contains("FAIL") || state.contains("REJECTED")) {
            fail(taskId, "执行容器失败: " + state + (notBlank(response.getData().getStatus().getErrMsg())
                    ? " " + response.getData().getStatus().getErrMsg() : ""), "gov:" + taskId + ":failed");
            delete(jobId);
        } else {
            // RUNNING/PENDING：容器常驻服务，输出就绪即完成（取回后 stopJob/deleteJob 终止）
            if (finalizeIfReady(task, jobId)) {
                return;
            }
            String startedAt = string(task.get("started_at"));
            if (notBlank(startedAt) && isTimeout(startedAt)) {
                String stopError = stop(jobId, "Governance custom task timeout");
                fail(taskId, "自定义代码执行超时（" + timeoutSeconds + "s）" + (notBlank(stopError) ? "，停止失败: " + stopError : ""),
                        "gov:" + taskId + ":timeout");
                delete(jobId);
            } else {
                // 心跳：仍在运行，刷新 updated_at
                jdbc.update("update ds_governance_task set updated_at=? where id=?", now(), taskId);
            }
        }
    }

    /** 任务仍 Running 但结果已可取（endpoint 存在且 /result 是有效 CSV）→ 提前完成。 */
    private boolean finalizeIfReady(Map<String, Object> task, String jobId) {
        Job.QueryJobResponse response;
        try {
            response = kuscia.queryJob(Job.QueryJobRequest.newBuilder().setJobId(jobId).build());
        } catch (Exception e) {
            return false;
        }
        if (response.getStatus().getCode() != 0) {
            return false;
        }
        String endpoint = extractEndpoint(response, ENDPOINT_PORT);
        if (endpoint.isEmpty()) {
            return false;
        }
        byte[] body = fetchOutput(endpoint, "/result");
        if (body == null) {
            return false;
        }
        completeSuccess(task, jobId, body);
        return true;
    }

    private void finalizeSuccess(Map<String, Object> task, String jobId, Job.QueryJobResponse response) {
        String taskId = string(task.get("id"));
        String nodeId = string(task.get("source_node_id"));
        String endpoint = extractEndpoint(response, ENDPOINT_PORT);
        if (endpoint.isEmpty()) {
            fail(taskId, "执行容器未暴露结果端点（sampler/Cluster）", "gov:" + taskId + ":failed");
            delete(jobId);
            return;
        }
        byte[] body = fetchOutput(endpoint, "/result");
        if (body == null) {
            fail(taskId, "取回容器输出失败: " + endpoint, "gov:" + taskId + ":failed");
            delete(jobId);
            return;
        }
        completeSuccess(task, jobId, body);
    }

    private void completeSuccess(Map<String, Object> task, String jobId, byte[] body) {
        String taskId = string(task.get("id"));
        String nodeId = string(task.get("source_node_id"));
        String csv = new String(body, StandardCharsets.UTF_8);
        List<List<String>> parsed = CsvUtil.parse(csv);
        if (parsed.isEmpty()) {
            fail(taskId, "容器输出为空或不是有效 CSV", "gov:" + taskId + ":failed");
            delete(jobId);
            return;
        }
        List<String> header = new ArrayList<>(parsed.get(0));
        List<List<String>> rows = parsed.size() > 1 ? new ArrayList<>(parsed.subList(1, parsed.size())) : new ArrayList<>();
        byte[] resultBody = body;
        try {
            List<GovernanceMaskingExecutor.MaskRule> rules = maskingRules(task);
            if (!rules.isEmpty()) {
                for (GovernanceMaskingExecutor.MaskRule rule : rules) {
                    if (!header.contains(rule.column())) {
                        throw new IllegalArgumentException("自定义抽样输出缺少待脱敏字段: " + rule.column());
                    }
                }
                GovernanceMaskingExecutor.MaskResult masked = GovernanceMaskingExecutor.apply(header, rows, rules);
                header = masked.header();
                rows = masked.rows();
                resultBody = CsvUtil.toCsv(header, rows).getBytes(StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            fail(taskId, "自定义抽样结果脱敏失败: " + truncate(e.getMessage(), 900), "gov:" + taskId + ":failed");
            delete(jobId);
            return;
        }
        Map<String,Object> resultAsset=dataAssetService.registerGovernedResult(taskId,nodeId,resultBody);
        String domainDataId=string(resultAsset.get("datatable_id"));
        int affected = jdbc.update("update ds_governance_task set status=?,result_node_id=?,result_datatable_id=?,"
                        + "source_rows=?,result_rows=?,finished_at=?,updated_at=? where id=? and status=?",
                STATUS_SUCCEEDED, nodeId, domainDataId, num(task.get("source_rows")), rows.size(), now(), now(), taskId, STATUS_RUNNING);
        if (affected == 1) {
            insertLineage(taskId, string(task.get("source_node_id")), string(task.get("source_datatable_id")),
                    nodeId, domainDataId);
        }
        audit("GOVERNANCE_TASK_SUCCEEDED", "GOVERNANCE_TASK", taskId,
                "rows=" + num(task.get("source_rows")) + "->" + rows.size() + " result=" + domainDataId, true);
        dispatch("governance.task.succeeded", Map.of("id", taskId, "sourceRows", num(task.get("source_rows")),
                "resultRows", rows.size(), "resultDatatableId", domainDataId));
        // 查询/拉取完成后删除 Job（幂等）
        delete(jobId);
    }

    /** 从任务快照恢复字段脱敏规则，供自定义脚本输出回收后执行。 */
    private List<GovernanceMaskingExecutor.MaskRule> maskingRules(Map<String, Object> task) {
        List<GovernanceMaskingExecutor.MaskRule> rules = new ArrayList<>();
        String execParams = string(task.get("exec_params"));
        if (!notBlank(execParams)) {
            return rules;
        }
        try {
            Map<?, ?> snapshot = objectMapper.readValue(execParams, Map.class);
            Object maskingValue = snapshot == null ? null : snapshot.get("masking");
            if (!(maskingValue instanceof List<?> masking)) {
                return rules;
            }
            for (Object value : masking) {
                if (!(value instanceof Map<?, ?> item)) {
                    continue;
                }
                String column = string(item.get("column"));
                String method = string(item.get("method"));
                if (!notBlank(column) || !notBlank(method)) {
                    throw new IllegalArgumentException("脱敏规则缺少 column/method");
                }
                Map<String, String> params = new LinkedHashMap<>();
                if (item.get("params") instanceof Map<?, ?> rawParams) {
                    rawParams.forEach((key, param) -> params.put(String.valueOf(key), string(param)));
                }
                rules.add(new GovernanceMaskingExecutor.MaskRule(column, method, params));
            }
            return rules;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("任务脱敏快照不是合法 JSON", e);
        }
    }

    private void fail(String taskId, String errorMessage, String dedupeKey) {
        jdbc.update("update ds_governance_task set status=?,error_message=?,finished_at=?,updated_at=? where id=? and status=?",
                STATUS_FAILED, truncate(errorMessage, 1900), now(), now(), taskId, STATUS_RUNNING);
        mvp.raiseAlert("WARNING", "GOVERNANCE", "数据治理自定义任务失败",
                "任务 " + taskId + "：" + truncate(errorMessage, 900), dedupeKey);
        audit("GOVERNANCE_TASK_FAILED", "GOVERNANCE_TASK", taskId, truncate(errorMessage, 1500), false);
        dispatch("governance.task.failed", Map.of("id", taskId, "error", truncate(errorMessage, 500)));
    }

    private boolean isTimeout(String startedAt) {
        try {
            LocalDateTime start = LocalDateTime.parse(startedAt);
            return LocalDateTime.now().isAfter(start.plusSeconds(timeoutSeconds));
        } catch (Exception e) {
            return false;
        }
    }

    /* ------------------------------- 结果取回 ------------------------------- */

    /** scope=Cluster 端点经 gateway（Host 头路由）转发；本机/测试端点直连。 */
    private byte[] fetchOutput(String endpoint, String path) {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            String target = "http://" + (isClusterService(endpoint) ? gateway : endpoint) + path;
            var builder = RequestBuilder.create("GET").setUri(target)
                    .setConfig(RequestConfig.custom()
                            .setConnectTimeout(5_000)
                            .setConnectionRequestTimeout(5_000)
                            .setSocketTimeout(30_000)
                            .build());
            if (isClusterService(endpoint)) {
                builder.setHeader("Host", endpoint);
            }
            try (CloseableHttpResponse response = client.execute(builder.build())) {
                if (response.getStatusLine().getStatusCode() != 200) {
                    log.warn("Governance output fetch from {} returned {}", endpoint, response.getStatusLine().getStatusCode());
                    return null;
                }
                if (response.getEntity() == null) {
                    return null;
                }
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                response.getEntity().writeTo(buffer);
                if (buffer.size() > MAX_RESULT_BYTES) {
                    log.warn("Governance output from {} exceeds {} bytes", endpoint, MAX_RESULT_BYTES);
                    return null;
                }
                return buffer.toByteArray();
            }
        } catch (Exception e) {
            log.warn("Governance output fetch from {} failed: {}", endpoint, e.getMessage());
            return null;
        }
    }

    private boolean isClusterService(String endpoint) {
        return endpoint != null && (endpoint.endsWith(".svc") || endpoint.contains(".svc:"));
    }

    private String extractEndpoint(Job.QueryJobResponse response, String portName) {
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

    private String writeResultCsv(String nodeId, String taskId, List<String> header, List<List<String>> rows) {
        String resultUri = taskId + "-" + shortId() + ".csv";
        Path base = Path.of(storeDir, nodeId).toAbsolutePath().normalize();
        try {
            Files.createDirectories(base);
        } catch (java.io.IOException e) {
            throw new IllegalStateException("无法创建结果目录: " + e.getMessage(), e);
        }
        Path target = base.resolve(resultUri).normalize();
        if (!target.startsWith(base)) {
            throw new IllegalStateException(DataGovernanceService.GOV_PARAM_INVALID + ": 非法结果路径");
        }
        try {
            Files.writeString(target, CsvUtil.toCsv(header, rows), StandardCharsets.UTF_8);
        } catch (java.io.IOException e) {
            throw new IllegalStateException("写入结果 CSV 失败: " + e.getMessage(), e);
        }
        return resultUri;
    }

    /** 结果数据集注册为 Kuscia DomainData（type=table, CSV）；输出列可能被脚本变换，类型统一按 str。 */
    private String registerResultDomainData(String nodeId, String taskId, String relativeUri, List<String> header) {
        String domainDataId = UUIDUtils.random(8);
        List<Common.DataColumn> columns = new ArrayList<>();
        for (String col : header) {
            columns.add(Common.DataColumn.newBuilder().setName(col).setType("str").setComment("").build());
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

    private void insertLineage(String taskId, String sourceNodeId, String sourceDatatableId,
            String targetNodeId, String targetDatatableId) {
        jdbc.update("insert into ds_governance_lineage(task_id,source_node_id,source_datatable_id,target_node_id,target_datatable_id,op_type,created_by,created_at,deleted)"
                        + " values(?,?,?,?,?,?,?,?,0)",
                taskId, sourceNodeId, sourceDatatableId, targetNodeId, targetDatatableId,
                "CUSTOM", actor(), now());
    }

    /* ------------------------------- 审计 / 辅助 ------------------------------- */

    private void audit(String action, String resourceType, String resourceId, String detail, boolean success) {
        mvp.auditAs("OPERATION", success ? "INFO" : "ERROR", actor(), action, resourceType, resourceId, detail, success);
    }

    private void dispatch(String event, Map<String, Object> payload) {
        mvp.dispatchWebhooks(event, payload);
    }

    private String actor() {
        UserContextDTO user = UserContext.getUserOrNotExist();
        return user == null || !notBlank(user.getName()) ? "system" : user.getName();
    }

    private String json(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            return String.valueOf(value);
        }
    }

    private static long num(Object value) {
        return value instanceof Number number ? number.longValue() : 0;
    }

    private static String string(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }

    private static String shortId() {
        return java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    private static String truncate(String value, int max) {
        String safe = string(value);
        return safe.length() <= max ? safe : safe.substring(0, max);
    }

    private static String now() {
        return LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS).toString();
    }
}
