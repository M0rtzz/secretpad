/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.service.dev;

import org.secretflow.secretpad.common.constant.DomainDataConstants;
import org.secretflow.secretpad.common.constant.DomainDatasourceConstants;
import org.secretflow.secretpad.common.dto.UserContextDTO;
import org.secretflow.secretpad.common.util.UUIDUtils;
import org.secretflow.secretpad.common.util.UserContext;
import org.secretflow.secretpad.kuscia.v1alpha1.service.impl.KusciaGrpcClientAdapter;
import org.secretflow.secretpad.web.service.DataSandboxMvpService;
import org.secretflow.secretpad.web.service.SandboxDataControlService;
import org.secretflow.secretpad.web.service.governance.CsvUtil;
import org.secretflow.secretpad.web.service.storage.SandboxDbService;

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
import java.util.Base64;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Z-05 计算任务运行组件：JAR / PYTHON 通过一次性 Kuscia Job 运行，输入子集 base64 内联进
 * {@code task_input_config}，AppImage 按 {@code exec_type} 选择 jar/python-runner（scope=Cluster 结果端口），
 * 轮询 {@code queryJob} 取回容器输出 CSV + 调试日志，写 {@code ds_dev_run_log}。
 *
 * <p>运行模式区分：PROD 正式运行注册结果 Kuscia DomainData + 血缘；DEV 调试运行仅存结果预览与日志
 * （不注册结果表、不产生血缘）。查询/拉取必须在 deleteJob 之前。</p>
 *
 * <p>执行隔离：一次性 Job、无卷无密钥、仅 task_input_config 入参、CPU/内存限额、超时 kill（stopJob）
 * + 跑完即删（deleteJob）、网络策略 GOVERNANCE（无 Cluster 端口时容器不可达）。</p>
 */
@Slf4j
@Component
public class DevJobExecutor {

    private static final String STATUS_RUNNING = "RUNNING";
    private static final String NETWORK_POLICY = "GOVERNANCE";
    private static final long MAX_RESULT_BYTES = 8 * 1024 * 1024;

    private static final String ATTR_DATASOURCE_TYPE = "DatasourceType";
    private static final String ATTR_DATASOURCE_NAME = "DatasourceName";
    private static final String ATTR_DESC = "description";

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final KusciaGrpcClientAdapter kuscia;
    private final DataSandboxMvpService mvp;
    private final SandboxDbService sandboxDb;
    private final SandboxDataControlService dataControl;

    @Value("${secretpad.data.dir-path:/app/data/}")
    private String storeDir;

    @Value("${secretpad.gateway:127.0.0.1:80}")
    private String gateway;

    @Value("${secretpad.data-sandbox.dev.input-bytes:262144}")
    private long maxInputBytes;

    @Value("${secretpad.data-sandbox.dev.sandbox-db-bytes:20971520}")
    private long maxSandboxDbBytes;

    @Value("${secretpad.data-sandbox.dev.timeout-seconds:300}")
    private long timeoutSeconds;

    @Value("${secretpad.data-sandbox.dev.cpu:0.5}")
    private String cpu;

    @Value("${secretpad.data-sandbox.dev.memory:512Mi}")
    private String memory;

    @Value("${secretpad.data-sandbox.dev.jar-app-image:data-sandbox-jar-runner}")
    private String jarAppImage;

    @Value("${secretpad.data-sandbox.dev.python-app-image:data-sandbox-python-runner}")
    private String pythonAppImage;

    @Value("${secretpad.data-sandbox.dev.result-preview-rows:50}")
    private int resultPreviewRows;

    @Value("${secretpad.data-sandbox.model.api.poll-interval-ms:500}")
    private long apiPollIntervalMs;

    @Value("${secretpad.data-sandbox.kuscia.enabled:false}")
    private boolean kusciaEnabled;

    public DevJobExecutor(
            @Qualifier("jdbcTemplate") JdbcTemplate jdbc,
            ObjectMapper objectMapper,
            KusciaGrpcClientAdapter kuscia,
            DataSandboxMvpService mvp,
            SandboxDbService sandboxDb,
            SandboxDataControlService dataControl) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.kuscia = kuscia;
        this.mvp = mvp;
        this.sandboxDb = sandboxDb;
        this.dataControl = dataControl;
    }

    /* ------------------------------- 提交 ------------------------------- */

    /**
     * 提交 JAR/PYTHON 计算任务（channel 默认 'dev'，兼容 Z-05 调用方）。
     *
     * @see #submit(String, String, String, String, String, Map, List, String)
     */
    public void submit(String taskId, String nodeId, String inputB64, String execType,
            String jarB64OrScript, Map<String, Object> params, List<String> allowedImports) {
        submit(taskId, nodeId, inputB64, execType, jarB64OrScript, params, allowedImports, "dev");
    }

    /**
     * 提交 JAR/PYTHON 计算任务：输入子集 + jar 字节/脚本内联进 task_input_config 后拉起一次性 Kuscia Job。
     * 失败抛异常，由调用方（DataDevService / ModelTestService / ModelApiService）置 FAILED。
     *
     * @param channel 执行通道：dev（Z-05 默认，调度器轮询）/ model（模型测试，调度器轮询）/ api（invoke，runAndAwait 同步收官）
     */
    public void submit(String taskId, String nodeId, String inputB64, String execType,
            String jarB64OrScript, Map<String, Object> params, List<String> allowedImports, String channel) {
        doSubmit(taskId, nodeId, inputB64, execType, jarB64OrScript, params, allowedImports, channel, null);
    }

    /**
     * 沙箱表源任务（Stage 4）：与 {@link #submit} 相同的 CSV base64 回退通道，额外在
     * {@code task_input_config} 注入沙箱库 JDBC 契约（{@code jdbc_url}/{@code input_table}/
     * {@code output_table}）与整库快照 {@code sandbox_db_b64}，供升级版 runner 直连
     * {@code /workspace/sandbox_data.db} 计算；DB 快照缺失（sandboxId 为空）时 runner
     * 自动回退旧 CSV 模式。
     */
    public void submitSandbox(String taskId, String nodeId, String inputB64, String execType,
            String jarB64OrScript, Map<String, Object> params, List<String> allowedImports,
            String sandboxId, String inputTable, String outputTable) {
        submitSandboxChannel(taskId, nodeId, inputB64, execType, jarB64OrScript, params, allowedImports,
                sandboxId, inputTable, outputTable, Set.of(inputTable), "dev");
    }

    /**
     * 沙箱表源任务（画布节点通道）：与 {@link #submitSandbox} 相同，额外指定 channel（'canvas'）。
     * channel 由 SandboxCanvasService 使用，result CSV 照常落盘（persistResult 含 canvas），
     * 供画布层回填 op_* 输出表与自动注册模型；不注册 DomainData、不产生血缘。
     */
    public void submitSandboxChannel(String taskId, String nodeId, String inputB64, String execType,
            String jarB64OrScript, Map<String, Object> params, List<String> allowedImports,
            String sandboxId, String inputTable, String outputTable, Set<String> allowedTables, String channel) {
        Map<String, Object> extra = new LinkedHashMap<>();
        extra.put("jdbc_url", "jdbc:sqlite:/workspace/sandbox_data.db");
        extra.put("input_table", inputTable);
        if (notBlank(outputTable)) {
            extra.put("output_table", outputTable);
        }
        if (notBlank(sandboxId)) {
            byte[] db = sandboxDb.executionSnapshotBytes(sandboxId, allowedTables);
            if (db.length > maxSandboxDbBytes) {
                throw new IllegalStateException(DevErrors.DEV_INPUT_TOO_LARGE
                        + ": 沙箱数据库超过 " + maxSandboxDbBytes + " 字节上限（当前 "
                        + db.length + " 字节）");
            }
            extra.put("sandbox_db_b64", Base64.getEncoder().encodeToString(db));
        }
        doSubmit(taskId, nodeId, inputB64, execType, jarB64OrScript, params, allowedImports, channel, extra);
    }

    /**
     * 携带预置 SQLite 快照提交计算任务（函数 API 调用通道）：与 {@link #submitSandboxChannel} 的
     * 快照机制一致，但 DB 字节由调用方按「调用方输入行」构造（不再按 sandboxId 打包整库），
     * channel='api' 由 runAndAwait 同步收官。
     */
    public void submitWithSnapshot(String taskId, String nodeId, String inputB64, String execType,
            String jarB64OrScript, Map<String, Object> params, List<String> allowedImports,
            String channel, byte[] dbBytes, String inputTable, String outputTable) {
        Map<String, Object> extra = new LinkedHashMap<>();
        extra.put("jdbc_url", "jdbc:sqlite:/workspace/sandbox_data.db");
        extra.put("input_table", inputTable);
        if (notBlank(outputTable)) {
            extra.put("output_table", outputTable);
        }
        if (dbBytes != null && dbBytes.length > 0) {
            if (dbBytes.length > maxSandboxDbBytes) {
                throw new IllegalStateException(DevErrors.DEV_INPUT_TOO_LARGE
                        + ": 沙箱数据库超过 " + maxSandboxDbBytes + " 字节上限（当前 " + dbBytes.length + " 字节）");
            }
            extra.put("sandbox_db_b64", Base64.getEncoder().encodeToString(dbBytes));
        }
        doSubmit(taskId, nodeId, inputB64, execType, jarB64OrScript, params, allowedImports, channel, extra);
    }

    private void doSubmit(String taskId, String nodeId, String inputB64, String execType,
            String jarB64OrScript, Map<String, Object> params, List<String> allowedImports, String channel,
            Map<String, Object> extraConfig) {
        if (!kusciaEnabled) {
            throw new IllegalStateException(DevErrors.DEV_PARAM_INVALID + ": Kuscia 运行时未启用，无法执行 " + execType + " 任务");
        }
        if (!notBlank(jarB64OrScript)) {
            throw new IllegalStateException(DevErrors.DEV_PARAM_INVALID + ": 缺少运行载荷（JAR 字节或脚本）");
        }
        if (!notBlank(inputB64) || inputB64.length() > maxInputBytes) {
            throw new IllegalStateException(DevErrors.DEV_INPUT_TOO_LARGE
                    + ": 输入数据超过 " + maxInputBytes + " 字节上限");
        }
        boolean jar = "JAR".equals(execType);
        Map<String, Object> config = new LinkedHashMap<>();
        if (jar) {
            config.put("jar_b64", jarB64OrScript);
        } else {
            config.put("script", jarB64OrScript);
            config.put("allowed_imports", allowedImports == null ? List.of() : allowedImports);
        }
        config.put("input_csv_b64", inputB64);
        config.put("params", params == null ? new LinkedHashMap<>() : params);
        if (extraConfig != null) {
            config.putAll(extraConfig);
        }
        String taskInputConfig = json(config);

        String jobId = "dt-" + taskId;
        String kusciaTaskId = jobId + "-task";
        Job.Party party = Job.Party.newBuilder().setDomainId(nodeId).setRole("server")
                .setResources(Job.JobResource.newBuilder().setCpu(cpu).setMemory(memory)).build();
        Job.Task task = Job.Task.newBuilder().setTaskId(kusciaTaskId).setAlias("data-dev")
                .setAppImage(jar ? jarAppImage : pythonAppImage).addParties(party)
                .setTaskInputConfig(taskInputConfig).build();
        Job.CreateJobResponse response;
        try {
            response = kuscia.createJob(Job.CreateJobRequest.newBuilder().setJobId(jobId).setInitiator(nodeId)
                    .setMaxParallelism(1).addTasks(task)
                    .putCustomFields("task_id", taskId)
                    .putCustomFields("network_policy", NETWORK_POLICY)
                    .build());
        } catch (Exception e) {
            throw new IllegalStateException("创建 " + execType + " 执行容器失败: " + truncate(e.getMessage(), 900), e);
        }
        if (response.getStatus().getCode() != 0) {
            throw new IllegalStateException("创建 " + execType + " 执行容器失败: " + response.getStatus().getMessage());
        }
        jdbc.update("update ds_dev_task set kuscia_job_id=?,channel=?,updated_at=? where id=? and status=?",
                jobId, channel == null ? "dev" : channel, now(), taskId, STATUS_RUNNING);
        log.info("Dev {} task {} submitted as Kuscia job {} channel={}", execType, taskId, jobId, channel);
    }

    /** 停止计算 Job（幂等，jobId 为空返回 ""）。取消/超时终止复用。 */
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

    public String delete(String jobId) {
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

    /* ------------------------------- 同步执行（API 调用） ------------------------------- */

    /**
     * Z-06 同步执行直至任务终态（channel='api' invoke 使用，调度器不轮询该通道）。
     *
     * <p>循环复用幂等 {@link #pollDevTask}（不复制任何 finalize 逻辑），任务进入终态即返回
     * {@code {status, header, rows, errorMessage, elapsedMs}}；SUCCEEDED 时优先读结果 CSV
     * 全量行（{@code result_uri}），无结果文件时回退结果预览。死线超时按 cancelTask 语义
     * stop+delete+fail。</p>
     *
     * @param taskId 已创建为 RUNNING 的 ds_dev_task 行
     */
    public Map<String, Object> runAndAwait(String taskId) {
        long startNanos = System.nanoTime();
        Map<String, Object> task = requireTask(taskId);
        while (STATUS_RUNNING.equals(string(task.get("status")))) {
            try {
                pollDevTask(task);
            } catch (Exception e) {
                log.warn("Dev invoke poll failed for {}: {}", taskId, e.getMessage());
            }
            task = requireTask(taskId);
            if (STATUS_RUNNING.equals(string(task.get("status")))) {
                if (isTimeout(string(task.get("started_at")))) {
                    String jobId = string(task.get("kuscia_job_id"));
                    String stopError = stop(jobId, "Model invoke timeout");
                    fail(taskId, "API 调用执行超时（" + timeoutSeconds + "s）"
                            + (notBlank(stopError) ? "，停止失败: " + stopError : ""), "model:" + taskId + ":timeout");
                    delete(jobId);
                    task = requireTask(taskId);
                    break;
                }
                sleepQuietly(apiPollIntervalMs);
            }
        }
        return syncResult(task, startNanos);
    }

    /** 组装同步执行结果：SUCCEEDED 读结果 CSV 全量行，其余返回状态/错误/耗时。 */
    private Map<String, Object> syncResult(Map<String, Object> task, long startNanos) {
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", string(task.get("status")));
        result.put("errorMessage", string(task.get("error_message")));
        result.put("elapsedMs", elapsedMs);
        List<String> header = new ArrayList<>();
        List<List<String>> rows = new ArrayList<>();
        if (STATUS_RUNNING.equals(string(task.get("status"))) || "SUCCEEDED".equals(string(task.get("status")))) {
            List<List<String>> parsed = readResultCsv(task);
            if (!parsed.isEmpty()) {
                header = new ArrayList<>(parsed.get(0));
                rows = parsed.size() > 1 ? new ArrayList<>(parsed.subList(1, parsed.size())) : new ArrayList<>();
            } else {
                Map<String, Object> preview = parsePreview(string(task.get("result_preview")));
                @SuppressWarnings("unchecked")
                List<Object> previewHeader = preview.get("header") instanceof List<?> list ? (List<Object>) list : new ArrayList<>();
                header = new ArrayList<>();
                for (Object col : previewHeader) {
                    header.add(string(col));
                }
                @SuppressWarnings("unchecked")
                List<Object> previewRows = preview.get("rows") instanceof List<?> list ? (List<Object>) list : new ArrayList<>();
                for (Object row : previewRows) {
                    List<String> rowValues = new ArrayList<>();
                    if (row instanceof List<?> list) {
                        for (Object cell : list) {
                            rowValues.add(string(cell));
                        }
                    }
                    rows.add(rowValues);
                }
            }
        }
        result.put("header", header);
        result.put("rows", rows);
        return result;
    }

    /** 读结果 CSV 全量（canonical 路径安全；无 result_uri/不可读返回空列表）。 */
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
        } catch (java.io.IOException e) {
            log.warn("Dev result read failed for {}: {}", task.get("id"), e.getMessage());
            return new ArrayList<>();
        }
    }

    private Map<String, Object> parsePreview(String json) {
        try {
            if (!notBlank(json) || "{}".equals(json)) {
                return new LinkedHashMap<>();
            }
            Map<?, ?> map = objectMapper.readValue(json, Map.class);
            Map<String, Object> result = new LinkedHashMap<>();
            if (map != null) {
                map.forEach((key, value) -> result.put(String.valueOf(key), value));
            }
            return result;
        } catch (JsonProcessingException e) {
            return new LinkedHashMap<>();
        }
    }

    private Map<String, Object> requireTask(String id) {
        List<Map<String, Object>> rows = jdbc.queryForList("select * from ds_dev_task where id=? and deleted=0", id);
        if (rows.isEmpty()) {
            throw new IllegalStateException(DevErrors.DEV_NOT_FOUND + ": 任务不存在: " + id);
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /* ------------------------------- 轮询取回 ------------------------------- */

    @Scheduled(fixedDelayString = "${secretpad.data-sandbox.dev.poll-interval-ms:10000}")
    public void pollDevTasks() {
        if (!kusciaEnabled) {
            return;
        }
        // Z-06：channel='api' 的 invoke 任务由 runAndAwait 同步收官，调度器绝不轮询（避免双收官）。
        // canvas 节点任务由 SandboxCanvasService runAndAwait 收官，调度器兜底轮询（后台线程异常退出时防止悬挂）。
        List<Map<String, Object>> tasks = jdbc.queryForList(
                "select * from ds_dev_task where deleted=0 and status=? and kuscia_job_id<>'' and channel in ('dev','model','canvas')",
                STATUS_RUNNING);
        for (Map<String, Object> task : tasks) {
            try {
                pollDevTask(task);
            } catch (Exception e) {
                log.warn("Dev task poll failed for {}: {}", task.get("id"), e.getMessage(), e);
            }
        }
    }

    private void pollDevTask(Map<String, Object> task) {
        String taskId = string(task.get("id"));
        String jobId = string(task.get("kuscia_job_id"));
        Job.QueryJobResponse response;
        try {
            response = kuscia.queryJob(Job.QueryJobRequest.newBuilder().setJobId(jobId).build());
        } catch (Exception e) {
            fail(taskId, "查询计算 Job 失败: " + truncate(e.getMessage(), 900), "dev:" + taskId + ":failed");
            delete(jobId);
            return;
        }
        if (response.getStatus().getCode() != 0) {
            fail(taskId, "Kuscia Job 已不存在: " + response.getStatus().getMessage(), "dev:" + taskId + ":failed");
            delete(jobId);
            return;
        }
        String state = effectiveKusciaState(response);
        if (state.contains("SUCCEED")) {
            finalizeSuccess(task, jobId, response);
        } else if (state.contains("FAIL") || state.contains("REJECTED")) {
            fail(taskId, "执行容器失败: " + state + (notBlank(response.getData().getStatus().getErrMsg())
                    ? " " + response.getData().getStatus().getErrMsg() : ""), "dev:" + taskId + ":failed");
            delete(jobId);
        } else {
            // RUNNING/PENDING：容器常驻服务，输出就绪即完成（取回后 stopJob/deleteJob 终止）
            if (finalizeIfReady(task, jobId)) {
                return;
            }
            String startedAt = string(task.get("started_at"));
            if (notBlank(startedAt) && isTimeout(startedAt)) {
                String stopError = stop(jobId, "Dev task timeout");
                fail(taskId, "计算任务执行超时（" + timeoutSeconds + "s）" + (notBlank(stopError) ? "，停止失败: " + stopError : ""),
                        "dev:" + taskId + ":timeout");
                delete(jobId);
            } else {
                // 心跳：仍在运行，刷新 updated_at
                jdbc.update("update ds_dev_task set updated_at=? where id=?", now(), taskId);
            }
        }
    }

    /** 任务仍 Running 但结果已可取（endpoint 存在且 /result 是有效 CSV）→ 提前完成。 */
    private boolean finalizeIfReady(Map<String, Object> task, String jobId) {
        String taskId = string(task.get("id"));
        Job.QueryJobResponse response;
        try {
            response = kuscia.queryJob(Job.QueryJobRequest.newBuilder().setJobId(jobId).build());
        } catch (Exception e) {
            return false;
        }
        if (response.getStatus().getCode() != 0) {
            return false;
        }
        String endpoint = extractEndpoint(response, portName(string(task.get("exec_type"))));
        if (endpoint.isEmpty()) {
            return false;
        }
        // runner 失败时容器不退出：/status 返回 "failed"，保持提供 /log 供取回失败原因。
        byte[] statusBody = fetchOutput(endpoint, "/status");
        if (statusBody == null) {
            return false; // 端点未就绪（容器启动 / 脚本执行中）
        }
        String runnerStatus = new String(statusBody, StandardCharsets.UTF_8).trim();
        if ("failed".equalsIgnoreCase(runnerStatus)) {
            String logText = fetchLog(endpoint);
            String reason = extractFailureReason(logText);
            fail(taskId, "执行容器失败: " + (notBlank(reason) ? reason : "脚本执行失败"),
                    "dev:" + taskId + ":failed");
            appendRunLog(taskId, retryCount(task), logText);
            delete(jobId);
            return true;
        }
        byte[] body = fetchOutput(endpoint, "/result");
        if (body == null) {
            return false;
        }
        completeSuccess(task, jobId, endpoint, body);
        return true;
    }

    /** 从 run.log 提取明确的失败原因（ImportError / 超时 / rc 等），优先取 EXECUTION FAILED 行。 */
    static String extractFailureReason(String logText) {
        if (notBlank(logText)) {
            String fallback = "";
            for (String line : logText.split("\\n")) {
                String t = line.trim();
                if (t.isEmpty()) {
                    continue;
                }
                if (t.contains("EXECUTION FAILED")) {
                    return truncate(t.replaceFirst("^\\[[^]]*\\] EXECUTION FAILED: ", ""), 300);
                }
                if (t.contains("ImportError") || t.contains("SyntaxError") || t.contains("failed rc=")
                        || t.contains("timed out")) {
                    fallback = truncate(t, 300);
                }
            }
            if (notBlank(fallback)) {
                return fallback;
            }
        }
        return "";
    }

    private void finalizeSuccess(Map<String, Object> task, String jobId, Job.QueryJobResponse response) {
        String taskId = string(task.get("id"));
        String endpoint = extractEndpoint(response, portName(string(task.get("exec_type"))));
        if (endpoint.isEmpty()) {
            fail(taskId, "执行容器未暴露结果端点（" + portName(string(task.get("exec_type"))) + "/Cluster）", "dev:" + taskId + ":failed");
            delete(jobId);
            return;
        }
        byte[] body = fetchOutput(endpoint, "/result");
        if (body == null) {
            fail(taskId, "取回容器输出失败: " + endpoint, "dev:" + taskId + ":failed");
            delete(jobId);
            return;
        }
        completeSuccess(task, jobId, endpoint, body);
    }

    private void completeSuccess(Map<String, Object> task, String jobId, String endpoint, byte[] body) {
        String taskId = string(task.get("id"));
        String nodeId = string(task.get("source_node_id"));
        String runMode = string(task.get("run_mode"));
        String channel = string(task.get("channel"));
        // Z-06/Z-07：model/api/canvas 通道需要结果 CSV 供指标计算/调用取数/画布回填——即使 runMode=DEV 也落盘
        boolean persistResult = "PROD".equals(runMode) || "model".equals(channel)
                || "api".equals(channel) || "canvas".equals(channel);
        String csv = new String(body, StandardCharsets.UTF_8);
        List<List<String>> parsed = CsvUtil.parse(csv);
        if (parsed.isEmpty()) {
            fail(taskId, "容器输出为空或不是有效 CSV", "dev:" + taskId + ":failed");
            delete(jobId);
            return;
        }
        List<String> header = new ArrayList<>(parsed.get(0));
        List<List<String>> rows = parsed.size() > 1 ? new ArrayList<>(parsed.subList(1, parsed.size())) : new ArrayList<>();
        String preview = previewJson(header, rows, resultPreviewRows);
        appendRunLog(taskId, retryCount(task), fetchLog(endpoint));

        try {
            // 沙箱结果与权限登记必须早于 SUCCEEDED，避免出现可见结果缺少控制记录。
            backfillSandboxResultIfNeeded(task, header, rows);
        } catch (Exception e) {
            fail(taskId, "沙箱结果登记失败: " + e.getMessage(), "dev:" + taskId + ":result-control-failed");
            delete(jobId);
            return;
        }

        if ("PROD".equals(runMode)) {
            String resultUri = writeResultCsv(nodeId, taskId, header, rows);
            String domainDataId = registerResultDomainData(nodeId, taskId, resultUri, header);
            int affected = jdbc.update("update ds_dev_task set status=?,result_node_id=?,result_datatable_id=?,result_uri=?,result_preview=?,"
                            + "source_rows=?,result_rows=?,finished_at=?,updated_at=? where id=? and status=?",
                    "SUCCEEDED", nodeId, domainDataId, resultUri, preview, num(task.get("source_rows")), rows.size(), now(), now(), taskId, STATUS_RUNNING);
            if (affected == 1) {
                insertLineage(taskId, string(task.get("source_node_id")), string(task.get("source_datatable_id")),
                        nodeId, domainDataId);
            }
            audit("DEV_TASK_SUCCEEDED", "DEV_TASK", taskId,
                    "mode=PROD rows=" + num(task.get("source_rows")) + "->" + rows.size() + " result=" + domainDataId, true);
            dispatch("dev.task.succeeded", Map.of("id", taskId, "sourceRows", num(task.get("source_rows")),
                    "resultRows", rows.size(), "resultDatatableId", domainDataId));
        } else if (persistResult) {
            // model/api 通道：落盘结果 CSV（供模型测试指标 / API 调用取数），不注册 DomainData、不产生血缘
            String resultUri = writeResultCsv(nodeId, taskId, header, rows);
            jdbc.update("update ds_dev_task set status=?,result_node_id=?,result_uri=?,result_preview=?,"
                            + "source_rows=?,result_rows=?,finished_at=?,updated_at=? where id=? and status=?",
                    "SUCCEEDED", nodeId, resultUri, preview, num(task.get("source_rows")), rows.size(), now(), now(), taskId, STATUS_RUNNING);
            audit("DEV_TASK_RESULT_PERSISTED", "DEV_TASK", taskId,
                    "channel=" + channel + " rows=" + num(task.get("source_rows")) + "->" + rows.size(), true);
            dispatch("dev.task.succeeded", Map.of("id", taskId, "sourceRows", num(task.get("source_rows")),
                    "resultRows", rows.size(), "channel", channel));
        } else {
            // DEV 调试运行：仅存结果预览 + 日志，不注册结果表、不产生血缘
            jdbc.update("update ds_dev_task set status=?,result_preview=?,result_rows=?,finished_at=?,updated_at=? where id=? and status=?",
                    "SUCCEEDED", preview, rows.size(), now(), now(), taskId, STATUS_RUNNING);
            audit("DEV_TASK_DEBUG_SUCCEEDED", "DEV_TASK", taskId,
                    "mode=DEV rows=" + num(task.get("source_rows")) + "->" + rows.size(), true);
            dispatch("dev.task.debugSucceeded", Map.of("id", taskId, "sourceRows", num(task.get("source_rows")),
                    "resultRows", rows.size()));
        }
        // 查询/拉取完成后删除 Job（幂等）
        delete(jobId);
    }

    /** 沙箱表源任务 PROD 成功：结果表写入 sandbox_data.db 并登记 RESULT 清单/目录。 */
    private void backfillSandboxResultIfNeeded(Map<String, Object> task, List<String> header, List<List<String>> rows) {
        String sandboxId = string(task.get("sandbox_id"));
        if (!notBlank(sandboxId) || !"PROD".equals(string(task.get("run_mode")))) {
            return;
        }
        String taskId = string(task.get("id"));
        String resultTable = string(sandboxDb.backfillResultTable(sandboxId, taskId,
                string(task.get("name")), header, rows).get("tableName"));
        dataControl.registerResultControl(taskId, sandboxId, resultTable);
        jdbc.update("update ds_dev_task set result_table_name=? where id=?", resultTable, taskId);
        log.info("沙箱任务 {} 结果已回填沙箱库: {}", taskId, resultTable);
    }

    private void fail(String taskId, String errorMessage, String dedupeKey) {
        jdbc.update("update ds_dev_task set status=?,error_message=?,finished_at=?,updated_at=? where id=? and status=?",
                "FAILED", truncate(errorMessage, 1900), now(), now(), taskId, STATUS_RUNNING);
        mvp.raiseAlert("WARNING", "DATA_DEV", "计算任务执行失败",
                "任务 " + taskId + "：" + truncate(errorMessage, 900), dedupeKey);
        audit("DEV_TASK_FAILED", "DEV_TASK", taskId, truncate(errorMessage, 1500), false);
        dispatch("dev.task.failed", Map.of("id", taskId, "error", truncate(errorMessage, 500)));
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
                    log.warn("Dev output fetch from {} returned {}", endpoint, response.getStatusLine().getStatusCode());
                    return null;
                }
                if (response.getEntity() == null) {
                    return null;
                }
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                response.getEntity().writeTo(buffer);
                if (buffer.size() > MAX_RESULT_BYTES) {
                    log.warn("Dev output from {} exceeds {} bytes", endpoint, MAX_RESULT_BYTES);
                    return null;
                }
                return buffer.toByteArray();
            }
        } catch (Exception e) {
            log.warn("Dev output fetch from {} failed: {}", endpoint, e.getMessage());
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
            throw new IllegalStateException(DevErrors.DEV_PARAM_INVALID + ": 非法结果路径");
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
                .setName("dev-" + taskId)
                .setType("table")
                .setFileFormat(Common.FileFormat.CSV)
                .setDatasourceId(DomainDatasourceConstants.DEFAULT_DATASOURCE)
                .putAttributes(ATTR_DATASOURCE_TYPE, DomainDataConstants.DEFAULT_LOCAL_DATASOURCE_TYPE)
                .putAttributes(ATTR_DATASOURCE_NAME, DomainDataConstants.DEFAULT_LOCAL_DATASOURCE_NAME)
                .putAttributes(DomainDataConstants.NULL_STRS, "[]")
                .putAttributes(ATTR_DESC, "数据开发结果：" + taskId)
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
        // Z-05 血缘由 ds_dev_task 行派生（source_* -> result_*），无需独立血缘表；
        // 这里通过审计记录事件，便于 ds_unified_log 检索。
        audit("DEV_TASK_LINEAGE", "DEV_TASK", taskId,
                sourceNodeId + "/" + sourceDatatableId + " -> " + targetNodeId + "/" + targetDatatableId, true);
    }

    /* ------------------------------- 调试日志 / 预览 ------------------------------- */

    private String fetchLog(String endpoint) {
        byte[] body = fetchOutput(endpoint, "/log");
        return body == null ? "" : new String(body, StandardCharsets.UTF_8);
    }

    private void appendRunLog(String taskId, int attempt, String logText) {
        jdbc.update("insert into ds_dev_run_log(id,task_id,attempt,log_text,created_at) values(?,?,?,?,?)",
                "dl-" + shortId(), taskId, attempt, truncate(logText, 64000), now());
    }

    private String previewJson(List<String> header, List<List<String>> rows, int limit) {
        List<List<String>> previewRows = new ArrayList<>();
        for (int i = 0; i < Math.min(limit, rows.size()); i++) {
            previewRows.add(new ArrayList<>(rows.get(i)));
        }
        return json(Map.of("header", header, "rows", previewRows, "resultRows", rows.size()));
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

    private static String portName(String execType) {
        return "JAR".equals(execType) ? "jar" : "py";
    }

    private static int retryCount(Map<String, Object> task) {
        Object value = task.get("retry_count");
        return value instanceof Number number ? number.intValue() : 0;
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
