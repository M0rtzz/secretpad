/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.service.canvas;

import org.secretflow.secretpad.common.dto.UserContextDTO;
import org.secretflow.secretpad.common.util.UserContext;
import org.secretflow.secretpad.web.service.DataSandboxMvpService;
import org.secretflow.secretpad.web.service.SandboxDataControlService;
import org.secretflow.secretpad.web.service.dev.DataDevService;
import org.secretflow.secretpad.web.service.dev.DevJobExecutor;
import org.secretflow.secretpad.web.service.dev.ModelMetricsEvaluator;
import org.secretflow.secretpad.web.service.model.ModelApprovalService;
import org.secretflow.secretpad.web.service.storage.SandboxDbService;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 可视化建模画布执行引擎（智能建模与服务化闭环之「画布」）。
 *
 * <p>执行模型：画布节点 = kuscia python 任务（channel='canvas'，v2-ml 镜像），复用
 * {@link DevJobExecutor#submitSandboxChannel}/{@code runAndAwait}：
 * <ol>
 *   <li>解析 {@code ds_compute_canvas.graph_json} → 拓扑排序（检测环）；</li>
 *   <li>逐节点渲染 {@code import modeling_ops} 脚本（虚拟节点 data.table 不执行，直接映射挂载表）；</li>
 *   <li>上游输出表 {@code op_{runId}_{nodeId}} 落沙箱库（{@link SandboxDbService#backfillOperatorTable}）；</li>
 *   <li>ml.* 训练节点成功 → joblib(base64) 注册为 {@code source='CANVAS'} 的制品与模型（对开发制品列表不可见），
 *       并回填 {@code ds_compute_node_run.model_id}，供工作流模型保存与 API 发布；</li>
 *   <li>{@code ds_compute_run / ds_compute_node_run} 记录整图/节点状态，节点输出/日志对前端可见。</li>
 * </ol>
 *
 * <p>边界：result_* 表不可作画布输入；op_* 表仅画布内部消费（{@link DataDevService} 已拒绝作 dev 源）。</p>
 */
@Slf4j
@Service
public class SandboxCanvasService {

    /** 画布自动注册产物的来源标记：开发制品/任务列表据此隔离。 */
    private static final String ARTIFACT_SOURCE_CANVAS = "CANVAS";

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final DataDevService dataDevService;
    private final DevJobExecutor devJobExecutor;
    private final SandboxDbService sandboxDb;
    private final DataSandboxMvpService mvp;
    private final ModelApprovalService modelApprovalService;
    private final SandboxDataControlService dataControl;

    private final ExecutorService canvasExecutor = Executors.newSingleThreadExecutor();

    public SandboxCanvasService(
            @Qualifier("jdbcTemplate") JdbcTemplate jdbc,
            ObjectMapper mapper,
            DataDevService dataDevService,
            DevJobExecutor devJobExecutor,
            SandboxDbService sandboxDb,
            DataSandboxMvpService mvp,
            ModelApprovalService modelApprovalService,
            SandboxDataControlService dataControl) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.dataDevService = dataDevService;
        this.devJobExecutor = devJobExecutor;
        this.sandboxDb = sandboxDb;
        this.mvp = mvp;
        this.modelApprovalService = modelApprovalService;
        this.dataControl = dataControl;
    }

    /* ============================== 整图/节点运行 ============================== */

    /**
     * 启动画布运行：创建 {@code ds_compute_run} + 本次涉及的 {@code ds_compute_node_run}（PENDING），
     * 后台线程逐节点执行（恢复请求线程的 UserContext，使 actor()/权限判断在后台保持一致）。
     *
     * @param mode    ALL 整图 / SINGLE 单节点 / DOWN 单步向下（节点+下游）/ UP 执行到节点（节点+上游）/ CONTINUE 断点继续
     * @param nodeIds 前端传入的精确运行节点集（run-all/single/down/up/continue 均已在前端算好闭包），非空时以它为准
     */
    public Map<String, Object> run(String canvasId, String mode, String nodeId, List<String> nodeIds) {
        Map<String, Object> canvas = requireCanvas(canvasId);
        String sandboxId = string(canvas.get("sandbox_id"));
        requireUsableSandbox(sandboxId, true);
        String resolvedMode = normalizeMode(mode);
        GraphModel graph = parseGraph(string(canvas.get("graph_json")));
        topoSort(graph); // 环检测
        Set<String> included;
        if (nodeIds != null && !nodeIds.isEmpty()) {
            included = new LinkedHashSet<>();
            for (String id : nodeIds) {
                if (graph.nodeById(id) != null) {
                    included.add(id);
                }
            }
        } else {
            included = selectNodes(resolvedMode, nodeId, graph);
        }
        if (included.isEmpty()) {
            throw new IllegalArgumentException("本次运行未包含任何节点（单节点/断点节点不存在）");
        }
        String runId = "cr-" + shortId();
        String now = now();
        jdbc.update("insert into ds_compute_run(id,canvas_id,sandbox_id,status,mode,node_ids,started_by,started_at,finished_at,created_at,updated_at,deleted) "
                        + "values(?,?,?,'PENDING',?,?,?,?,'',?,?,0)",
                runId, canvasId, sandboxId, resolvedMode, json(included), actor(), now, now, now);
        for (String id : included) {
            Node node = graph.nodeById(id);
            if (node == null) {
                throw new IllegalArgumentException("画布节点不存在: " + id);
            }
            jdbc.update("insert into ds_compute_node_run(id,run_id,canvas_id,sandbox_id,node_id,component_code,status,"
                            + "started_at,finished_at,created_at,updated_at,deleted) "
                            + "values(?,?,?,?,?,?,'PENDING','','',?,?,0)",
                    "nr-" + shortId(), runId, canvasId, sandboxId, node.id, node.componentCode, now, now);
        }
        UserContextDTO user = UserContext.getUserOrNotExist();
        canvasExecutor.execute(() -> executeRun(runId, user));
        return runDetail(runId);
    }

    /** 停止运行：停掉所有 RUNNING 节点任务 + 置 run/node_run CANCELLED。 */
    public Map<String, Object> stopRun(String runId) {
        Map<String, Object> run = requireRun(runId);
        String status = string(run.get("status"));
        // 已结束/已取消的 run 无需处理；PENDING（排队中未启动）也要能停：置 CANCELLED 后，
        // 等待中的 executeRun 会在首个节点 isCancelled 检查处整体跳过。
        if (!"RUNNING".equals(status) && !"PENDING".equals(status)) {
            return runDetail(runId);
        }
        List<Map<String, Object>> nrs = jdbc.queryForList(
                "select * from ds_compute_node_run where run_id=? and status='RUNNING' and deleted=0", runId);
        for (Map<String, Object> nr : nrs) {
            String taskId = string(nr.get("task_id"));
            if (notBlank(taskId)) {
                stopTask(taskId);
            }
            jdbc.update("update ds_compute_node_run set status='CANCELLED',finished_at=?,updated_at=? where id=?",
                    now(), now(), nr.get("id"));
        }
        if ("PENDING".equals(status)) {
            jdbc.update("update ds_compute_node_run set status='CANCELLED',finished_at=?,updated_at=? where run_id=? and status='PENDING' and deleted=0",
                    now(), now(), runId);
        }
        jdbc.update("update ds_compute_run set status='CANCELLED',finished_at=?,updated_at=? where id=?",
                now(), now(), runId);
        audit("CANVAS_RUN_STOPPED", "COMPUTE_RUN", runId, "canvas=" + string(run.get("canvas_id")), true);
        return runDetail(runId);
    }

    /** 整图/最近一次运行状态 + 节点状态（前端 queryStatus 轮询映射 X6 节点着色）。 */
    public Map<String, Object> runStatus(String canvasId) {
        Map<String, Object> canvas = requireCanvas(canvasId);
        List<Map<String, Object>> runs = jdbc.queryForList(
                "select * from ds_compute_run where canvas_id=? and deleted=0 order by created_at desc limit 1", canvasId);
        if (runs.isEmpty()) {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("canvasId", canvasId);
            result.put("run", null);
            result.put("nodes", List.of());
            return result;
        }
        Map<String, Object> run = new LinkedHashMap<>(runs.get(0));
        String runId = string(run.get("id"));
        run.put("nodeRuns", nodeRuns(runId));
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("canvasId", canvasId);
        result.put("run", run);
        result.put("nodes", nodeRunMap(runId));
        return result;
    }

    public List<Map<String, Object>> runs(String canvasId) {
        requireCanvas(canvasId);
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select * from ds_compute_run where canvas_id=? and deleted=0 order by created_at desc limit 100", canvasId);
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            Map<String, Object> item = new LinkedHashMap<>(row);
            item.put("nodeRuns", nodeRuns(string(row.get("id"))));
            result.add(item);
        }
        return result;
    }

    public Map<String, Object> runDetail(String runId) {
        Map<String, Object> run = requireRun(runId);
        Map<String, Object> result = new LinkedHashMap<>(run);
        result.put("nodeRuns", nodeRuns(runId));
        return result;
    }

    /** 节点输出数据预览：按 runId 读取对应节点结果；未传 runId 时读取该节点最近一次成功运行。 */
    public Map<String, Object> nodeOutput(String canvasId, String nodeId, String runId, int limit) {
        Map<String, Object> canvas = requireCanvas(canvasId);
        String sandboxId = string(canvas.get("sandbox_id"));
        requireUsableSandbox(sandboxId, false);
        Map<String, Object> nodeRun = resolveNodeRunForOutput(canvasId, nodeId, runId);
        if (nodeRun == null) {
            return unavailableOutput("", runId, nodeId, "该节点尚无成功输出");
        }
        String resolvedRunId = string(nodeRun.get("run_id"));
        String table = string(nodeRun.get("output_table"));
        boolean virtual = CanvasOperatorRegistry.isVirtual(string(nodeRun.get("component_code")));
        boolean legacySharedTable = table.equals(legacyOpTableName(canvasId, nodeId));
        boolean safeTableSnapshot = virtual || !legacySharedTable || isLatestSuccessfulNodeRun(canvasId, nodeId, nodeRun);
        if (safeTableSnapshot && !table.isBlank() && sandboxDb.hasTable(sandboxId, table)) {
            dataControl.requireMountTableUsable(sandboxId, table);
            Map<String, Object> preview = sandboxDb.previewTable(sandboxId, table, Math.max(1, Math.min(limit, 500)));
            preview.put("available", true);
            preview.put("nodeId", nodeId);
            preview.put("runId", resolvedRunId);
            preview.put("taskId", nodeRun.get("task_id"));
            preview.put("displayName", sandboxDb.tableDisplayName(sandboxId, table));
            preview.put("snapshotSource", virtual ? "MOUNT_TABLE" : "RUN_TABLE");
            preview.put("previewOnly", false);
            return preview;
        }
        Map<String, Object> taskPreview = taskPreview(nodeRun, table, nodeId, resolvedRunId);
        if (taskPreview != null) return taskPreview;
        return unavailableOutput(table, resolvedRunId, nodeId,
                legacySharedTable ? "该历史运行的完整结果已被后续运行覆盖，且没有可恢复的任务预览"
                        : "该运行的结果表和任务预览均不可用");
    }

    private Map<String, Object> resolveNodeRunForOutput(String canvasId, String nodeId, String runId) {
        List<Map<String, Object>> rows;
        if (notBlank(runId)) {
            Map<String, Object> run = requireRun(runId);
            if (!canvasId.equals(string(run.get("canvas_id")))) {
                throw new IllegalArgumentException("运行记录不属于当前画布: " + runId);
            }
            rows = jdbc.queryForList("select * from ds_compute_node_run where run_id=? and node_id=? and deleted=0 limit 1",
                    runId, nodeId);
        } else {
            rows = jdbc.queryForList("select * from ds_compute_node_run where canvas_id=? and node_id=? "
                            + "and status='SUCCEEDED' and deleted=0 order by finished_at desc,created_at desc limit 1",
                    canvasId, nodeId);
        }
        return rows.isEmpty() ? null : new LinkedHashMap<>(rows.get(0));
    }

    private boolean isLatestSuccessfulNodeRun(String canvasId, String nodeId, Map<String, Object> selected) {
        List<Map<String, Object>> rows = jdbc.queryForList("select id from ds_compute_node_run where canvas_id=? and node_id=? "
                        + "and status='SUCCEEDED' and deleted=0 order by finished_at desc,created_at desc limit 1",
                canvasId, nodeId);
        return !rows.isEmpty() && Objects.equals(string(rows.get(0).get("id")), string(selected.get("id")));
    }

    private Map<String, Object> taskPreview(Map<String, Object> nodeRun, String table, String nodeId, String runId) {
        String taskId = string(nodeRun.get("task_id"));
        if (!notBlank(taskId)) return null;
        List<Map<String, Object>> tasks = jdbc.queryForList(
                "select result_preview,result_rows from ds_dev_task where id=? and status='SUCCEEDED' and deleted=0", taskId);
        if (tasks.isEmpty()) return null;
        Map<String, Object> stored = parseMapOrEmpty(string(tasks.get(0).get("result_preview")));
        if (!(stored.get("header") instanceof List<?> header) || !(stored.get("rows") instanceof List<?> rawRows)) {
            return null;
        }
        List<Object> rows = new ArrayList<>(rawRows);
        while (!rows.isEmpty() && isMarkerRow(rows.get(rows.size() - 1))) rows.remove(rows.size() - 1);
        List<Map<String, Object>> schema = new ArrayList<>();
        for (Object name : header) schema.add(Map.of("name", string(name), "type", "string"));
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("tableName", table);
        result.put("available", true);
        result.put("schema", schema);
        result.put("rows", rows);
        Map<String, Object> summary = parseMapOrEmpty(string(nodeRun.get("result_summary")));
        result.put("totalRows", summary.getOrDefault("rowCount", tasks.get(0).get("result_rows")));
        result.put("nodeId", nodeId);
        result.put("runId", runId);
        result.put("taskId", taskId);
        result.put("displayName", table);
        result.put("snapshotSource", "TASK_PREVIEW");
        result.put("previewOnly", true);
        return result;
    }

    private boolean isMarkerRow(Object row) {
        return row instanceof List<?> cells && !cells.isEmpty()
                && Set.of(MODEL_MARKER, PREPROC_MARKER).contains(string(cells.get(0)));
    }

    private Map<String, Object> unavailableOutput(String table, String runId, String nodeId, String message) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("tableName", table);
        result.put("displayName", table);
        result.put("available", false);
        result.put("message", message);
        result.put("schema", List.of());
        result.put("rows", List.of());
        result.put("totalRows", 0);
        result.put("nodeId", nodeId);
        result.put("runId", runId);
        return result;
    }

    /** 节点日志：该节点最近一次运行的 ds_dev_task（channel='canvas'）日志。 */
    public Map<String, Object> nodeLogs(String canvasId, String nodeId, String runId) {
        Map<String, Object> canvas = requireCanvas(canvasId);
        String sandboxId = string(canvas.get("sandbox_id"));
        requireUsableSandbox(sandboxId, false);
        List<Map<String, Object>> nodeRuns;
        if (notBlank(runId)) {
            nodeRuns = jdbc.queryForList(
                    "select * from ds_compute_node_run where run_id=? and node_id=? and deleted=0 order by created_at desc limit 1",
                    runId, nodeId);
        } else {
            nodeRuns = jdbc.queryForList(
                    "select * from ds_compute_node_run where canvas_id=? and node_id=? and deleted=0 order by created_at desc limit 1",
                    canvasId, nodeId);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("canvasId", canvasId);
        result.put("nodeId", nodeId);
        if (nodeRuns.isEmpty()) {
            result.put("logs", List.of());
            result.put("errorMessage", "");
            return result;
        }
        Map<String, Object> nodeRun = new LinkedHashMap<>(nodeRuns.get(0));
        String taskId = string(nodeRun.get("task_id"));
        result.put("errorMessage", string(nodeRun.get("error_message")));
        if (notBlank(taskId)) {
            result.put("logs", jdbc.queryForList(
                    "select * from ds_dev_run_log where task_id=? order by id asc limit 200", taskId));
        } else {
            result.put("logs", List.of());
        }
        return result;
    }

    /**
     * 节点当前输入数据表（节点配置抽屉用）：解析入边 → 上游 data.table 挂载表或上游组件最近一次成功输出
     * 的 op_* 表，返回 schema + 预览行。用于处理列/预测列下拉候选与「查看输入数据表」预览。
     */
    public Map<String, Object> nodeInput(String canvasId, String nodeId, int limit) {
        Map<String, Object> canvas = requireCanvas(canvasId);
        String sandboxId = string(canvas.get("sandbox_id"));
        requireUsableSandbox(sandboxId, false);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("nodeId", nodeId);
        result.put("available", false);
        result.put("tableName", "");
        result.put("displayName", "");
        result.put("schema", List.of());
        result.put("rows", List.of());
        result.put("totalRows", 0);
        result.put("message", "");
        GraphModel graph = parseGraph(string(canvas.get("graph_json")));
        Node node = graph.nodeById(nodeId);
        if (node == null) {
            result.put("message", "画布节点不存在: " + nodeId);
            return result;
        }
        List<String> sources = new ArrayList<>();
        for (Edge edge : graph.edges) {
            if (edge.target.equals(nodeId)) {
                sources.add(edge.source);
            }
        }
        if (sources.isEmpty()) {
            result.put("message", "该节点暂无输入（请从数据资源节点连线）");
            return result;
        }
        if (sources.size() > 1) {
            result.put("message", "该节点存在多个输入，当前仅支持单输入算子");
            return result;
        }
        Node source = graph.nodeById(sources.get(0));
        String table;
        if (CanvasOperatorRegistry.isVirtual(source.componentCode)) {
            table = string(source.params.get("table"));
            if (!sandboxDb.hasTable(sandboxId, table)) {
                result.put("message", "上游数据资源节点未配置挂载表: " + sources.get(0));
                return result;
            }
        } else {
            table = latestOutputTable(sandboxId, canvasId, source.id);
            if (!sandboxDb.hasTable(sandboxId, table)) {
                result.put("message", "上游组件尚未成功运行，暂无输入数据（请先执行上游节点）");
                return result;
            }
        }
        try {
            dataControl.requireMountTableUsable(sandboxId, table);
        } catch (Exception e) {
            result.put("message", e.getMessage());
            return result;
        }
        Map<String, Object> preview = sandboxDb.previewTable(sandboxId, table, Math.max(1, Math.min(limit, 100)));
        result.putAll(preview);
        result.put("available", true);
        result.put("message", "");
        result.put("displayName", sandboxDb.tableDisplayName(sandboxId, table));
        result.put("sourceNodeId", source.id);
        result.put("sourceComponentCode", source.componentCode);
        return result;
    }

    /* ============================== 画布数据资源（节点配置用） ============================== */

    /**
     * 画布可用数据资源：沙箱挂载表 + op_* 中间结果（不含 result_*），供 data.table/compare_table/列选择。
     * 中间结果语义：每次执行组件节点产出结果数据都会记录一条，命名为「<组件/节点名>输出数据<N>」，
     * 例如数据资源节点的输出、标准化节点的「标准化输出数据1」。op_* 名称友好化（兼容历史「画布输出-*」命名）。
     */
    public Map<String, Object> dataResources(String sandboxId) {
        requireUsableSandbox(sandboxId, false);
        Map<String, Object> dir = dataControl.enrichDirectory(sandboxDb.directory(sandboxId));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) dir.get("items");
        List<Map<String, Object>> resources = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (Map<String, Object> item : items) {
            String kind = string(item.get("kind"));
            String table = string(item.get("tableName"));
            if (!Set.of("MOUNT", "OPERATOR").contains(kind) || !seen.add(table)) {
                continue;
            }
            if ("OPERATOR".equals(kind)) {
                enrichOperatorDisplayName(item);
            }
            resources.add(item);
        }
        // 数据资源节点（data.table）每次成功执行也记录为中间结果，命名为「<节点名>输出数据<N>」
        for (Map<String, Object> run : dataTableNodeRuns()) {
            String table = string(run.get("output_table"));
            if (!notBlank(table) || !seen.add(table)) {
                continue;
            }
            String displayName = runNodeDisplayName(string(run.get("canvas_id")), string(run.get("node_id")));
            if (displayName == null) {
                continue;
            }
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("tableName", table);
            item.put("name", displayName);
            item.put("kind", "OPERATOR");
            item.put("columns", tableColumns(sandboxId, table));
            item.put("canPreview", true);
            resources.add(item);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("sandboxId", sandboxId);
        result.put("resources", resources);
        return result;
    }

    /** 数据资源节点（data.table）的成功运行记录（每次执行一条）。 */
    private List<Map<String, Object>> dataTableNodeRuns() {
        return jdbc.queryForList(
                "select canvas_id,node_id,output_table from ds_compute_node_run "
                        + "where status='SUCCEEDED' and deleted=0 and component_code='data.table' "
                        + "and output_table<>'' order by finished_at desc,created_at desc");
    }

    /** op_* 中间结果友好名：从所属画布节点名推导，兼容历史「画布输出-*」命名。 */
    private void enrichOperatorDisplayName(Map<String, Object> item) {
        String table = string(item.get("tableName"));
        if (!notBlank(table) || !table.startsWith("op_")) {
            return;
        }
        List<Map<String, Object>> nrs = jdbc.queryForList(
                "select canvas_id,node_id from ds_compute_node_run where output_table=? "
                        + "and status='SUCCEEDED' and deleted=0 order by finished_at desc,created_at desc limit 1",
                table);
        if (nrs.isEmpty()) {
            return;
        }
        String displayName = runNodeDisplayName(string(nrs.get(0).get("canvas_id")), string(nrs.get(0).get("node_id")));
        if (displayName != null) {
            item.put("displayName", displayName);
        }
    }

    /** 通过画布 graph_json 定位节点并推导输出友好名；画布已删/节点缺失返回 null。 */
    private String runNodeDisplayName(String canvasId, String nodeId) {
        try {
            Map<String, Object> canvas = requireCanvas(canvasId);
            GraphModel graph = parseGraph(string(canvas.get("graph_json")));
            Node node = graph.nodeById(nodeId);
            return node == null ? null : operatorOutputName(node, graph);
        } catch (Exception e) {
            return null;
        }
    }

    /* ============================== 模板 ============================== */

    public List<Map<String, Object>> templates() {
        return CanvasTemplates.templates();
    }

    /** 模板一键导入：以模板 graph_json 新建画布（data.table 的 table 参数留空由用户选定挂载表）。 */
    public Map<String, Object> importTemplate(String sandboxId, String code, String name) {
        Map<String, Object> sandbox = requireSandbox(sandboxId);
        requireUsableSandbox(sandboxId, true);
        Map<String, Object> tpl = CanvasTemplates.templates().stream()
                .filter(t -> code.equals(string(t.get("code"))))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("模板不存在: " + code));
        String now = now();
        String id = "canvas-" + shortId();
        String canvasName = (notBlank(name) ? name : string(tpl.get("name"))).trim();
        Long duplicate = jdbc.queryForObject(
                "select count(1) from ds_compute_canvas where sandbox_id=? and deleted=0 "
                        + "and lower(name)=lower(?)",
                Long.class, sandboxId, canvasName);
        if (duplicate != null && duplicate > 0) {
            throw new IllegalArgumentException("同一沙箱内画布名称不能重复: " + canvasName);
        }
        String graph = json(tpl.get("graph"));
        jdbc.update("insert into ds_compute_canvas(id,project_id,sandbox_id,name,description,graph_json,version,status,created_by,created_at,updated_at,deleted) "
                        + "values(?,?,?,?,?,?,1,'DRAFT',?,?,?,0)",
                id, sandbox.get("project_id"), sandboxId, canvasName, string(tpl.get("description")),
                graph, actor(), now, now);
        jdbc.update("insert into ds_compute_canvas_version(id,canvas_id,version,name,graph_json,created_by,created_at,deleted) "
                        + "values(?,?,1,?,?,?,?,0)",
                "cv-" + shortId(), id, canvasName, graph, actor(), now);
        audit("CANVAS_TEMPLATE_IMPORT", "COMPUTE_CANVAS", id, "template=" + code, true);
        return requireCanvas(id);
    }

    /* ============================== 版本管理 ============================== */

    public List<Map<String, Object>> versions(String canvasId) {
        requireCanvas(canvasId);
        return jdbc.queryForList(
                "select * from ds_compute_canvas_version where canvas_id=? and deleted=0 order by version desc", canvasId);
    }

    /* ============================== 工作流模型 ============================== */

    /** 查询画布显式保存的工作流模型，graph_json 为保存时的不可变拓扑快照。 */
    public List<Map<String, Object>> models(String canvasId) {
        Map<String, Object> canvas = requireCanvas(canvasId);
        requireUsableSandbox(string(canvas.get("sandbox_id")), false);
        return jdbc.queryForList(
                "select cm.*,m.status model_status,m.version model_version,m.artifact_id,m.artifact_version_id "
                        + "from ds_compute_canvas_model cm left join ds_model m on m.id=cm.model_id and m.deleted=0 "
                        + "where cm.canvas_id=? and cm.deleted=0 order by cm.created_at desc",
                canvasId);
    }

    /**
     * 工作流模型报告：以保存模型时绑定的运行批次为依据，汇总评估指标、最终入模特征和前处理链路。
     * 历史模型没有 source_run_id 时，仅可通过唯一的 model_id 运行记录回溯，并显式标记为兼容推断。
     */
    public Map<String, Object> modelReport(String canvasModelId, String testId) {
        Map<String, Object> canvasModel = requireCanvasModel(canvasModelId);
        Map<String, Object> canvas = requireCanvas(string(canvasModel.get("canvas_id")));
        String sandboxId = string(canvas.get("sandbox_id"));
        requireUsableSandbox(sandboxId, false);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("model", modelReportSummary(canvasModel));
        if (!"READY".equals(string(canvasModel.get("status")))
                || !notBlank(string(canvasModel.get("model_id")))) {
            result.put("reportStatus", "UNAVAILABLE");
            result.put("message", "当前模型仅保存工作流拓扑，尚未关联成功训练结果");
            return result;
        }

        String sourceRunId = string(canvasModel.get("source_run_id"));
        boolean inferredRun = false;
        if (!notBlank(sourceRunId)) {
            List<Map<String, Object>> legacyRuns = jdbc.queryForList(
                    "select run_id,task_id,node_id,input_table,output_table from ds_compute_node_run "
                            + "where canvas_id=? and model_id=? and status='SUCCEEDED' and deleted=0 "
                            + "order by finished_at desc,created_at desc limit 1",
                    canvasModel.get("canvas_id"), canvasModel.get("model_id"));
            if (!legacyRuns.isEmpty()) {
                sourceRunId = string(legacyRuns.get(0).get("run_id"));
                inferredRun = true;
            }
        }
        if (!notBlank(sourceRunId)) {
            result.put("reportStatus", "INCOMPLETE");
            result.put("message", "未找到该模型对应的训练运行批次，无法准确生成报告");
            return result;
        }
        List<Map<String, Object>> sourceRuns = jdbc.queryForList(
                "select id from ds_compute_run where id=? and canvas_id=? and deleted=0",
                sourceRunId, canvasModel.get("canvas_id"));
        if (sourceRuns.isEmpty()) {
            throw new SecurityException("模型训练运行批次不属于当前画布");
        }

        List<Map<String, Object>> trainRuns = jdbc.queryForList(
                "select * from ds_compute_node_run where run_id=? and model_id=? "
                        + "and status='SUCCEEDED' and deleted=0 order by finished_at desc limit 1",
                sourceRunId, canvasModel.get("model_id"));
        if (trainRuns.isEmpty()) {
            result.put("reportStatus", "INCOMPLETE");
            result.put("message", "训练运行记录不完整，无法确认实际入模特征");
            return result;
        }

        Map<String, Object> trainRun = trainRuns.get(0);
        GraphModel graph = parseGraph(string(canvasModel.get("graph_json")));
        Node trainNode = graph.nodeById(string(trainRun.get("node_id")));
        boolean recoveredGraph = false;
        if (trainNode == null || !CanvasOperatorRegistry.isTrain(trainNode.componentCode)) {
            GraphModel legacyGraph = legacyTrainingGraph(canvasModel, string(trainRun.get("node_id")));
            Node legacyTrainNode = legacyGraph == null ? null
                    : legacyGraph.nodeById(string(trainRun.get("node_id")));
            if (legacyTrainNode != null && CanvasOperatorRegistry.isTrain(legacyTrainNode.componentCode)) {
                graph = legacyGraph;
                trainNode = legacyTrainNode;
                recoveredGraph = true;
            }
        }
        if (trainNode == null || !CanvasOperatorRegistry.isTrain(trainNode.componentCode)) {
            result.put("reportStatus", "INCOMPLETE");
            result.put("message", "工作流快照中没有找到对应的训练节点");
            return result;
        }

        String inputTable = string(trainRun.get("input_table"));
        List<Map<String, Object>> trainingSchema = tableSchema(sandboxId, inputTable);
        Map<String, Object> workflowInput = workflowInput(graph, sandboxId);
        List<Map<String, Object>> sourceSchema = tableSchema(sandboxId, string(workflowInput.get("table")));
        String label = string(trainNode.params.get("label"));
        List<String> selectedFeatures = stringList(trainNode.params.get("features"));
        String selectionMethod = "MANUAL";
        if (selectedFeatures.isEmpty()) {
            selectionMethod = "AUTO";
            for (Map<String, Object> column : trainingSchema) {
                String name = string(column.get("name"));
                if (notBlank(name) && !name.equals(label)) {
                    selectedFeatures.add(name);
                }
            }
        }

        List<Map<String, Object>> preprocessing = preprocessingReport(graph, sourceRunId, trainNode);
        Map<String, Map<String, Object>> sourceByName = schemaByName(sourceSchema);
        Map<String, Map<String, Object>> trainingByName = schemaByName(trainingSchema);
        List<Map<String, Object>> features = new ArrayList<>();
        for (String name : selectedFeatures) {
            Map<String, Object> feature = new LinkedHashMap<>();
            feature.put("name", name);
            feature.put("sourceType", schemaType(sourceByName.get(name)));
            feature.put("modelType", schemaType(trainingByName.get(name)));
            feature.put("role", "FEATURE");
            feature.put("selectionMethod", selectionMethod);
            feature.put("preprocessing", featurePreprocessing(name, preprocessing));
            feature.put("usedInModel", true);
            features.add(feature);
        }

        List<Map<String, Object>> excluded = new ArrayList<>();
        Set<String> selected = new LinkedHashSet<>(selectedFeatures);
        for (Map<String, Object> column : sourceSchema) {
            String name = string(column.get("name"));
            if (!notBlank(name) || selected.contains(name) || name.equals(label)) {
                continue;
            }
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("name", name);
            item.put("type", schemaType(column));
            item.put("reason", trainingByName.containsKey(name)
                    ? "训练节点未选择" : "上游前处理后未进入训练输入");
            excluded.add(item);
        }

        Map<String, Object> featureSummary = new LinkedHashMap<>();
        featureSummary.put("sourceFieldCount", sourceSchema.size());
        featureSummary.put("modelFeatureCount", features.size());
        featureSummary.put("excludedFieldCount", excluded.size());
        featureSummary.put("preprocessingCount", preprocessing.size());
        featureSummary.put("label", label);
        featureSummary.put("inputTable", inputTable);

        result.put("reportStatus", "AVAILABLE");
        result.put("sourceRunId", sourceRunId);
        result.put("sourceTaskId", string(trainRun.get("task_id")));
        result.put("runBinding", inferredRun ? "LEGACY_INFERRED" : "EXACT");
        result.put("graphBinding", recoveredGraph ? "LEGACY_VERSION_RECOVERED" : "SAVED_SNAPSHOT");
        result.put("algorithm", Map.of(
                "componentCode", trainNode.componentCode,
                "componentName", trainNode.name,
                "parameters", trainNode.params));
        result.put("featureSummary", featureSummary);
        result.put("features", features);
        result.put("excludedFields", excluded);
        result.put("preprocessingSteps", preprocessing);
        Map<String, Object> savedEvaluation = savedModelEvaluation(canvasModel);
        result.put("evaluation", savedEvaluation.isEmpty()
                ? modelEvaluation(string(canvasModel.get("model_id")), testId,
                        sandboxId, trainRun, trainNode, graph, sourceRunId)
                : savedEvaluation);
        result.put("testHistory", modelTestHistory(string(canvasModel.get("model_id"))));
        Map<String, Object> reportConfig = parseMapOrEmpty(string(canvasModel.get("report_config")));
        result.put("reportConfig", reportConfig);
        List<String> visibleSections = stringList(reportConfig.get("visibleSections"));
        boolean legacyReportConfig = reportConfig.isEmpty();
        result.put("featureImportance", legacyReportConfig || visibleSections.contains("featureImportance")
                ? cachedFeatureImportance(trainRun, trainNode) : Map.of("supported", false, "hidden", true));
        result.put("treeStructure", legacyReportConfig || visibleSections.contains("treeStructure")
                ? cachedTreeStructure(trainRun, trainNode) : Map.of("supported", false, "hidden", true));
        Map<String, Object> fullMetrics = parseMapOrEmpty(string(canvasModel.get("evaluation_metrics")));
        result.put("scorecard", visibleSections.contains("scorecard")
                ? parseMapOrEmpty(json(fullMetrics.get("scorecard"))) : Map.of());
        return result;
    }

    /** 保存模型时已全量计算的指标优先于历史测试和画布评估节点，并按报告配置过滤展示字段。 */
    private Map<String, Object> savedModelEvaluation(Map<String, Object> canvasModel) {
        String status = string(canvasModel.get("evaluation_status"));
        Map<String, Object> full = parseMapOrEmpty(string(canvasModel.get("evaluation_metrics")));
        if (!"SUCCEEDED".equals(status) || full.isEmpty()) {
            if (notBlank(status)) {
                return Map.of(
                        "status", status,
                        "message", firstNotBlank(string(canvasModel.get("evaluation_error")), "模型评估结果暂不可用"));
            }
            return Map.of();
        }
        Map<String, Object> config = parseMapOrEmpty(string(canvasModel.get("report_config")));
        List<String> selected = stringList(config.get("visibleMetrics"));
        if (selected.isEmpty()) {
            selected = applicableMetrics(string(canvasModel.get("task_type")));
        }
        Map<String, Object> visible = new LinkedHashMap<>();
        visible.put("metricType", full.get("metricType"));
        for (String metric : selected) {
            if (full.containsKey(metric)) {
                visible.put(metric, full.get(metric));
            }
        }
        Map<String, Object> evaluation = new LinkedHashMap<>();
        evaluation.put("status", "AVAILABLE");
        evaluation.put("source", "MODEL_SAVE");
        evaluation.put("metricsScope", "CONFIGURED");
        evaluation.put("metricType", full.get("metricType"));
        evaluation.put("taskType", canvasModel.get("task_type"));
        evaluation.put("modelCategory", canvasModel.get("model_category"));
        evaluation.put("selectedMetrics", selected);
        evaluation.put("availableMetrics", applicableMetrics(string(canvasModel.get("task_type"))));
        evaluation.put("sampleCount", full.get("samples"));
        evaluation.put("positiveLabel", full.get("positiveLabel"));
        evaluation.put("metrics", visible);
        evaluation.put("inputSummary", Map.of());
        evaluation.put("outputSummary", Map.of("rowCount", intValue(full.get("samples"), 0)));
        evaluation.put("resultPreview", Map.of());
        evaluation.put("createdAt", canvasModel.get("created_at"));
        evaluation.put("finishedAt", canvasModel.get("created_at"));
        return evaluation;
    }

    /**
     * 特征重要性缓存：joblib 模型只能在执行侧解析，此处只读已落库的结果。
     * 未计算时返回 {@code NOT_COMPUTED}，由 {@link #computeFeatureImportance(String)} 按需触发。
     */
    private Map<String, Object> cachedFeatureImportance(Map<String, Object> trainRun, Node trainNode) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("componentCode", trainNode.componentCode);
        result.put("supported", supportsFeatureImportance(trainNode.componentCode));
        Map<String, Object> cached = parseMapOrEmpty(string(trainRun.get("feature_importance")));
        if (cached.isEmpty()) {
            result.put("status", Boolean.TRUE.equals(result.get("supported")) ? "NOT_COMPUTED" : "UNSUPPORTED");
            result.put("items", List.of());
            return result;
        }
        result.put("status", "AVAILABLE");
        result.put("source", cached.get("source"));
        result.put("computedAt", cached.get("computedAt"));
        result.put("items", cached.getOrDefault("items", List.of()));
        return result;
    }

    /** 树模型读不纯度重要性、线性模型读系数绝对值；其余算子没有可解释的特征权重。 */
    private boolean supportsFeatureImportance(String componentCode) {
        return List.of("ml.decision_tree", "ml.xgboost", "ml.lightgbm",
                "ml.linear_regression", "ml.logistic_regression").contains(componentCode);
    }

    /**
     * 按需计算特征重要性：在执行侧加载训练产物 joblib，读取 {@code feature_importances_} 或 {@code coef_}，
     * 结果写入 {@code ds_compute_node_run.feature_importance} 供报告复用（已计算则直接返回缓存）。
     */
    public Map<String, Object> computeFeatureImportance(String canvasModelId) {
        Map<String, Object> canvasModel = requireCanvasModel(canvasModelId);
        Map<String, Object> canvas = requireCanvas(string(canvasModel.get("canvas_id")));
        String sandboxId = string(canvas.get("sandbox_id"));
        requireUsableSandbox(sandboxId, false);
        String sourceRunId = string(canvasModel.get("source_run_id"));
        String modelId = string(canvasModel.get("model_id"));
        if (!notBlank(sourceRunId) || !notBlank(modelId)) {
            throw new IllegalArgumentException("当前模型没有绑定训练运行批次，无法计算特征重要性");
        }
        List<Map<String, Object>> runs = jdbc.queryForList(
                "select * from ds_compute_node_run where run_id=? and model_id=? and status='SUCCEEDED' and deleted=0 "
                        + "order by finished_at desc limit 1",
                sourceRunId, modelId);
        if (runs.isEmpty()) {
            throw new IllegalArgumentException("未找到该模型对应的训练运行记录");
        }
        Map<String, Object> trainRun = runs.get(0);
        GraphModel graph = parseGraph(string(canvasModel.get("graph_json")));
        Node trainNode = graph.nodeById(string(trainRun.get("node_id")));
        if (trainNode == null) {
            throw new IllegalArgumentException("工作流快照中没有找到对应的训练节点");
        }
        if (!supportsFeatureImportance(trainNode.componentCode)) {
            return cachedFeatureImportance(trainRun, trainNode);
        }
        Map<String, Object> cached = parseMapOrEmpty(string(trainRun.get("feature_importance")));
        if (!cached.isEmpty()) {
            return cachedFeatureImportance(trainRun, trainNode);
        }

        String modelB64 = string(trainRun.get("model_b64"));
        if (!notBlank(modelB64)) {
            throw new IllegalArgumentException("训练运行记录中没有保存模型产物，无法计算特征重要性");
        }
        String inputTable = string(trainRun.get("input_table"));
        List<String> features = stringList(trainNode.params.get("features"));
        if (features.isEmpty()) {
            for (Map<String, Object> column : tableSchema(sandboxId, inputTable)) {
                String name = string(column.get("name"));
                if (notBlank(name) && !name.equals(string(trainNode.params.get("label")))) {
                    features.add(name);
                }
            }
        }
        Map<String, Object> execution = runModelInspection(sandboxId, string(canvasModel.get("canvas_id")),
                trainNode.id, inputTable, "report.feature_importance", "op_fi_",
                CanvasFeatureImportanceScript.generate(modelB64, features), "特征重要性计算");
        @SuppressWarnings("unchecked")
        List<String> header = (List<String>) execution.get("header");
        @SuppressWarnings("unchecked")
        List<List<String>> rows = (List<List<String>>) execution.get("rows");
        Map<String, Object> payload = featureImportancePayload(header, rows);
        jdbc.update("update ds_compute_node_run set feature_importance=?,updated_at=? where id=?",
                json(payload), now(), trainRun.get("id"));
        audit("CANVAS_FEATURE_IMPORTANCE_COMPUTED", "COMPUTE_NODE_RUN", string(trainRun.get("id")),
                "canvasModel=" + canvasModelId + " node=" + trainNode.id, true);
        trainRun.put("feature_importance", json(payload));
        return cachedFeatureImportance(trainRun, trainNode);
    }

    /**
     * 模型产物解析任务：训练产物为 joblib 二进制，服务端无法读取，统一提交到执行侧运行解析脚本。
     * 与画布节点同通道（{@code channel='canvas'}），不进入数据开发任务列表。
     */
    private Map<String, Object> runModelInspection(String sandboxId, String canvasId, String nodeId,
            String inputTable, String operatorCode, String outputPrefix, String script, String failureLabel) {
        String outputTable = outputPrefix + shortId();
        String compatibleScript = CanvasOperatorRegistry.PYTHON_ASCII_OPEN_COMPAT + script;
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("op", operatorCode);
        String taskId = dataDevService.createCanvasTask(sandboxId, canvasId, nodeId, operatorCode,
                compatibleScript, params, List.of(), outputTable);
        dataDevService.claimCanvasTask(taskId);
        byte[] inputCsv = sandboxDb.readTableCsv(sandboxId, inputTable);
        String nodeDomain = string(jdbc.queryForMap(
                "select owner_id from ds_sandbox where id=? and deleted=0", sandboxId).get("owner_id"));
        devJobExecutor.submitSandboxChannel(taskId, nodeDomain,
                Base64.getEncoder().encodeToString(inputCsv), "PYTHON", compatibleScript, params, List.of(),
                sandboxId, inputTable, outputTable, new LinkedHashSet<>(Set.of(inputTable)), "canvas");
        Map<String, Object> execution = devJobExecutor.runAndAwait(taskId);
        if (!"SUCCEEDED".equals(string(execution.get("status")))) {
            throw new IllegalStateException(failureLabel + "失败: " + string(execution.get("errorMessage")));
        }
        return execution;
    }

    /** 树结构缓存：与特征重要性同样只读已落库结果，未计算时返回 NOT_COMPUTED。 */
    private Map<String, Object> cachedTreeStructure(Map<String, Object> trainRun, Node trainNode) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("componentCode", trainNode.componentCode);
        result.put("supported", supportsTreeStructure(trainNode.componentCode));
        Map<String, Object> cached = parseMapOrEmpty(string(trainRun.get("tree_structure")));
        if (cached.isEmpty()) {
            result.put("status", Boolean.TRUE.equals(result.get("supported")) ? "NOT_COMPUTED" : "UNSUPPORTED");
            result.put("nodes", List.of());
            return result;
        }
        result.put("status", "AVAILABLE");
        result.putAll(cached);
        result.put("status", "AVAILABLE");
        return result;
    }

    /** 仅树模型具备可导出的树结构。 */
    private boolean supportsTreeStructure(String componentCode) {
        return List.of("ml.decision_tree", "ml.xgboost", "ml.lightgbm").contains(componentCode);
    }

    /**
     * 按需导出树结构：在执行侧加载训练产物 joblib，导出指定序号的单棵树（节点数上限 800，超出截断），
     * 结果写入 {@code ds_compute_node_run.tree_structure} 供报告复用。重复导出同一棵树直接返回缓存。
     */
    public Map<String, Object> computeTreeStructure(String canvasModelId, int treeIndex) {
        Map<String, Object> context = requireTrainingContext(canvasModelId);
        @SuppressWarnings("unchecked")
        Map<String, Object> trainRun = (Map<String, Object>) context.get("trainRun");
        Node trainNode = (Node) context.get("trainNode");
        String sandboxId = string(context.get("sandboxId"));
        String canvasId = string(context.get("canvasId"));
        if (!supportsTreeStructure(trainNode.componentCode)) {
            return cachedTreeStructure(trainRun, trainNode);
        }
        Map<String, Object> cached = parseMapOrEmpty(string(trainRun.get("tree_structure")));
        if (!cached.isEmpty() && intValue(cached.get("treeIndex"), -1) == treeIndex) {
            return cachedTreeStructure(trainRun, trainNode);
        }
        String modelB64 = string(trainRun.get("model_b64"));
        if (!notBlank(modelB64)) {
            throw new IllegalArgumentException("训练运行记录中没有保存模型产物，无法导出树结构");
        }
        String inputTable = string(trainRun.get("input_table"));
        List<String> features = trainingFeatures(sandboxId, trainNode, inputTable);
        Map<String, Object> execution = runModelInspection(sandboxId, canvasId, trainNode.id, inputTable,
                "report.tree_structure", "op_ts_",
                CanvasTreeStructureScript.generate(modelB64, features, treeIndex), "树结构导出");
        @SuppressWarnings("unchecked")
        List<String> header = (List<String>) execution.get("header");
        @SuppressWarnings("unchecked")
        List<List<String>> rows = (List<List<String>>) execution.get("rows");
        Map<String, Object> payload = parseMapOrEmpty(joinChunks(header, rows));
        if (payload.isEmpty()) {
            throw new IllegalStateException("树结构导出结果无法解析");
        }
        payload.put("computedAt", now());
        jdbc.update("update ds_compute_node_run set tree_structure=?,updated_at=? where id=?",
                json(payload), now(), trainRun.get("id"));
        audit("CANVAS_TREE_STRUCTURE_EXPORTED", "COMPUTE_NODE_RUN", string(trainRun.get("id")),
                "canvasModel=" + canvasModelId + " tree=" + treeIndex, true);
        trainRun.put("tree_structure", json(payload));
        return cachedTreeStructure(trainRun, trainNode);
    }

    /** 分片输出（part/payload 两列）按 part 升序拼回完整 JSON 文本。 */
    private String joinChunks(List<String> header, List<List<String>> rows) {
        int partIndex = header == null ? -1 : header.indexOf("part");
        int payloadIndex = header == null ? -1 : header.indexOf("payload");
        if (payloadIndex < 0 || rows == null) {
            return "";
        }
        List<List<String>> ordered = new ArrayList<>(rows);
        if (partIndex >= 0) {
            ordered.sort(Comparator.comparingInt(row -> partIndex < row.size()
                    ? intValue(row.get(partIndex), 0) : 0));
        }
        StringBuilder text = new StringBuilder();
        for (List<String> row : ordered) {
            if (payloadIndex < row.size() && row.get(payloadIndex) != null) {
                text.append(row.get(payloadIndex));
            }
        }
        return text.toString();
    }

    /** 训练节点显式选择的特征；未选择时以训练输入表除标签外的全部列为准。 */
    private List<String> trainingFeatures(String sandboxId, Node trainNode, String inputTable) {
        List<String> features = stringList(trainNode.params.get("features"));
        if (!features.isEmpty()) {
            return features;
        }
        String label = string(trainNode.params.get("label"));
        List<String> resolved = new ArrayList<>();
        for (Map<String, Object> column : tableSchema(sandboxId, inputTable)) {
            String name = string(column.get("name"));
            if (notBlank(name) && !name.equals(label)) {
                resolved.add(name);
            }
        }
        return resolved;
    }

    /** 模型报告类接口的公共校验：定位画布模型绑定的训练运行记录与训练节点。 */
    private Map<String, Object> requireTrainingContext(String canvasModelId) {
        Map<String, Object> canvasModel = requireCanvasModel(canvasModelId);
        Map<String, Object> canvas = requireCanvas(string(canvasModel.get("canvas_id")));
        String sandboxId = string(canvas.get("sandbox_id"));
        requireUsableSandbox(sandboxId, false);
        String sourceRunId = string(canvasModel.get("source_run_id"));
        String modelId = string(canvasModel.get("model_id"));
        if (!notBlank(sourceRunId) || !notBlank(modelId)) {
            throw new IllegalArgumentException("当前模型没有绑定训练运行批次，无法解析模型产物");
        }
        List<Map<String, Object>> runs = jdbc.queryForList(
                "select * from ds_compute_node_run where run_id=? and model_id=? and status='SUCCEEDED' and deleted=0 "
                        + "order by finished_at desc limit 1",
                sourceRunId, modelId);
        if (runs.isEmpty()) {
            throw new IllegalArgumentException("未找到该模型对应的训练运行记录");
        }
        Map<String, Object> trainRun = runs.get(0);
        GraphModel graph = parseGraph(string(canvasModel.get("graph_json")));
        Node trainNode = graph.nodeById(string(trainRun.get("node_id")));
        if (trainNode == null) {
            throw new IllegalArgumentException("工作流快照中没有找到对应的训练节点");
        }
        Map<String, Object> context = new LinkedHashMap<>();
        context.put("canvasModel", canvasModel);
        context.put("canvasId", string(canvasModel.get("canvas_id")));
        context.put("sandboxId", sandboxId);
        context.put("trainRun", trainRun);
        context.put("trainNode", trainNode);
        return context;
    }

    private Map<String, Object> featureImportancePayload(List<String> header, List<List<String>> rows) {
        int featureIndex = header == null ? -1 : header.indexOf("feature");
        int importanceIndex = header == null ? -1 : header.indexOf("importance");
        int sourceIndex = header == null ? -1 : header.indexOf("source");
        List<Map<String, Object>> items = new ArrayList<>();
        String source = "UNSUPPORTED";
        if (featureIndex >= 0 && importanceIndex >= 0 && rows != null) {
            for (List<String> row : rows) {
                if (featureIndex >= row.size() || importanceIndex >= row.size()) {
                    continue;
                }
                if (sourceIndex >= 0 && sourceIndex < row.size() && notBlank(row.get(sourceIndex))) {
                    source = row.get(sourceIndex);
                }
                String name = row.get(featureIndex);
                if (!notBlank(name)) {
                    continue;
                }
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("feature", name);
                item.put("importance", numberOrText(row.get(importanceIndex)));
                items.add(item);
            }
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("source", source);
        payload.put("computedAt", now());
        payload.put("items", items);
        return payload;
    }

    private Map<String, Object> modelReportSummary(Map<String, Object> model) {
        Map<String, Object> summary = new LinkedHashMap<>();
        for (String key : List.of("id", "canvas_id", "canvas_version", "model_id", "source_node_id",
                "source_run_id", "source_task_id", "name", "description", "status", "model_status",
                "model_version", "task_type", "model_category", "evaluation_status", "created_by", "created_at")) {
            summary.put(key, model.get(key));
        }
        return summary;
    }

    /**
     * 评估结果来源优先级：指定测试批次 &gt; 成功的模型测试记录 &gt; 画布内配置的评估节点 &gt; 训练输出自动评估。
     * 画布未配置评估节点时按模型任务类型自动计算全部适用指标，配置了则以评估节点产出的指标为准。
     */
    private Map<String, Object> modelEvaluation(String modelId, String testId, String sandboxId,
            Map<String, Object> trainRun, Node trainNode, GraphModel graph, String sourceRunId) {
        List<Map<String, Object>> rows;
        if (notBlank(testId)) {
            rows = jdbc.queryForList(
                    "select * from ds_model_test where id=? and model_id=? and status='SUCCEEDED' and deleted=0",
                    testId, modelId);
        } else {
            rows = jdbc.queryForList(
                    "select * from ds_model_test where model_id=? and status='SUCCEEDED' and deleted=0 "
                            + "order by finished_at desc,created_at desc limit 1",
                    modelId);
        }
        if (rows.isEmpty()) {
            if (!notBlank(testId)) {
                Map<String, Object> configured = canvasEvaluation(sandboxId, sourceRunId, graph, trainNode);
                if (!configured.isEmpty()) {
                    return configured;
                }
                Map<String, Object> recovered = historicalTrainingEvaluation(sandboxId, trainRun, trainNode);
                if (!recovered.isEmpty()) {
                    return recovered;
                }
            }
            return Map.of("status", "NOT_TESTED", "message", "当前模型尚无成功的模型测试报告");
        }
        Map<String, Object> test = rows.get(0);
        Map<String, Object> evaluation = new LinkedHashMap<>();
        evaluation.put("status", "AVAILABLE");
        evaluation.put("source", "MODEL_TEST");
        evaluation.put("metricsScope", "CONFIGURED");
        evaluation.put("testId", test.get("id"));
        evaluation.put("runMode", test.get("run_mode"));
        evaluation.put("metricType", test.get("metric_type"));
        evaluation.put("metrics", parseMapOrEmpty(string(test.get("metrics"))));
        evaluation.put("inputSummary", parseMapOrEmpty(string(test.get("input_summary"))));
        evaluation.put("outputSummary", parseMapOrEmpty(string(test.get("output_summary"))));
        evaluation.put("resultPreview", parseMapOrEmpty(string(test.get("result_preview"))));
        evaluation.put("createdAt", test.get("created_at"));
        evaluation.put("finishedAt", test.get("finished_at"));
        return evaluation;
    }

    /**
     * 画布内显式配置的评估节点结果：取训练节点下游、同一运行批次内成功执行的评估算子，
     * 其输出表为 {@code metric/value} 两列，报告按该节点配置的指标展示。
     */
    private Map<String, Object> canvasEvaluation(String sandboxId, String runId, GraphModel graph, Node trainNode) {
        if (graph == null || !notBlank(runId)) {
            return Map.of();
        }
        Set<String> downstream = downstream(graph, trainNode.id);
        for (Node node : topoSort(graph)) {
            if (!downstream.contains(node.id) || !CanvasOperatorRegistry.isEvaluation(node.componentCode)) {
                continue;
            }
            List<Map<String, Object>> runs = jdbc.queryForList(
                    "select * from ds_compute_node_run where run_id=? and node_id=? and status='SUCCEEDED' and deleted=0 "
                            + "order by finished_at desc limit 1",
                    runId, node.id);
            if (runs.isEmpty()) {
                continue;
            }
            Map<String, Object> run = runs.get(0);
            Map<String, Object> metrics = evaluationNodeMetrics(sandboxId, string(run.get("output_table")));
            if (metrics.isEmpty()) {
                continue;
            }
            metrics.put("metricType", CanvasOperatorRegistry.metricType(
                    trainNode.componentCode, trainNode.params.get("task")));

            Map<String, Object> evaluation = new LinkedHashMap<>();
            evaluation.put("status", "AVAILABLE");
            evaluation.put("source", "CANVAS_EVALUATION_NODE");
            evaluation.put("metricsScope", "CONFIGURED");
            evaluation.put("testId", string(run.get("task_id")));
            evaluation.put("runMode", "CANVAS_EVALUATION");
            evaluation.put("metricType", metrics.get("metricType"));
            evaluation.put("metrics", metrics);
            evaluation.put("evaluationNode", Map.of(
                    "nodeId", node.id,
                    "componentCode", node.componentCode,
                    "componentName", node.name,
                    "configuredParams", node.params));
            evaluation.put("inputSummary", tableSummary(sandboxId, string(run.get("input_table"))));
            evaluation.put("outputSummary", tableSummary(sandboxId, string(run.get("output_table"))));
            evaluation.put("resultPreview", Map.of());
            evaluation.put("createdAt", run.get("created_at"));
            evaluation.put("finishedAt", run.get("finished_at"));
            return evaluation;
        }
        return Map.of();
    }

    /** 评估节点输出表（metric/value 两列）→ 指标键值对。 */
    private Map<String, Object> evaluationNodeMetrics(String sandboxId, String table) {
        if (!notBlank(table)) {
            return new LinkedHashMap<>();
        }
        Map<String, Object> metrics = new LinkedHashMap<>();
        try {
            Map<String, Object> preview = sandboxDb.previewTable(sandboxId, table, 200);
            List<String> columns = schemaNames(preview.get("schema"));
            int metricIndex = columns.indexOf("metric");
            int valueIndex = columns.indexOf("value");
            if (metricIndex < 0 || valueIndex < 0) {
                return metrics;
            }
            for (List<String> row : tableRows(preview.get("rows"))) {
                if (metricIndex >= row.size() || valueIndex >= row.size()) {
                    continue;
                }
                String name = row.get(metricIndex);
                if (notBlank(name)) {
                    metrics.put(name, numberOrText(row.get(valueIndex)));
                }
            }
        } catch (Exception e) {
            log.warn("评估节点结果读取失败 sandboxId={} table={}: {}", sandboxId, table, e.getMessage());
            return new LinkedHashMap<>();
        }
        return metrics;
    }

    private Object numberOrText(String value) {
        if (value == null) {
            return "";
        }
        try {
            return Double.valueOf(value.trim());
        } catch (NumberFormatException e) {
            return value;
        }
    }

    /**
     * 存量模型没有独立测试记录时，从其训练输出回溯可验证的结果摘要。
     * 仅返回聚合指标，不回传训练明细行；接口内部保留来源字段用于审计。
     */
    private Map<String, Object> historicalTrainingEvaluation(String sandboxId,
            Map<String, Object> trainRun, Node trainNode) {
        String outputTable = string(trainRun.get("output_table"));
        if (!notBlank(outputTable)) {
            return Map.of();
        }
        try {
            Map<String, Object> output = sandboxDb.previewTable(sandboxId, outputTable, 500);
            List<String> columns = schemaNames(output.get("schema"));
            List<List<String>> rows = tableRows(output.get("rows"));
            int totalRows = intValue(output.get("totalRows"), rows.size());
            Map<String, Object> metrics = recoveredMetrics(trainNode, columns, rows, totalRows);
            if (metrics.isEmpty()) {
                return Map.of();
            }

            Map<String, Object> evaluation = new LinkedHashMap<>();
            evaluation.put("status", "AVAILABLE");
            evaluation.put("source", "AUTO_EVALUATION");
            evaluation.put("metricsScope", "AUTO");
            evaluation.put("testId", trainRun.get("task_id"));
            evaluation.put("runMode", "TRAINING");
            evaluation.put("metricType", metrics.get("metricType"));
            evaluation.put("metrics", metrics);
            evaluation.put("inputSummary", tableSummary(sandboxId, string(trainRun.get("input_table"))));
            evaluation.put("outputSummary", Map.of("rowCount", totalRows, "columnCount", columns.size()));
            evaluation.put("resultPreview", Map.of());
            evaluation.put("createdAt", trainRun.get("created_at"));
            evaluation.put("finishedAt", trainRun.get("finished_at"));
            return evaluation;
        } catch (Exception e) {
            log.warn("历史模型评估结果回溯失败 sandboxId={} table={}: {}",
                    sandboxId, outputTable, e.getMessage());
            return Map.of();
        }
    }

    /**
     * 画布未配置评估节点时的自动评估：按训练算子的任务类型计算全部适用指标。
     * 分类含准确率/精确率/召回率/F1/混淆矩阵，具备概率列时补 AUC；回归含 MAE/RMSE/R²；
     * 聚类含簇数、各簇样本分布与占比。
     */
    private Map<String, Object> recoveredMetrics(Node trainNode, List<String> columns,
            List<List<String>> rows, int totalRows) {
        return recoveredMetrics(trainNode, columns, rows, totalRows, Map.of());
    }

    private Map<String, Object> recoveredMetrics(Node trainNode, List<String> columns,
            List<List<String>> rows, int totalRows, Map<String, Object> reportConfig) {
        String metricType = CanvasOperatorRegistry.metricType(
                trainNode.componentCode, trainNode.params.get("task"));
        if ("clustering".equals(metricType)) {
            return clusteringMetrics(columns, rows, totalRows);
        }

        String label = string(trainNode.params.get("label"));
        int labelIndex = columns.indexOf(label);
        int predictionIndex = columns.indexOf("pred");
        if (predictionIndex < 0) {
            predictionIndex = columns.indexOf("prediction");
        }
        if (labelIndex < 0 || predictionIndex < 0 || rows.isEmpty()) {
            return Map.of();
        }
        List<String> labels = new ArrayList<>();
        List<String> predictions = new ArrayList<>();
        List<String> probabilities = new ArrayList<>();
        int probabilityIndex = columns.indexOf("pred_prob");
        for (List<String> row : rows) {
            if (labelIndex < row.size() && predictionIndex < row.size()) {
                labels.add(row.get(labelIndex));
                predictions.add(row.get(predictionIndex));
                probabilities.add(probabilityIndex >= 0 && probabilityIndex < row.size()
                        ? row.get(probabilityIndex) : "");
            }
        }
        if (labels.isEmpty()) {
            return Map.of();
        }
        String positiveLabel = firstNotBlank(string(reportConfig.get("positiveLabel")), "1");
        Map<String, Object> metrics = "classification".equals(metricType)
                ? classificationMetrics(labels, predictions, positiveLabel)
                : new LinkedHashMap<>(ModelMetricsEvaluator.evaluate(labels, predictions, metricType));
        metrics.put("samples", labels.size());
        if ("classification".equals(metricType)) {
            Double auc = rocAuc(labels, probabilities, string(metrics.get("positiveLabel")));
            if (auc != null) {
                metrics.put("auc", auc);
            }
        }
        if (rows.size() < totalRows) {
            metrics.put("totalRows", totalRows);
        }
        return metrics;
    }

    /** 二分类指标统一使用同一正类标签；多分类继续使用宏平均口径。 */
    private Map<String, Object> classificationMetrics(List<String> labels, List<String> predictions,
            String requestedPositiveLabel) {
        labels = labels.stream().map(this::canonicalLabel).toList();
        predictions = predictions.stream().map(this::canonicalLabel).toList();
        requestedPositiveLabel = canonicalLabel(requestedPositiveLabel);
        List<String> classes = new ArrayList<>(new LinkedHashSet<>(labels));
        for (String prediction : predictions) {
            if (!classes.contains(prediction)) {
                classes.add(prediction);
            }
        }
        Collections.sort(classes);
        if (classes.size() != 2) {
            return new LinkedHashMap<>(ModelMetricsEvaluator.evaluate(labels, predictions, "classification"));
        }
        String positive = classes.contains(requestedPositiveLabel)
                ? requestedPositiveLabel : classes.get(classes.size() - 1);
        int tp = 0, fp = 0, fn = 0, tn = 0;
        for (int i = 0; i < labels.size(); i++) {
            boolean actualPositive = positive.equals(labels.get(i));
            boolean predictedPositive = positive.equals(predictions.get(i));
            if (actualPositive && predictedPositive) {
                tp++;
            } else if (!actualPositive && predictedPositive) {
                fp++;
            } else if (actualPositive) {
                fn++;
            } else {
                tn++;
            }
        }
        double accuracy = (tp + tn) / (double) labels.size();
        double precision = tp + fp == 0 ? 0 : tp / (double) (tp + fp);
        double recall = tp + fn == 0 ? 0 : tp / (double) (tp + fn);
        double f1 = precision + recall == 0 ? 0 : 2 * precision * recall / (precision + recall);
        Map<String, Object> confusion = new LinkedHashMap<>();
        confusion.put("positive", positive);
        confusion.put("tp", tp);
        confusion.put("fp", fp);
        confusion.put("fn", fn);
        confusion.put("tn", tn);
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("metricType", "classification");
        metrics.put("classes", classes);
        metrics.put("positiveLabel", positive);
        metrics.put("accuracy", round6(accuracy));
        metrics.put("precision", round6(precision));
        metrics.put("recall", round6(recall));
        metrics.put("f1", round6(f1));
        metrics.put("confusionMatrix", confusion);
        return metrics;
    }

    private Map<String, Object> clusteringMetrics(List<String> columns, List<List<String>> rows, int totalRows) {
        int clusterIndex = columns.indexOf("cluster");
        if (clusterIndex < 0) {
            return Map.of();
        }
        Map<String, Integer> distribution = new LinkedHashMap<>();
        for (List<String> row : rows) {
            if (clusterIndex < row.size()) {
                distribution.merge(row.get(clusterIndex), 1, Integer::sum);
            }
        }
        if (distribution.isEmpty()) {
            return Map.of();
        }
        int counted = distribution.values().stream().mapToInt(Integer::intValue).sum();
        Map<String, Object> ratios = new LinkedHashMap<>();
        distribution.forEach((cluster, count) ->
                ratios.put(cluster, round6(count / (double) counted)));
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("metricType", "clustering");
        metrics.put("samples", totalRows);
        metrics.put("clusterCount", distribution.size());
        metrics.put("clusterDistribution", distribution);
        metrics.put("clusterRatio", ratios);
        if (rows.size() < totalRows) {
            metrics.put("distributionSampleRows", rows.size());
        }
        return metrics;
    }

    /**
     * 二分类 ROC AUC（按正类概率排序的秩和公式，并列取平均秩）。
     * 标签非二分类、概率列缺失或存在非数值时返回 {@code null}，由调用方省略该指标。
     */
    private Double rocAuc(List<String> labels, List<String> probabilities) {
        return rocAuc(labels, probabilities, "");
    }

    private Double rocAuc(List<String> labels, List<String> probabilities, String requestedPositiveLabel) {
        labels = labels.stream().map(this::canonicalLabel).toList();
        requestedPositiveLabel = canonicalLabel(requestedPositiveLabel);
        Set<String> classes = new LinkedHashSet<>(labels);
        if (classes.size() != 2 || probabilities.size() != labels.size()) {
            return null;
        }
        List<String> ordered = new ArrayList<>(classes);
        Collections.sort(ordered);
        String positive = ordered.contains(requestedPositiveLabel) ? requestedPositiveLabel : ordered.get(1);
        List<double[]> scored = new ArrayList<>();
        for (int i = 0; i < labels.size(); i++) {
            String raw = probabilities.get(i);
            if (raw == null || raw.isBlank()) {
                return null;
            }
            try {
                scored.add(new double[]{Double.parseDouble(raw.trim()), positive.equals(labels.get(i)) ? 1 : 0});
            } catch (NumberFormatException e) {
                return null;
            }
        }
        scored.sort(Comparator.comparingDouble(item -> item[0]));
        double positives = 0;
        double negatives = 0;
        double rankSum = 0;
        int index = 0;
        while (index < scored.size()) {
            int tieEnd = index;
            while (tieEnd + 1 < scored.size() && scored.get(tieEnd + 1)[0] == scored.get(index)[0]) {
                tieEnd++;
            }
            double averageRank = (index + tieEnd + 2) / 2.0;
            for (int i = index; i <= tieEnd; i++) {
                if (scored.get(i)[1] == 1) {
                    positives++;
                    rankSum += averageRank;
                } else {
                    negatives++;
                }
            }
            index = tieEnd + 1;
        }
        if (positives == 0 || negatives == 0) {
            return null;
        }
        return round6((rankSum - positives * (positives + 1) / 2) / (positives * negatives));
    }

    private String canonicalLabel(String value) {
        String text = value == null ? "" : value.trim();
        try {
            BigDecimal number = new BigDecimal(text).stripTrailingZeros();
            return number.compareTo(BigDecimal.ZERO) == 0 ? "0" : number.toPlainString();
        } catch (NumberFormatException e) {
            return text;
        }
    }

    private static double round6(double value) {
        return Math.round(value * 1_000_000d) / 1_000_000d;
    }

    private Map<String, Object> tableSummary(String sandboxId, String table) {
        if (!notBlank(table)) {
            return Map.of();
        }
        try {
            Map<String, Object> preview = sandboxDb.previewTable(sandboxId, table, 1);
            return Map.of(
                    "rowCount", intValue(preview.get("totalRows"), 0),
                    "columnCount", schemaNames(preview.get("schema")).size());
        } catch (Exception e) {
            return Map.of();
        }
    }

    private GraphModel legacyTrainingGraph(Map<String, Object> canvasModel, String nodeId) {
        int version = intValue(canvasModel.get("canvas_version"), Integer.MAX_VALUE);
        for (Map<String, Object> row : jdbc.queryForList(
                "select graph_json from ds_compute_canvas_version where canvas_id=? and version<=? "
                        + "and deleted=0 order by version desc",
                canvasModel.get("canvas_id"), version)) {
            GraphModel candidate = parseGraph(string(row.get("graph_json")));
            Node node = candidate.nodeById(nodeId);
            if (node != null && CanvasOperatorRegistry.isTrain(node.componentCode)) {
                return candidate;
            }
        }
        return null;
    }

    private List<Map<String, Object>> modelTestHistory(String modelId) {
        return jdbc.queryForList(
                "select id,status,metric_type,run_mode,created_at,finished_at from ds_model_test "
                        + "where model_id=? and status='SUCCEEDED' and deleted=0 order by finished_at desc,created_at desc",
                modelId);
    }

    private List<Map<String, Object>> preprocessingReport(GraphModel graph, String runId, Node trainNode) {
        Set<String> upstream = upstream(graph, trainNode.id);
        Map<String, Map<String, Object>> runByNode = new LinkedHashMap<>();
        for (Map<String, Object> row : jdbc.queryForList(
                "select * from ds_compute_node_run where run_id=? and deleted=0 order by created_at asc", runId)) {
            runByNode.put(string(row.get("node_id")), row);
        }
        List<Map<String, Object>> steps = new ArrayList<>();
        int order = 0;
        for (Node node : topoSort(graph)) {
            if (!upstream.contains(node.id) || node.id.equals(trainNode.id)
                    || !node.componentCode.startsWith("preprocessing.")) {
                continue;
            }
            Map<String, Object> run = runByNode.getOrDefault(node.id, Map.of());
            Map<String, Object> step = new LinkedHashMap<>();
            step.put("order", ++order);
            step.put("nodeId", node.id);
            step.put("componentCode", node.componentCode);
            step.put("componentName", node.name);
            step.put("columns", stringList(node.params.get("columns")));
            step.put("appliesToAll", stringList(node.params.get("columns")).isEmpty());
            step.put("configuredParams", node.params);
            step.put("fittedParams", parseMapOrEmpty(string(run.get("fit_params"))));
            step.put("inputTable", string(run.get("input_table")));
            step.put("outputTable", string(run.get("output_table")));
            step.put("status", string(run.get("status")));
            steps.add(step);
        }
        return steps;
    }

    private List<String> featurePreprocessing(String feature, List<Map<String, Object>> steps) {
        List<String> names = new ArrayList<>();
        for (Map<String, Object> step : steps) {
            List<String> columns = stringList(step.get("columns"));
            Map<String, Object> params = step.get("configuredParams") instanceof Map<?, ?> raw
                    ? mapOf(raw) : Map.of();
            boolean derived = feature.equals(string(params.get("new_column")));
            if (Boolean.TRUE.equals(step.get("appliesToAll")) || columns.contains(feature) || derived) {
                names.add(string(step.get("componentName")));
            }
        }
        return names;
    }

    private Map<String, Map<String, Object>> schemaByName(List<Map<String, Object>> schema) {
        Map<String, Map<String, Object>> result = new LinkedHashMap<>();
        for (Map<String, Object> column : schema) {
            result.put(string(column.get("name")), column);
        }
        return result;
    }

    private String schemaType(Map<String, Object> column) {
        if (column == null) {
            return "UNKNOWN";
        }
        return firstNotBlank(string(column.get("type")), string(column.get("dataType")), "UNKNOWN");
    }

    /**
     * 查询「可执行的工作流结果」候选：工作流中最近一次成功运行的最终输出节点（终态节点）。
     * 每个候选携带输出表与整个工作流的输入数据（数据资源挂载表 + 列），供保存模型时确定 API 输入/输出。
     */
    public List<Map<String, Object>> modelCandidates(String canvasId) {
        Map<String, Object> canvas = requireCanvas(canvasId);
        String sandboxId = string(canvas.get("sandbox_id"));
        requireUsableSandbox(sandboxId, false);
        GraphModel graph = parseGraph(string(canvas.get("graph_json")));
        Map<String, Object> input = workflowInput(graph, sandboxId);
        String inputTable = string(input.get("table"));
        List<String> inputColumns = new ArrayList<>();
        Object ic = input.get("columns");
        if (ic instanceof List<?> list) {
            for (Object o : list) {
                inputColumns.add(String.valueOf(o));
            }
        }
        List<Map<String, Object>> rows = List.of();
        List<Map<String, Object>> runs = jdbc.queryForList(
                "select id from ds_compute_run where canvas_id=? and deleted=0 order by created_at desc limit 1", canvasId);
        if (!runs.isEmpty()) {
            rows = jdbc.queryForList("select * from ds_compute_node_run where run_id=? and status='SUCCEEDED' and deleted=0 "
                    + "order by finished_at asc,created_at asc", string(runs.get(0).get("id")));
        }
        List<Map<String, Object>> result = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (Map<String, Object> nr : rows) {
            String nodeId = string(nr.get("node_id"));
            Node node = graph.nodeById(nodeId);
            if (node == null || !CanvasOperatorRegistry.isTrain(node.componentCode) || !seen.add(nodeId)) {
                continue;
            }
            result.add(candidateRow(nr, node, inputTable, inputColumns));
        }
        if (result.isEmpty()) {
            // 最近一次运行无成功终态节点：回退到任意一次成功运行的终态节点
            for (Map<String, Object> nr : jdbc.queryForList(
                    "select * from ds_compute_node_run where canvas_id=? and status='SUCCEEDED' and deleted=0 "
                            + "order by finished_at desc,created_at desc limit 50", canvasId)) {
                String nodeId = string(nr.get("node_id"));
                Node node = graph.nodeById(nodeId);
                if (node == null || !CanvasOperatorRegistry.isTrain(node.componentCode) || !seen.add(nodeId)) {
                    continue;
                }
                result.add(candidateRow(nr, node, inputTable, inputColumns));
            }
        }
        return result;
    }

    private Map<String, Object> candidateRow(Map<String, Object> nr, Node node, String inputTable, List<String> inputColumns) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("node_id", node.id);
        item.put("node_name", node.name);
        item.put("name", node.name);
        item.put("component_code", node.componentCode);
        item.put("run_id", string(nr.get("run_id")));
        item.put("task_id", string(nr.get("task_id")));
        item.put("output_table", string(nr.get("output_table")));
        item.put("model_id", string(nr.get("model_id")));
        item.put("status", string(nr.get("status")));
        item.put("finished_at", string(nr.get("finished_at")));
        item.put("input_table", inputTable);
        item.put("input_columns", inputColumns);
        String taskType = CanvasOperatorRegistry.metricType(node.componentCode, node.params.get("task"));
        item.put("task_type", taskType.toUpperCase(Locale.ROOT));
        item.put("model_category", supportsTreeStructure(node.componentCode) ? "TREE" : "GENERAL");
        item.put("label", string(node.params.get("label")));
        item.put("available_metrics", applicableMetrics(taskType));
        item.put("available_sections", supportsTreeStructure(node.componentCode)
                ? List.of("featureImportance", "treeStructure", "scorecard") : List.of());
        return item;
    }

    private List<String> applicableMetrics(String taskType) {
        if ("regression".equalsIgnoreCase(taskType)) {
            return List.of("mae", "rmse", "r2");
        }
        if ("clustering".equalsIgnoreCase(taskType)) {
            return List.of("clusterCount", "clusterDistribution", "clusterRatio");
        }
        return List.of("accuracy", "precision", "recall", "f1", "auc", "confusionMatrix");
    }

    /** 报告配置只决定展示范围；评估执行始终计算当前任务类型的全部适用指标。 */
    private Map<String, Object> normalizeReportConfig(Object raw, String taskType, String modelCategory) {
        Map<String, Object> requested = raw instanceof Map<?, ?> map ? mapOf(map) : Map.of();
        List<String> available = applicableMetrics(taskType);
        List<String> visible = stringList(requested.get("visibleMetrics"));
        visible = visible.stream().filter(available::contains).distinct().toList();
        if (visible.isEmpty()) {
            visible = available;
        }

        List<String> sections = new ArrayList<>();
        if ("TREE".equals(modelCategory)) {
            List<String> allowed = List.of("featureImportance", "treeStructure", "scorecard");
            sections.addAll(stringList(requested.get("visibleSections")).stream()
                    .filter(allowed::contains).distinct().toList());
            if (!requested.containsKey("visibleSections")) {
                sections.addAll(allowed);
            }
        }

        Map<String, Object> config = new LinkedHashMap<>();
        config.put("taskType", taskType);
        config.put("modelCategory", modelCategory);
        config.put("visibleMetrics", visible);
        config.put("visibleSections", sections);
        if ("CLASSIFICATION".equals(taskType)) {
            String positiveLabel = firstNotBlank(string(requested.get("positiveLabel")), "1");
            config.put("positiveLabel", positiveLabel);
            config.put("threshold", requested.get("threshold") instanceof Number number
                    ? number.doubleValue() : 0.5);
        }
        if ("TREE".equals(modelCategory)) {
            Map<String, Object> scorecard = requested.get("scorecard") instanceof Map<?, ?> map
                    ? mapOf(map) : new LinkedHashMap<>();
            scorecard.putIfAbsent("enabled", true);
            scorecard.put("mode", "CLASSIFICATION".equals(taskType)
                    ? "BINARY_RISK" : "CONTINUOUS_REGRESSION");
            scorecard.putIfAbsent("baseScore", 600);
            scorecard.putIfAbsent("pdo", 20);
            scorecard.putIfAbsent("baseOdds", 20);
            scorecard.putIfAbsent("scoreMin", 300);
            scorecard.putIfAbsent("scoreMax", 900);
            scorecard.putIfAbsent("higherScoreForHigherPrediction", true);
            config.put("scorecard", scorecard);
        }
        return config;
    }

    /** 从训练输出表读取全部行并计算完整指标，展示筛选在报告读取阶段完成。 */
    private Map<String, Object> fullEvaluation(String sandboxId, String outputTable,
            Node trainNode, Map<String, Object> reportConfig) {
        if (!notBlank(outputTable)) {
            return Map.of();
        }
        Map<String, Object> table = sandboxDb.readTable(sandboxId, outputTable);
        List<String> columns = stringList(table.get("header"));
        List<List<String>> rows = tableRows(table.get("rows"));
        Map<String, Object> metrics = new LinkedHashMap<>(
                recoveredMetrics(trainNode, columns, rows, rows.size(), reportConfig));
        if (supportsTreeStructure(trainNode.componentCode)) {
            metrics.put("scorecard", scorecardSummary(columns, rows,
                    CanvasOperatorRegistry.metricType(trainNode.componentCode, trainNode.params.get("task")),
                    reportConfig));
        }
        return metrics;
    }

    /** 树模型评分卡使用真实预测输出生成聚合分布，不保存逐行评分。 */
    private Map<String, Object> scorecardSummary(List<String> columns, List<List<String>> rows,
            String taskType, Map<String, Object> reportConfig) {
        Map<String, Object> config = reportConfig.get("scorecard") instanceof Map<?, ?> map
                ? mapOf(map) : Map.of();
        if (Boolean.FALSE.equals(config.get("enabled"))) {
            return Map.of("status", "DISABLED");
        }
        int valueIndex = columns.indexOf("classification".equals(taskType) ? "pred_prob" : "pred");
        if (valueIndex < 0) {
            return Map.of("status", "UNAVAILABLE", "message",
                    "classification".equals(taskType) ? "缺少 pred_prob 概率列" : "缺少 pred 预测列");
        }
        List<Double> values = new ArrayList<>();
        for (List<String> row : rows) {
            if (valueIndex >= row.size()) {
                continue;
            }
            try {
                values.add(Double.parseDouble(row.get(valueIndex)));
            } catch (Exception ignored) {
                // 非数值行不进入评分卡汇总，评估样本数会反映实际参与数量。
            }
        }
        if (values.isEmpty()) {
            return Map.of("status", "UNAVAILABLE", "message", "预测列没有可评分的数值");
        }
        List<Double> scores = new ArrayList<>();
        Map<String, Object> result = new LinkedHashMap<>();
        if ("classification".equals(taskType)) {
            double baseScore = number(config.get("baseScore"), 600);
            double pdo = number(config.get("pdo"), 20);
            double baseOdds = Math.max(number(config.get("baseOdds"), 20), 0.000001);
            double factor = pdo / Math.log(2);
            double offset = baseScore + factor * Math.log(baseOdds);
            for (double probability : values) {
                double p = Math.max(0.000001, Math.min(0.999999, probability));
                scores.add(offset - factor * Math.log(p / (1 - p)));
            }
            result.put("mode", "BINARY_RISK");
            result.put("baseScore", baseScore);
            result.put("pdo", pdo);
            result.put("baseOdds", baseOdds);
        } else {
            List<Double> sorted = new ArrayList<>(values);
            Collections.sort(sorted);
            double lower = percentile(sorted, 0.05);
            double upper = percentile(sorted, 0.95);
            double scoreMin = number(config.get("scoreMin"), 300);
            double scoreMax = number(config.get("scoreMax"), 900);
            boolean ascending = !Boolean.FALSE.equals(config.get("higherScoreForHigherPrediction"));
            for (double value : values) {
                double ratio = upper == lower ? 0.5 : (Math.max(lower, Math.min(upper, value)) - lower) / (upper - lower);
                scores.add(ascending ? scoreMin + ratio * (scoreMax - scoreMin)
                        : scoreMax - ratio * (scoreMax - scoreMin));
            }
            result.put("mode", "CONTINUOUS_REGRESSION");
            result.put("scoreMin", scoreMin);
            result.put("scoreMax", scoreMax);
            result.put("predictionP5", round6(lower));
            result.put("predictionP95", round6(upper));
            result.put("higherScoreForHigherPrediction", ascending);
        }
        result.put("status", "AVAILABLE");
        result.put("samples", scores.size());
        result.put("minimum", round6(scores.stream().mapToDouble(Double::doubleValue).min().orElse(0)));
        result.put("maximum", round6(scores.stream().mapToDouble(Double::doubleValue).max().orElse(0)));
        result.put("average", round6(scores.stream().mapToDouble(Double::doubleValue).average().orElse(0)));
        result.put("distribution", scoreDistribution(scores, 10));
        return result;
    }

    private List<Map<String, Object>> scoreDistribution(List<Double> scores, int bins) {
        double min = scores.stream().mapToDouble(Double::doubleValue).min().orElse(0);
        double max = scores.stream().mapToDouble(Double::doubleValue).max().orElse(min);
        double width = max == min ? 1 : (max - min) / bins;
        int[] counts = new int[bins];
        for (double score : scores) {
            int index = max == min ? 0 : Math.min(bins - 1, (int) ((score - min) / width));
            counts[index]++;
        }
        List<Map<String, Object>> distribution = new ArrayList<>();
        for (int i = 0; i < bins; i++) {
            if (counts[i] == 0) {
                continue;
            }
            distribution.add(Map.of(
                    "from", round6(min + i * width),
                    "to", round6(min + (i + 1) * width),
                    "count", counts[i]));
        }
        return distribution;
    }

    private double percentile(List<Double> sorted, double percentile) {
        if (sorted.size() == 1) {
            return sorted.get(0);
        }
        double position = percentile * (sorted.size() - 1);
        int lower = (int) Math.floor(position);
        int upper = (int) Math.ceil(position);
        if (lower == upper) {
            return sorted.get(lower);
        }
        return sorted.get(lower) + (position - lower) * (sorted.get(upper) - sorted.get(lower));
    }

    private double number(Object value, double fallback) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        try {
            return Double.parseDouble(string(value));
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    /** 工作流输入数据：第一个已配置的数据资源节点（data.table 挂载表 + 列）。 */
    private Map<String, Object> workflowInput(GraphModel graph, String sandboxId) {
        for (Node n : graph.nodes) {
            if (CanvasOperatorRegistry.isVirtual(n.componentCode)) {
                String t = string(n.params.get("table"));
                if (notBlank(t) && sandboxDb.hasTable(sandboxId, t)) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("table", t);
                    m.put("columns", tableColumns(sandboxId, t));
                    m.put("nodeName", n.name);
                    return m;
                }
            }
        }
        return Map.of("table", "", "columns", List.of(), "nodeName", "");
    }

    /** 是否终态节点（无下游出边 = 工作流最终输出）。 */
    private boolean isTerminal(GraphModel graph, String nodeId) {
        for (Edge edge : graph.edges) {
            if (edge.source.equals(nodeId)) {
                return false;
            }
        }
        return true;
    }

    /** 节点最近一次成功运行的输出表（op_*）；无记录时回退到 legacy 画布级输出表名。 */
    private String latestOutputTable(String sandboxId, String canvasId, String nodeId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select output_table from ds_compute_node_run where canvas_id=? and node_id=? "
                        + "and status='SUCCEEDED' and deleted=0 order by finished_at desc,created_at desc limit 1",
                canvasId, nodeId);
        String table = rows.isEmpty() ? "" : string(rows.get(0).get("output_table"));
        if (notBlank(table) && sandboxDb.hasTable(sandboxId, table)) {
            return table;
        }
        return legacyOpTableName(canvasId, nodeId);
    }

    /** 沙箱表列名（通过预览 schema 读取；不可读时返回空）。 */
    private List<String> tableColumns(String sandboxId, String table) {
        try {
            Map<String, Object> p = sandboxDb.previewTable(sandboxId, table, 1);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> schema = (List<Map<String, Object>>) p.get("schema");
            List<String> cols = new ArrayList<>();
            for (Map<String, Object> c : schema) {
                cols.add(string(c.get("name")));
            }
            return cols;
        } catch (Exception e) {
            return List.of();
        }
    }

    /** 沙箱表字段快照（字段名 + 类型）；报告读取失败时返回空，禁止用其他批次结果替代。 */
    private List<Map<String, Object>> tableSchema(String sandboxId, String table) {
        if (!notBlank(table)) {
            return List.of();
        }
        try {
            Map<String, Object> preview = sandboxDb.previewTable(sandboxId, table, 1);
            Object raw = preview.get("schema");
            if (!(raw instanceof List<?> list)) {
                return List.of();
            }
            List<Map<String, Object>> schema = new ArrayList<>();
            for (Object item : list) {
                if (item instanceof Map<?, ?> map) {
                    schema.add(mapOf(map));
                }
            }
            return schema;
        } catch (Exception e) {
            log.warn("工作流模型报告读取表结构失败 sandboxId={} table={}: {}",
                    sandboxId, table, e.getMessage());
            return List.of();
        }
    }

    /** 画布节点输出友好名：<节点名>输出数据<同名序号>，如「标准化输出数据1」。 */
    private String operatorOutputName(Node node, GraphModel graph) {
        int idx = 0;
        String base = notBlank(node.name) ? node.name : node.componentCode;
        for (Node n : graph.nodes) {
            String nName = notBlank(n.name) ? n.name : n.componentCode;
            if (Objects.equals(base, nName)) {
                idx++;
                if (n.id.equals(node.id)) {
                    break;
                }
            }
        }
        return base + "输出数据" + idx;
    }

    /**
     * 将当前画布保存为工作流模型。未选择训练输出时保存为 DRAFT 拓扑快照；选择成功训练产生的
     * modelId 后保存为 READY，后续才允许在自定义算法中发布 API。
     */
    public Map<String, Object> saveModel(Map<String, Object> request) {
        String canvasId = string(request.get("canvasId"));
        if (!notBlank(canvasId)) {
            throw new IllegalArgumentException("canvasId 不能为空");
        }
        Map<String, Object> canvas = requireCanvas(canvasId);
        requireUsableSandbox(string(canvas.get("sandbox_id")), true);
        String graphJson = string(canvas.get("graph_json"));
        GraphModel graph = parseGraph(graphJson);
        if (graph.nodes.isEmpty()) {
            throw new IllegalArgumentException("空画布不能保存为模型，请先添加工作流组件");
        }
        topoSort(graph);

        String modelId = string(request.get("modelId"));
        String nodeId = string(request.get("nodeId"));
        String sourceNodeId = "";
        String sourceRunId = "";
        String sourceTaskId = "";
        String status = "DRAFT";
        String outputTable = "";
        String outputColumns = "[]";
        String taskType = "";
        String modelCategory = "";
        Map<String, Object> reportConfig = Map.of();
        Map<String, Object> evaluationMetrics = Map.of();
        String evaluationStatus = "";
        String evaluationError = "";
        String sandboxId = string(canvas.get("sandbox_id"));
        if (notBlank(nodeId)) {
            Node sourceNode = graph.nodeById(nodeId);
            if (sourceNode == null || !CanvasOperatorRegistry.isTrain(sourceNode.componentCode)) {
                throw new IllegalArgumentException("保存为模型只能选择成功运行的训练节点");
            }
            // 前端选择「可执行工作流结果」节点：取其最近一次成功运行的输出表与模型
            List<Map<String, Object>> nrs = jdbc.queryForList(
                    "select model_id,output_table,run_id,task_id from ds_compute_node_run where canvas_id=? and node_id=? "
                            + "and status='SUCCEEDED' and deleted=0 order by finished_at desc,created_at desc limit 1",
                    canvasId, nodeId);
            if (nrs.isEmpty()) {
                throw new IllegalArgumentException("所选工作流结果节点尚未成功运行: " + nodeId);
            }
            modelId = string(nrs.get(0).get("model_id"));
            sourceNodeId = nodeId;
            sourceRunId = string(nrs.get(0).get("run_id"));
            sourceTaskId = string(nrs.get(0).get("task_id"));
            outputTable = string(nrs.get(0).get("output_table"));
            if (notBlank(modelId)) {
                status = "READY";
            }
            if (notBlank(outputTable) && sandboxDb.hasTable(sandboxId, outputTable)) {
                outputColumns = json(tableColumns(sandboxId, outputTable));
            }
            taskType = CanvasOperatorRegistry.metricType(sourceNode.componentCode,
                    sourceNode.params.get("task")).toUpperCase(Locale.ROOT);
            modelCategory = supportsTreeStructure(sourceNode.componentCode) ? "TREE" : "GENERAL";
            reportConfig = normalizeReportConfig(request.get("reportConfig"), taskType, modelCategory);
            try {
                evaluationMetrics = fullEvaluation(sandboxId, outputTable, sourceNode, reportConfig);
                evaluationStatus = evaluationMetrics.isEmpty() ? "UNAVAILABLE" : "SUCCEEDED";
                if (evaluationMetrics.isEmpty()) {
                    evaluationError = "训练结果缺少评估所需的标签列或预测列";
                }
            } catch (Exception e) {
                evaluationStatus = "FAILED";
                evaluationError = truncate(e.getMessage(), 1900);
                log.warn("保存模型时全量评估失败 canvasId={} nodeId={}: {}", canvasId, nodeId, e.getMessage());
            }
        } else if (notBlank(modelId)) {
            // 兼容旧调用：仅传 modelId（训练节点产物）
            List<Map<String, Object>> candidates = jdbc.queryForList(
                    "select nr.node_id,nr.output_table,nr.run_id,nr.task_id from ds_compute_node_run nr "
                            + "join ds_model m on m.id=nr.model_id and m.deleted=0 "
                            + "where nr.canvas_id=? and nr.model_id=? and nr.status='SUCCEEDED' and nr.deleted=0 "
                            + "order by nr.finished_at desc limit 1",
                    canvasId, modelId);
            if (candidates.isEmpty()) {
                throw new IllegalArgumentException("所选模型不是该画布成功训练产生的模型");
            }
            sourceNodeId = string(candidates.get(0).get("node_id"));
            sourceRunId = string(candidates.get(0).get("run_id"));
            sourceTaskId = string(candidates.get(0).get("task_id"));
            outputTable = string(candidates.get(0).get("output_table"));
            status = "READY";
            if (notBlank(outputTable) && sandboxDb.hasTable(sandboxId, outputTable)) {
                outputColumns = json(tableColumns(sandboxId, outputTable));
            }
            Node sourceNode = graph.nodeById(sourceNodeId);
            if (sourceNode != null && CanvasOperatorRegistry.isTrain(sourceNode.componentCode)) {
                taskType = CanvasOperatorRegistry.metricType(sourceNode.componentCode,
                        sourceNode.params.get("task")).toUpperCase(Locale.ROOT);
                modelCategory = supportsTreeStructure(sourceNode.componentCode) ? "TREE" : "GENERAL";
                reportConfig = normalizeReportConfig(request.get("reportConfig"), taskType, modelCategory);
                try {
                    evaluationMetrics = fullEvaluation(sandboxId, outputTable, sourceNode, reportConfig);
                    evaluationStatus = evaluationMetrics.isEmpty() ? "UNAVAILABLE" : "SUCCEEDED";
                    if (evaluationMetrics.isEmpty()) {
                        evaluationError = "训练结果缺少评估所需的标签列或预测列";
                    }
                } catch (Exception e) {
                    evaluationStatus = "FAILED";
                    evaluationError = truncate(e.getMessage(), 1900);
                    log.warn("保存模型时全量评估失败 canvasId={} modelId={}: {}", canvasId, modelId, e.getMessage());
                }
            }
        }
        // 工作流输入数据（数据资源挂载表 + 列），供发布 API 时确定输入 schema
        Map<String, Object> input = workflowInput(graph, sandboxId);
        String inputTable = string(input.get("table"));
        String inputColumns = json(input.get("columns"));

        String name = string(request.get("name"));
        if (!notBlank(name)) {
            name = string(canvas.get("name")) + "-模型";
        }
        String id = "cm-" + shortId();
        String now = now();
        jdbc.update("insert into ds_compute_canvas_model(id,canvas_id,canvas_version,model_id,source_node_id,"
                        + "source_run_id,source_task_id,name,task_type,model_category,report_config,"
                        + "evaluation_metrics,evaluation_status,evaluation_error,"
                        + "description,graph_json,status,input_table,input_columns,output_table,output_columns,"
                        + "created_by,created_at,updated_at,deleted) "
                        + "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)",
                id, canvasId, intValue(canvas.get("version"), 1), modelId, sourceNodeId,
                sourceRunId, sourceTaskId, name, taskType, modelCategory, json(reportConfig),
                json(evaluationMetrics), evaluationStatus, evaluationError,
                string(request.get("description")), graphJson, status,
                inputTable, inputColumns, outputTable, outputColumns, actor(), now, now);
        audit("CANVAS_MODEL_SAVED", "CANVAS_MODEL", id,
                "canvas=" + canvasId + " version=" + canvas.get("version") + " model=" + modelId
                        + " input=" + inputTable + " output=" + outputTable, true);
        return requireCanvasModel(id);
    }

    /** 回滚：将画布 graph_json 恢复为指定版本内容（并自增版本 + 快照）。 */
    public Map<String, Object> rollbackVersion(String versionId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select * from ds_compute_canvas_version where id=? and deleted=0", versionId);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("画布版本不存在: " + versionId);
        }
        Map<String, Object> version = rows.get(0);
        String canvasId = string(version.get("canvas_id"));
        Map<String, Object> canvas = requireCanvas(canvasId);
        if (!Objects.equals(actor(), string(canvas.get("created_by")))) {
            throw new SecurityException("仅画布创建人可回滚版本");
        }
        int newVersion = intValue(canvas.get("version"), 0) + 1;
        String now = now();
        String graph = string(version.get("graph_json"));
        jdbc.update("update ds_compute_canvas set graph_json=?,version=?,updated_at=? where id=? and deleted=0",
                graph, newVersion, now, canvasId);
        jdbc.update("insert into ds_compute_canvas_version(id,canvas_id,version,name,graph_json,created_by,created_at,deleted) "
                        + "values(?,?,?,?,?,?,?,0)",
                "cv-" + shortId(), canvasId, newVersion, string(version.get("name")) + "-回滚", graph, actor(), now);
        audit("CANVAS_VERSION_ROLLBACK", "COMPUTE_CANVAS", canvasId, "from=" + string(version.get("version"))
                + " to=" + newVersion, true);
        return requireCanvas(canvasId);
    }

    /** 版本对比：结构级 diff（节点增删/参数变化、边增删），供前端可视化展示。 */
    public Map<String, Object> compareVersions(String versionIdA, String versionIdB) {
        Map<String, Object> a = requireVersion(versionIdA);
        Map<String, Object> b = requireVersion(versionIdB);
        GraphModel ga = parseGraph(string(a.get("graph_json")));
        GraphModel gb = parseGraph(string(b.get("graph_json")));
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("versionA", Map.of("id", versionIdA, "version", a.get("version"), "name", a.get("name")));
        result.put("versionB", Map.of("id", versionIdB, "version", b.get("version"), "name", b.get("name")));
        result.put("nodeAdds", diffNodes(ga, gb));
        result.put("nodeRemoves", diffNodes(gb, ga));
        result.put("edgeAdds", diffEdges(ga, gb));
        result.put("edgeRemoves", diffEdges(gb, ga));
        result.put("paramChanges", paramChanges(ga, gb));
        return result;
    }

    /* ============================== 执行引擎（后台线程） ============================== */

    private void executeRun(String runId, UserContextDTO user) {
        if (user != null) {
            UserContext.setBaseUser(user);
        }
        try {
            Map<String, Object> run = requireRun(runId);
            String canvasId = string(run.get("canvas_id"));
            String sandboxId = string(run.get("sandbox_id"));
            Map<String, Object> canvas = requireCanvas(canvasId);
            GraphModel graph = parseGraph(string(canvas.get("graph_json")));
            List<Node> order = topoSort(graph);
            String nodeDomain = string(jdbc.queryForMap("select owner_id from ds_sandbox where id=? and deleted=0", sandboxId).get("owner_id"));
            Set<String> included = new LinkedHashSet<>();
            for (Map<String, Object> nr : jdbc.queryForList(
                    "select node_id from ds_compute_node_run where run_id=? and deleted=0", runId)) {
                included.add(string(nr.get("node_id")));
            }
            jdbc.update("update ds_compute_run set status='RUNNING',started_at=?,updated_at=? where id=?",
                    now(), now(), runId);
            String failMessage = "";
            for (Node node : order) {
                if (!included.contains(node.id)) {
                    continue;
                }
                if (isCancelled(runId)) {
                    markRemainingCancelled(runId, node.id);
                    break;
                }
                try {
                    executeNode(node, runId, canvasId, sandboxId, canvas, graph, nodeDomain);
                } catch (Exception e) {
                    failMessage = truncate(e.getMessage(), 1900);
                    log.error("画布节点 {} 执行失败: {}", node.id, failMessage, e);
                    break;
                }
            }
            String status = isCancelled(runId) ? "CANCELLED"
                    : notBlank(failMessage) ? "FAILED" : "SUCCEEDED";
            jdbc.update("update ds_compute_run set status=?,error_message=?,finished_at=?,updated_at=? where id=?",
                    status, failMessage, now(), now(), runId);
            audit("CANVAS_RUN_FINISHED", "COMPUTE_RUN", runId, "canvas=" + canvasId + " status=" + status
                    + (notBlank(failMessage) ? " error=" + failMessage : ""), "SUCCEEDED".equals(status));
        } catch (Exception e) {
            log.error("画布运行后台执行失败 runId={}", runId, e);
            try {
                jdbc.update("update ds_compute_run set status='FAILED',error_message=?,finished_at=?,updated_at=? where id=?",
                        truncate(e.getMessage(), 1900), now(), now(), runId);
            } catch (Exception ignored) {
                // 运行记录可能已被删，忽略
            }
        } finally {
            UserContext.remove();
        }
    }

    private void executeNode(Node node, String runId, String canvasId, String sandboxId,
            Map<String, Object> canvas, GraphModel graph, String nodeDomain) {
        Map<String, Object> nodeRun = jdbc.queryForMap(
                "select * from ds_compute_node_run where run_id=? and node_id=? and deleted=0", runId, node.id);
        String nodeRunId = string(nodeRun.get("id"));
        jdbc.update("update ds_compute_node_run set status='RUNNING',started_at=?,updated_at=? where id=?",
                now(), now(), nodeRunId);
        try {
            if (CanvasOperatorRegistry.isVirtual(node.componentCode)) {
                String table = string(node.params.get("table"));
                if (!sandboxDb.hasTable(sandboxId, table)) {
                    throw new IllegalArgumentException("数据资源表不存在，请在数据资源节点配置已挂载数据表: " + table);
                }
                if (sandboxDb.isResultTable(sandboxId, table)) {
                    throw new IllegalArgumentException("画布输入不能引用计算结果表（result_*）: " + table);
                }
                dataControl.requireMountTableUsable(sandboxId, table);
                jdbc.update("update ds_compute_node_run set status='SUCCEEDED',input_table=?,output_table=?,finished_at=?,updated_at=? where id=?",
                        table, table, now(), now(), nodeRunId);
                return;
            }
            String inputTable = resolveInputTable(node, graph, runId, canvasId, sandboxId);
            dataControl.requireMountTableUsable(sandboxId, inputTable);
            Map<String, Object> params = new LinkedHashMap<>();
            params.put("op", node.componentCode);
            params.putAll(node.params);
            if (CanvasOperatorRegistry.needsCompareTable(node.componentCode)) {
                String compareTable = string(node.params.get("compare_table"));
                if (!sandboxDb.hasTable(sandboxId, compareTable)) {
                    throw new IllegalArgumentException("PSI/特征对齐需要配置 compare_table（参考表，沙箱内挂载表或上游 op_*）: " + compareTable);
                }
                if (sandboxDb.isResultTable(sandboxId, compareTable)) {
                    throw new IllegalArgumentException("参考表不能引用计算结果表（result_*）: " + compareTable);
                }
                dataControl.requireMountTableUsable(sandboxId, compareTable);
                params.put("compare_table", compareTable);
            }
            byte[] inputCsv = sandboxDb.readTableCsv(sandboxId, inputTable);
            if (inputCsv.length > MAX_INPUT_BYTES) {
                throw new IllegalArgumentException("输入数据超过 " + MAX_INPUT_BYTES + " 字节上限（当前 " + inputCsv.length + "），请先在数据开发中做行数裁剪");
            }
            String inputB64 = Base64.getEncoder().encodeToString(inputCsv);
            String outputTable = opTableName(runId, node.id);
            String taskId = dataDevService.createCanvasTask(sandboxId, canvasId, node.id, node.componentCode,
                    CanvasOperatorRegistry.RENDER_SCRIPT, params, List.of(), outputTable);
            dataDevService.claimCanvasTask(taskId);
            Set<String> allowedTables = new LinkedHashSet<>(Set.of(inputTable));
            if (CanvasOperatorRegistry.needsCompareTable(node.componentCode)) {
                allowedTables.add(string(params.get("compare_table")));
            }
            devJobExecutor.submitSandboxChannel(taskId, nodeDomain, inputB64, "PYTHON",
                    CanvasOperatorRegistry.RENDER_SCRIPT, params, List.of(), sandboxId, inputTable, outputTable,
                    allowedTables, "canvas");
            Map<String, Object> result = devJobExecutor.runAndAwait(taskId);
            if (!"SUCCEEDED".equals(string(result.get("status")))) {
                throw new IllegalStateException("节点执行失败: " + string(result.get("errorMessage")));
            }
            @SuppressWarnings("unchecked")
            List<String> header = (List<String>) result.get("header");
            @SuppressWarnings("unchecked")
            List<List<String>> rows = (List<List<String>>) result.get("rows");
            NodeMarkers markers = stripMarkers(rows);
            String modelB64 = markers.modelB64;
            String fitParams = markers.preprocJson;
            if (rows.isEmpty()) {
                throw new IllegalStateException("节点无有效输出行");
            }
            sandboxDb.backfillOperatorTable(sandboxId, runId, node.id, operatorOutputName(node, graph), header, rows);
            Map<String, Object> summary = new LinkedHashMap<>();
            summary.put("header", header);
            summary.put("rowCount", rows.size());
            summary.put("columnCount", header.size());
            jdbc.update("update ds_compute_node_run set status='SUCCEEDED',task_id=?,input_table=?,output_table=?,"
                            + "result_summary=?,model_b64=?,fit_params=?,finished_at=?,updated_at=? where id=?",
                    taskId, inputTable, outputTable, json(summary), modelB64, fitParams, now(), now(), nodeRunId);
            boolean modelRegistered = false;
            if (CanvasOperatorRegistry.isTrain(node.componentCode) && notBlank(modelB64)) {
                modelRegistered = registerModelFromTrainNode(canvas, node, modelB64, sandboxId, graph, runId);
            }
            audit("CANVAS_NODE_SUCCEEDED", "COMPUTE_NODE_RUN", nodeRunId,
                    "node=" + node.id + " op=" + node.componentCode + " rows=" + rows.size()
                            + (notBlank(modelB64) ? " modelCaptured=true" : "")
                            + (modelRegistered ? " modelRegistered=true" : ""), true);
        } catch (Exception e) {
            String message = truncate(e.getMessage(), 1900);
            log.error("画布节点执行异常 nodeId={} op={}", node.id, node.componentCode, e);
            jdbc.update("update ds_compute_node_run set status='FAILED',error_message=?,finished_at=?,updated_at=? where id=?",
                    message, now(), now(), nodeRunId);
            audit("CANVAS_NODE_FAILED", "COMPUTE_NODE_RUN", nodeRunId, "node=" + node.id + " error=" + message, false);
            throw new IllegalStateException("节点 " + node.name + "（" + node.componentCode + "）执行失败: " + message, e);
        }
    }

    /** 剥离输出行尾部的 MODELB64 / PREPROC 标记行，返回模型 base64 与预处理拟合参数。 */
    private NodeMarkers stripMarkers(List<List<String>> rows) {
        NodeMarkers markers = new NodeMarkers();
        while (!rows.isEmpty()) {
            List<String> last = rows.get(rows.size() - 1);
            String key = string(last.size() >= 1 ? last.get(0) : "");
            if (MODEL_MARKER.equals(key)) {
                markers.modelB64 = last.size() >= 2 ? last.get(1) : "";
                rows.remove(rows.size() - 1);
            } else if (PREPROC_MARKER.equals(key)) {
                String encoded = last.size() >= 2 ? last.get(1) : "";
                try {
                    markers.preprocJson = new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
                } catch (IllegalArgumentException ignored) {
                    markers.preprocJson = "";
                }
                rows.remove(rows.size() - 1);
            } else {
                break;
            }
        }
        return markers;
    }

    private static final class NodeMarkers {
        String modelB64 = "";
        String preprocJson = "";
    }

    /**
     * 训练节点产物注册：{@code ds_dev_artifact(PYTHON) + 版本(predict 脚本) + ds_model(APPROVED)}，按制品名幂等。
     *
     * <p>制品与模型均标记 {@code source='CANVAS'}，开发制品列表与任务列表据此过滤，
     * 画布产物不会出现在数据开发中；模型侧保持可见，否则无法保存工作流模型与发布 API。
     * 注册失败不影响节点本身的成功状态，仅记录审计与告警——此时工作流模型退化为快照。</p>
     *
     * @return 是否成功回填 {@code ds_compute_node_run.model_id}
     */
    private boolean registerModelFromTrainNode(Map<String, Object> canvas, Node node, String modelB64, String sandboxId,
            GraphModel graph, String runId) {
        try {
            String projectId = string(canvas.get("project_id"));
            String artifactName = "画布模型-" + string(canvas.get("name")) + "-" + string(node.name);
            List<String> features = stringList(node.params.get("features"));
            String task = string(node.params.get("task"));
            String kind = node.componentCode.startsWith("ml.") ? node.componentCode.substring(3) : node.componentCode;
            String preprocess = buildPreprocessScript(node, graph, runId);
            String script = CanvasPredictScript.generate(modelB64, kind, features, task, preprocess);
            List<Map<String, Object>> existing = jdbc.queryForList(
                    "select * from ds_dev_artifact where name=? and sandbox_id=? and deleted=0", artifactName, sandboxId);
            String artifactId;
            if (existing.isEmpty()) {
                Map<String, Object> req = new LinkedHashMap<>();
                req.put("name", artifactName);
                req.put("type", "PYTHON");
                req.put("projectId", projectId);
                req.put("sandboxId", sandboxId);
                req.put("source", ARTIFACT_SOURCE_CANVAS);
                req.put("description", "画布节点 " + node.id + " 训练产物（" + kind + "），推理脚本自动生成");
                artifactId = string(dataDevService.createArtifact(req).get("id"));
            } else {
                artifactId = string(existing.get(0).get("id"));
            }
            Map<String, Object> vreq = new LinkedHashMap<>();
            vreq.put("artifactId", artifactId);
            vreq.put("contentText", script);
            vreq.put("description", "训练时间 " + now());
            String versionId = string(dataDevService.createVersion(vreq).get("id"));
            Map<String, Object> model = modelApprovalService.registerModelAutoApproved(
                    artifactName, projectId, artifactId, versionId, sandboxId,
                    "画布训练产物自动注册（" + kind + "）");
            String modelId = string(model.get("id"));
            jdbc.update("update ds_compute_node_run set model_id=?,updated_at=? "
                            + "where run_id=? and node_id=? and deleted=0",
                    modelId, now(), runId, node.id);
            audit("CANVAS_MODEL_AUTO_REGISTERED", "MODEL", modelId, "canvas=" + string(canvas.get("id"))
                    + " artifact=" + artifactId + " v=" + versionId, true);
            return true;
        } catch (Exception e) {
            log.warn("画布训练产物注册失败 node={} op={}: {}", node.id, node.componentCode, e.getMessage(), e);
            audit("CANVAS_MODEL_AUTO_REGISTER_FAILED", "COMPUTE_NODE_RUN", node.id,
                    "canvas=" + string(canvas.get("id")) + " error=" + truncate(e.getMessage(), 400), false);
            return false;
        }
    }

    /* ====================== predict 脚本预处理链复刻 ====================== */

    /**
     * 生成 predict 脚本内嵌的 {@code _preprocess(df)} 函数体：把训练节点上游的预处理变换链
     * （data.table → fillna/outlier/standardize/binning/unique/derive → 训练节点）用执行时回传的
     * 拟合参数逐节点复刻，保证 API 推理输入与画布训练特征分布一致。
     */
    private String buildPreprocessScript(Node trainNode, GraphModel graph, String runId) {
        List<Node> ancestors = new ArrayList<>();
        Deque<String> queue = new LinkedList<>();
        Set<String> seen = new LinkedHashSet<>();
        queue.addLast(trainNode.id);
        seen.add(trainNode.id);
        while (!queue.isEmpty()) {
            String id = queue.pollFirst();
            for (Edge edge : graph.edges) {
                if (edge.target.equals(id) && seen.add(edge.source)) {
                    Node node = graph.nodeById(edge.source);
                    if (node != null) {
                        ancestors.add(node);
                        queue.addLast(node.id);
                    }
                }
            }
        }
        Collections.reverse(ancestors); // data.table → 训练节点 拓扑序
        StringBuilder lines = new StringBuilder();
        for (Node node : ancestors) {
            if (CanvasOperatorRegistry.isVirtual(node.componentCode) || !REPLAYABLE_OPS.contains(node.componentCode)) {
                continue;
            }
            Map<String, Object> fit = loadFitParams(runId, node.id);
            if (fit == null) {
                continue;
            }
            lines.append(preprocessLines(node.componentCode, fit));
        }
        return lines.toString();
    }

    private Map<String, Object> loadFitParams(String runId, String nodeId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select fit_params from ds_compute_node_run where run_id=? and node_id=? and deleted=0", runId, nodeId);
        if (rows.isEmpty()) {
            return null;
        }
        String fp = string(rows.get(0).get("fit_params"));
        if (!notBlank(fp)) {
            return null;
        }
        try {
            return parseMap(fp);
        } catch (Exception e) {
            return null;
        }
    }

    private String preprocessLines(String componentCode, Map<String, Object> fit) {
        StringBuilder sb = new StringBuilder();
        switch (componentCode) {
            case "preprocessing.fillna": {
                Map<String, Object> values = map(fit.get("values"));
                for (Map.Entry<String, Object> e : values.entrySet()) {
                    // 守卫：调用方输入可能不含训练时的全量列（如只传特征列），缺失列跳过即可
                    sb.append("    if '").append(esc(e.getKey())).append("' in df.columns:\n");
                    sb.append("        df['").append(esc(e.getKey())).append("'] = df['").append(esc(e.getKey()))
                            .append("'].fillna(").append(num(e.getValue())).append(")\n");
                }
                break;
            }
            case "preprocessing.outlier": {
                Map<String, Object> bounds = map(fit.get("bounds"));
                for (Map.Entry<String, Object> e : bounds.entrySet()) {
                    Map<String, Object> b = map(e.getValue());
                    sb.append("    if '").append(esc(e.getKey())).append("' in df.columns:\n");
                    sb.append("        df['").append(esc(e.getKey())).append("'] = df['").append(esc(e.getKey()))
                            .append("'].clip(").append(num(b.get("lo"))).append(", ").append(num(b.get("hi"))).append(")\n");
                }
                break;
            }
            case "preprocessing.standardize": {
                String method = string(fit.get("method"));
                Map<String, Object> scaler = map(fit.get("scaler"));
                for (Map.Entry<String, Object> e : scaler.entrySet()) {
                    Map<String, Object> s = map(e.getValue());
                    sb.append("    if '").append(esc(e.getKey())).append("' in df.columns:\n");
                    if ("minmax".equals(method)) {
                        sb.append("        df['").append(esc(e.getKey())).append("'] = (df['").append(esc(e.getKey()))
                                .append("'] - ").append(num(s.get("min"))).append(") / (").append(num(s.get("max")))
                                .append(" - ").append(num(s.get("min"))).append(")\n");
                    } else {
                        sb.append("        df['").append(esc(e.getKey())).append("'] = (df['").append(esc(e.getKey()))
                                .append("'] - ").append(num(s.get("mean"))).append(") / ").append(num(s.get("std"))).append("\n");
                    }
                }
                break;
            }
            case "preprocessing.binning": {
                Map<String, Object> edges = map(fit.get("edges"));
                for (Map.Entry<String, Object> e : edges.entrySet()) {
                    sb.append("    if '").append(esc(e.getKey())).append("' in df.columns:\n");
                    sb.append("        df['").append(esc(e.getKey())).append("'] = pd.cut(df['").append(esc(e.getKey()))
                            .append("'], bins=").append(json(e.getValue())).append(", include_lowest=True)\n");
                }
                break;
            }
            case "preprocessing.unique": {
                Object drop = fit.get("drop");
                if (drop instanceof List<?> list && !list.isEmpty()) {
                    sb.append("    df = df.drop(columns=").append(json(list)).append(", errors='ignore')\n");
                }
                break;
            }
            case "preprocessing.derive": {
                String expr = string(fit.get("expression"));
                String newCol = string(fit.get("new_column"));
                if (!notBlank(expr) || !notBlank(newCol)) {
                    break;
                }
                sb.append("    _ns = dict(df); _ns.update({'np': np, 'pd': pd})\n");
                sb.append("    _s = pd.Series(eval(").append(pyQuote(expr))
                        .append(", {'__builtins__': {'float':float,'int':int,'str':str,'bool':bool,'abs':abs,'round':round,"
                                + "'min':min,'max':max,'len':len,'sum':sum,'True':True,'False':False,'None':None}}, _ns), index=df.index)\n");
                String cast = string(fit.get("cast"));
                if (notBlank(cast)) {
                    sb.append("    _s = _s.astype('").append(esc(cast)).append("')\n");
                }
                sb.append("    df['").append(esc(newCol)).append("'] = _s\n");
                break;
            }
            default:
                break;
        }
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object value) {
        if (value instanceof Map<?, ?> m) {
            Map<String, Object> out = new LinkedHashMap<>();
            m.forEach((k, v) -> out.put(String.valueOf(k), v));
            return out;
        }
        return new LinkedHashMap<>();
    }

    private static String esc(String s) {
        return s.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "");
    }

    private static String pyQuote(String s) {
        return "'" + esc(s) + "'";
    }

    private static String num(Object v) {
        if (v == null) {
            return "0";
        }
        String s = String.valueOf(v);
        return s.isEmpty() ? "0" : s;
    }

    /* ============================== 图解析 / 拓扑 ============================== */

    private GraphModel parseGraph(String graphJson) {
        Map<String, Object> graph = parseMap(graphJson);
        GraphModel model = new GraphModel();
        @SuppressWarnings("unchecked")
        List<Object> nodes = graph.get("nodes") instanceof List<?> list ? (List<Object>) list : List.of();
        for (Object raw : nodes) {
            if (!(raw instanceof Map<?, ?> map)) {
                continue;
            }
            Map<String, Object> node = new LinkedHashMap<>();
            map.forEach((k, v) -> node.put(String.valueOf(k), v));
            String id = string(node.get("id"));
            Object dataObj = node.get("data");
            Map<String, Object> data = dataObj instanceof Map<?, ?> d ? mapOf(d) : new LinkedHashMap<>();
            String componentCode = firstNotBlank(string(data.get("componentCode")), string(data.get("code")), string(node.get("componentCode")));
            Object paramsObj = data.get("params") != null ? data.get("params") : data.get("param");
            Map<String, Object> params = paramsObj instanceof Map<?, ?> p ? mapOf(p) : new LinkedHashMap<>();
            String name = firstNotBlank(string(data.get("name")), componentCode);
            model.nodes.add(new Node(id, componentCode, name, params));
        }
        @SuppressWarnings("unchecked")
        List<Object> edges = graph.get("edges") instanceof List<?> list ? (List<Object>) list : List.of();
        for (Object raw : edges) {
            if (!(raw instanceof Map<?, ?> map)) {
                continue;
            }
            Map<String, Object> edge = new LinkedHashMap<>();
            map.forEach((k, v) -> edge.put(String.valueOf(k), v));
            String source = string(edge.get("source"));
            String target = string(edge.get("target"));
            if (notBlank(source) && notBlank(target)) {
                model.edges.add(new Edge(source, target));
            }
        }
        return model;
    }

    /** Kahn 拓扑排序：detect cycle + 保证源节点先于下游执行。 */
    private List<Node> topoSort(GraphModel graph) {
        Map<String, Integer> inDegree = new HashMap<>();
        Map<String, Set<String>> adjacency = new LinkedHashMap<>();
        for (Node node : graph.nodes) {
            inDegree.put(node.id, 0);
            adjacency.put(node.id, new LinkedHashSet<>());
        }
        for (Edge edge : graph.edges) {
            if (!adjacency.containsKey(edge.source) || !adjacency.containsKey(edge.target)) {
                throw new IllegalArgumentException("边引用了不存在的节点: " + edge.source + " -> " + edge.target);
            }
            if (adjacency.get(edge.source).add(edge.target)) {
                inDegree.put(edge.target, inDegree.get(edge.target) + 1);
            }
        }
        Deque<String> queue = new LinkedList<>();
        for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                queue.addLast(entry.getKey());
            }
        }
        List<String> sortedIds = new ArrayList<>();
        while (!queue.isEmpty()) {
            String id = queue.pollFirst();
            sortedIds.add(id);
            for (String next : adjacency.get(id)) {
                int deg = inDegree.get(next) - 1;
                inDegree.put(next, deg);
                if (deg == 0) {
                    queue.addLast(next);
                }
            }
        }
        if (sortedIds.size() != graph.nodes.size()) {
            throw new IllegalArgumentException("画布存在循环依赖，无法执行（请检查连线）");
        }
        List<Node> result = new ArrayList<>();
        for (String id : sortedIds) {
            result.add(graph.nodeById(id));
        }
        return result;
    }

    private Set<String> selectNodes(String mode, String nodeId, GraphModel graph) {
        switch (mode) {
            case "SINGLE":
                return notBlank(nodeId) ? Set.of(nodeId) : Set.of();
            case "DOWN":
            case "CONTINUE":
                return downstream(graph, nodeId);
            case "UP":
                return upstream(graph, nodeId);
            case "ALL":
            default: {
                Set<String> all = new LinkedHashSet<>();
                for (Node node : graph.nodes) {
                    all.add(node.id);
                }
                return all;
            }
        }
    }

    private Set<String> downstream(GraphModel graph, String nodeId) {
        if (!notBlank(nodeId)) {
            return Set.of();
        }
        Set<String> result = new LinkedHashSet<>();
        result.add(nodeId);
        Set<String> frontier = new LinkedHashSet<>(Set.of(nodeId));
        while (!frontier.isEmpty()) {
            Set<String> next = new LinkedHashSet<>();
            for (Edge edge : graph.edges) {
                if (frontier.contains(edge.source) && result.add(edge.target)) {
                    next.add(edge.target);
                }
            }
            frontier = next;
        }
        return result;
    }

    private Set<String> upstream(GraphModel graph, String nodeId) {
        if (!notBlank(nodeId)) {
            return Set.of();
        }
        Set<String> result = new LinkedHashSet<>();
        result.add(nodeId);
        Set<String> frontier = new LinkedHashSet<>(Set.of(nodeId));
        while (!frontier.isEmpty()) {
            Set<String> next = new LinkedHashSet<>();
            for (Edge edge : graph.edges) {
                if (frontier.contains(edge.target) && result.add(edge.source)) {
                    next.add(edge.source);
                }
            }
            frontier = next;
        }
        return result;
    }

    private String resolveInputTable(Node node, GraphModel graph, String runId, String canvasId, String sandboxId) {
        List<String> sources = new ArrayList<>();
        for (Edge edge : graph.edges) {
            if (edge.target.equals(node.id)) {
                sources.add(edge.source);
            }
        }
        if (sources.isEmpty()) {
            throw new IllegalArgumentException("节点 " + node.name + "（" + node.componentCode + "）缺少输入（请从数据资源节点连线）");
        }
        if (sources.size() > 1) {
            throw new IllegalArgumentException("节点 " + node.name + " 存在多个输入，当前仅支持单输入算子");
        }
        Node source = graph.nodeById(sources.get(0));
        String table;
        if (CanvasOperatorRegistry.isVirtual(source.componentCode)) {
            table = string(source.params.get("table"));
            if (!sandboxDb.hasTable(sandboxId, table)) {
                throw new IllegalArgumentException("上游数据资源节点未配置挂载表，无法提供输入: " + sources.get(0));
            }
        } else {
            List<Map<String, Object>> current = jdbc.queryForList(
                    "select output_table from ds_compute_node_run where run_id=? and node_id=? and status='SUCCEEDED' and deleted=0 limit 1",
                    runId, source.id);
            if (!current.isEmpty()) {
                table = string(current.get(0).get("output_table"));
            } else {
                List<Map<String, Object>> previous = jdbc.queryForList(
                        "select output_table from ds_compute_node_run where canvas_id=? and node_id=? "
                                + "and run_id<>? and status='SUCCEEDED' and deleted=0 "
                                + "order by finished_at desc,created_at desc limit 1",
                        canvasId, source.id, runId);
                table = previous.isEmpty() ? legacyOpTableName(canvasId, source.id)
                        : string(previous.get(0).get("output_table"));
            }
        }
        if (!sandboxDb.hasTable(sandboxId, table)) {
            throw new IllegalArgumentException("上游节点输出表不存在（请先执行上游节点）: " + table);
        }
        if (sandboxDb.isResultTable(sandboxId, table)) {
            throw new IllegalArgumentException("画布节点不能引用计算结果表（result_*）作为输入: " + table);
        }
        dataControl.requireMountTableUsable(sandboxId, table);
        return table;
    }

    private boolean isCancelled(String runId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select status from ds_compute_run where id=? and deleted=0", runId);
        return !rows.isEmpty() && "CANCELLED".equals(string(rows.get(0).get("status")));
    }

    private void markRemainingCancelled(String runId, String fromNodeId) {
        // 取消所有尚未启动的节点（含 isCancelled 检查时当前待执行节点），避免留下 PENDING 残影
        jdbc.update("update ds_compute_node_run set status='CANCELLED',finished_at=?,updated_at=? where run_id=? and status='PENDING'",
                now(), now(), runId);
    }

    private void stopTask(String taskId) {
        try {
            devJobExecutor.stop("dt-" + taskId, "Canvas run cancelled");
        } catch (Exception e) {
            log.debug("停止画布任务 {} 失败: {}", taskId, e.getMessage());
        }
        try {
            devJobExecutor.delete("dt-" + taskId);
        } catch (Exception e) {
            log.debug("删除画布任务 {} 失败: {}", taskId, e.getMessage());
        }
    }

    private List<Map<String, Object>> nodeRuns(String runId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select * from ds_compute_node_run where run_id=? and deleted=0 order by created_at asc", runId);
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            Map<String, Object> item = new LinkedHashMap<>(row);
            Object summary = item.remove("result_summary");
            item.put("resultSummary", parseMapOrEmpty(String.valueOf(summary == null ? "" : summary)));
            result.add(item);
        }
        return result;
    }

    private Map<String, Object> nodeRunMap(String runId) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (Map<String, Object> nr : nodeRuns(runId)) {
            map.put(string(nr.get("node_id")), nr);
        }
        return map;
    }

    /* ============================== 版本 diff ============================== */

    private List<String> diffNodes(GraphModel a, GraphModel b) {
        List<String> result = new ArrayList<>();
        for (Node node : b.nodes) {
            if (a.nodeById(node.id) == null) {
                result.add(node.name + "（" + node.componentCode + "）");
            }
        }
        return result;
    }

    private List<String> diffEdges(GraphModel a, GraphModel b) {
        List<String> result = new ArrayList<>();
        for (Edge edge : b.edges) {
            if (!a.hasEdge(edge.source, edge.target)) {
                result.add(edge.source + " → " + edge.target);
            }
        }
        return result;
    }

    private List<Map<String, Object>> paramChanges(GraphModel a, GraphModel b) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Node nodeB : b.nodes) {
            Node nodeA = a.nodeById(nodeB.id);
            if (nodeA == null) {
                continue;
            }
            Map<String, Object> changes = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : nodeB.params.entrySet()) {
                Object old = nodeA.params.get(entry.getKey());
                if (!Objects.equals(String.valueOf(old), String.valueOf(entry.getValue()))) {
                    changes.put(entry.getKey(), Map.of("from", String.valueOf(old), "to", String.valueOf(entry.getValue())));
                }
            }
            if (!changes.isEmpty()) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("nodeId", nodeB.id);
                item.put("nodeName", nodeB.name);
                item.put("changes", changes);
                result.add(item);
            }
        }
        return result;
    }

    /* ============================== 内部工具 ============================== */

    private Map<String, Object> requireCanvas(String id) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select * from ds_compute_canvas where id=? and deleted=0", id);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("画布不存在: " + id);
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    private Map<String, Object> requireRun(String id) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select * from ds_compute_run where id=? and deleted=0", id);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("运行记录不存在: " + id);
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    private Map<String, Object> requireVersion(String id) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select * from ds_compute_canvas_version where id=? and deleted=0", id);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("画布版本不存在: " + id);
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    private Map<String, Object> requireCanvasModel(String id) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select cm.*,m.status model_status,m.version model_version,m.artifact_id,m.artifact_version_id "
                        + "from ds_compute_canvas_model cm left join ds_model m on m.id=cm.model_id and m.deleted=0 "
                        + "where cm.id=? and cm.deleted=0",
                id);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("工作流模型不存在: " + id);
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    private Map<String, Object> requireSandbox(String id) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select * from ds_sandbox where id=? and deleted=0", id);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("沙箱不存在: " + id);
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    private void requireUsableSandbox(String id, boolean creatorRequired) {
        Map<String, Object> sandbox = requireSandbox(id);
        String projectId = string(sandbox.get("project_id"));
        String node = nodeId();
        Long member = jdbc.queryForObject(
                "select count(1) from project_node where project_id=? and node_id=? and is_deleted=0",
                Long.class, projectId, node);
        if (member == null || member == 0) {
            throw new IllegalArgumentException("当前节点不是该项目成员，无权限操作该沙箱");
        }
        if (creatorRequired && (!matchesNode(string(sandbox.get("owner_id"))) || !Objects.equals(actor(), string(sandbox.get("created_by"))))) {
            throw new IllegalArgumentException("该沙箱仅创建人可执行此操作");
        }
    }

    private String nodeId() {
        UserContextDTO user = UserContext.getUserOrNotExist();
        if (user == null) {
            return "kuscia-system";
        }
        return user.getPlatformNodeId() != null && !user.getPlatformNodeId().isBlank()
                ? user.getPlatformNodeId() : string(user.getOwnerId());
    }

    private boolean matchesNode(String candidate) {
        UserContextDTO user = UserContext.getUserOrNotExist();
        return Objects.equals(nodeId(), candidate)
                || (user != null && Objects.equals(string(user.getOwnerId()), candidate));
    }

    private String actor() {
        UserContextDTO user = UserContext.getUserOrNotExist();
        return user == null || user.getName() == null || user.getName().isBlank() ? "system" : user.getName();
    }

    private void audit(String action, String resourceType, String resourceId, String detail, boolean success) {
        try {
            mvp.auditAs("OPERATION", success ? "INFO" : "ERROR", actor(), action, resourceType, resourceId, detail, success);
        } catch (Exception e) {
            log.debug("画布审计失败: {}", e.getMessage());
        }
    }

    private static String opTableName(String runId, String nodeId) {
        return "op_" + sanitize(runId) + "_" + sanitize(nodeId);
    }

    private static String legacyOpTableName(String canvasId, String nodeId) {
        return "op_" + sanitize(canvasId) + "_" + sanitize(nodeId);
    }

    private static String sanitize(String value) {
        return value == null ? "" : value.replaceAll("[^A-Za-z0-9_]", "_");
    }

    private static String normalizeMode(String mode) {
        String m = mode == null ? "" : mode.trim().toUpperCase();
        return Set.of("ALL", "SINGLE", "DOWN", "UP", "CONTINUE").contains(m) ? m : "ALL";
    }

    private static String firstNotBlank(String a, String b, String c) {
        return notBlank(a) ? a : notBlank(b) ? b : notBlank(c) ? c : "";
    }

    private static String firstNotBlank(String a, String b) {
        return notBlank(a) ? a : notBlank(b) ? b : "";
    }

    private String json(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalArgumentException("JSON 序列化失败", e);
        }
    }

    private Map<String, Object> parseMap(String value) {
        try {
            if (!notBlank(value) || "{}".equals(value.trim())) {
                return new LinkedHashMap<>();
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) mapper.readValue(value, Map.class);
            return map == null ? new LinkedHashMap<>() : map;
        } catch (Exception e) {
            throw new IllegalArgumentException("画布 graph_json 格式错误", e);
        }
    }

    private Map<String, Object> parseMapOrEmpty(String value) {
        try {
            if (!notBlank(value)) {
                return new LinkedHashMap<>();
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) mapper.readValue(value, Map.class);
            return map == null ? new LinkedHashMap<>() : map;
        } catch (Exception e) {
            return new LinkedHashMap<>();
        }
    }

    private static Map<String, Object> mapOf(Map<?, ?> map) {
        Map<String, Object> result = new LinkedHashMap<>();
        map.forEach((k, v) -> result.put(String.valueOf(k), v));
        return result;
    }

    private static List<String> schemaNames(Object value) {
        if (!(value instanceof List<?> list)) {
            return List.of();
        }
        List<String> names = new ArrayList<>();
        for (Object item : list) {
            if (item instanceof Map<?, ?> map) {
                names.add(string(map.get("name")));
            }
        }
        return names;
    }

    private static List<List<String>> tableRows(Object value) {
        if (!(value instanceof List<?> list)) {
            return List.of();
        }
        List<List<String>> rows = new ArrayList<>();
        for (Object item : list) {
            if (item instanceof List<?> row) {
                List<String> values = new ArrayList<>();
                for (Object cell : row) {
                    values.add(string(cell));
                }
                rows.add(values);
            }
        }
        return rows;
    }

    private static List<String> stringList(Object value) {
        List<String> result = new ArrayList<>();
        if (value instanceof List<?> list) {
            for (Object item : list) {
                result.add(String.valueOf(item));
            }
        } else if (value != null && !String.valueOf(value).isBlank()) {
            result.add(String.valueOf(value));
        }
        return result;
    }

    private static int intValue(Object v, int d) {
        if (v instanceof Number n) {
            return n.intValue();
        }
        if (v == null || String.valueOf(v).isBlank()) {
            return d;
        }
        try {
            return Integer.parseInt(String.valueOf(v).trim());
        } catch (NumberFormatException e) {
            return d;
        }
    }

    private static String string(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }

    private static String truncate(String value, int max) {
        if (value == null) {
            return "";
        }
        return value.length() <= max ? value : value.substring(0, max);
    }

    private static String now() {
        return java.time.LocalDateTime.now().truncatedTo(java.time.temporal.ChronoUnit.SECONDS).toString();
    }

    private static String shortId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    private static final long MAX_INPUT_BYTES = 256 * 1024L;
    private static final String MODEL_MARKER = "MODELB64:";
    private static final String PREPROC_MARKER = "PREPROC:";
    /** 可复刻进 predict 脚本的预处理算子（拟合参数已在执行时回传）。 */
    private static final Set<String> REPLAYABLE_OPS = Set.of(
            "preprocessing.fillna", "preprocessing.outlier", "preprocessing.standardize",
            "preprocessing.binning", "preprocessing.unique", "preprocessing.derive");

    /* ============================== 图模型 ============================== */

    private static final class Node {
        final String id;
        final String componentCode;
        final String name;
        final Map<String, Object> params;

        Node(String id, String componentCode, String name, Map<String, Object> params) {
            this.id = id;
            this.componentCode = componentCode;
            this.name = name;
            this.params = params;
        }
    }

    private static final class Edge {
        final String source;
        final String target;

        Edge(String source, String target) {
            this.source = source;
            this.target = target;
        }
    }

    private static final class GraphModel {
        final List<Node> nodes = new ArrayList<>();
        final List<Edge> edges = new ArrayList<>();

        Node nodeById(String id) {
            for (Node node : nodes) {
                if (node.id.equals(id)) {
                    return node;
                }
            }
            return null;
        }

        boolean hasEdge(String source, String target) {
            for (Edge edge : edges) {
                if (edge.source.equals(source) && edge.target.equals(target)) {
                    return true;
                }
            }
            return false;
        }
    }
}
