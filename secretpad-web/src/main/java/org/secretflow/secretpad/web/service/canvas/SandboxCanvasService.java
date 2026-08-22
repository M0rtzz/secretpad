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
import org.secretflow.secretpad.web.service.model.ModelApprovalService;
import org.secretflow.secretpad.web.service.storage.SandboxDbService;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
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
 *   <li>上游输出表 {@code op_{canvasId}_{nodeId}} 落沙箱库（{@link SandboxDbService#backfillOperatorTable}）；</li>
 *   <li>ml.* 训练节点成功 → joblib(base64) 自动注册 {@code ds_dev_artifact + ds_dev_artifact_version + ds_model(APPROVED)}；</li>
 *   <li>{@code ds_compute_run / ds_compute_node_run} 记录整图/节点状态，节点输出/日志对前端可见。</li>
 * </ol>
 *
 * <p>边界：result_* 表不可作画布输入；op_* 表仅画布内部消费（{@link DataDevService} 已拒绝作 dev 源）。</p>
 */
@Slf4j
@Service
public class SandboxCanvasService {

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

    /** 节点输出数据预览：非虚拟节点读 op_{canvasId}_{nodeId}，虚拟节点读挂载表。 */
    public Map<String, Object> nodeOutput(String canvasId, String nodeId, int limit) {
        Map<String, Object> canvas = requireCanvas(canvasId);
        String sandboxId = string(canvas.get("sandbox_id"));
        requireUsableSandbox(sandboxId, false);
        GraphModel graph = parseGraph(string(canvas.get("graph_json")));
        Node node = graph.nodeById(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("画布节点不存在: " + nodeId);
        }
        String table = CanvasOperatorRegistry.isVirtual(node.componentCode)
                ? string(node.params.get("table"))
                : opTableName(canvasId, nodeId);
        if (!sandboxDb.hasTable(sandboxId, table)) {
            Map<String, Object> empty = new LinkedHashMap<>();
            empty.put("tableName", table);
            empty.put("available", false);
            empty.put("message", "该节点尚无输出（未运行成功或尚未执行）");
            empty.put("schema", List.of());
            empty.put("rows", List.of());
            empty.put("totalRows", 0);
            return empty;
        }
        dataControl.requireMountTableUsable(sandboxId, table);
        Map<String, Object> preview = sandboxDb.previewTable(sandboxId, table, Math.max(1, Math.min(limit, 500)));
        preview.put("available", true);
        preview.put("nodeId", nodeId);
        return preview;
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

    /* ============================== 画布数据资源（节点配置用） ============================== */

    /** 画布可用数据资源：沙箱挂载表 + op_* 画布输出表（不含 result_*），供 data.table/compare_table/列选择。 */
    public Map<String, Object> dataResources(String sandboxId) {
        requireUsableSandbox(sandboxId, false);
        Map<String, Object> dir = dataControl.enrichDirectory(sandboxDb.directory(sandboxId));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) dir.get("items");
        List<Map<String, Object>> resources = new ArrayList<>();
        for (Map<String, Object> item : items) {
            String kind = string(item.get("kind"));
            if (!Set.of("MOUNT", "OPERATOR").contains(kind)) {
                continue;
            }
            resources.add(item);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("sandboxId", sandboxId);
        result.put("resources", resources);
        return result;
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
        String canvasName = notBlank(name) ? name : string(tpl.get("name"));
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

    /** 查询画布训练节点最近生成的可执行模型，供“保存为模型”时选择输出节点。 */
    public List<Map<String, Object>> modelCandidates(String canvasId) {
        Map<String, Object> canvas = requireCanvas(canvasId);
        requireUsableSandbox(string(canvas.get("sandbox_id")), false);
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select nr.model_id,nr.node_id,nr.component_code,nr.finished_at,m.name,m.status,m.version "
                        + "from ds_compute_node_run nr join ds_model m on m.id=nr.model_id and m.deleted=0 "
                        + "where nr.canvas_id=? and nr.deleted=0 and nr.status='SUCCEEDED' and nr.model_id<>'' "
                        + "order by nr.finished_at desc",
                canvasId);
        List<Map<String, Object>> result = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (Map<String, Object> row : rows) {
            if (seen.add(string(row.get("model_id")))) {
                result.add(row);
            }
        }
        return result;
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
        String sourceNodeId = "";
        String status = "DRAFT";
        if (notBlank(modelId)) {
            List<Map<String, Object>> candidates = jdbc.queryForList(
                    "select nr.node_id from ds_compute_node_run nr join ds_model m on m.id=nr.model_id and m.deleted=0 "
                            + "where nr.canvas_id=? and nr.model_id=? and nr.status='SUCCEEDED' and nr.deleted=0 "
                            + "order by nr.finished_at desc limit 1",
                    canvasId, modelId);
            if (candidates.isEmpty()) {
                throw new IllegalArgumentException("所选模型不是该画布成功训练产生的模型");
            }
            sourceNodeId = string(candidates.get(0).get("node_id"));
            status = "READY";
        }

        String name = string(request.get("name"));
        if (!notBlank(name)) {
            name = string(canvas.get("name")) + "-模型";
        }
        String id = "cm-" + shortId();
        String now = now();
        jdbc.update("insert into ds_compute_canvas_model(id,canvas_id,canvas_version,model_id,source_node_id,name,"
                        + "description,graph_json,status,created_by,created_at,updated_at,deleted) "
                        + "values(?,?,?,?,?,?,?,?,?,?,?,?,0)",
                id, canvasId, intValue(canvas.get("version"), 1), modelId, sourceNodeId, name,
                string(request.get("description")), graphJson, status, actor(), now, now);
        audit("CANVAS_MODEL_SAVED", "CANVAS_MODEL", id,
                "canvas=" + canvasId + " version=" + canvas.get("version") + " model=" + modelId, true);
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
            String inputTable = resolveInputTable(node, graph, canvasId, sandboxId);
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
            String outputTable = opTableName(canvasId, node.id);
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
            sandboxDb.backfillOperatorTable(sandboxId, canvasId, node.id, "画布输出-" + node.componentCode, header, rows);
            Map<String, Object> summary = new LinkedHashMap<>();
            summary.put("header", header);
            summary.put("rowCount", rows.size());
            summary.put("columnCount", header.size());
            jdbc.update("update ds_compute_node_run set status='SUCCEEDED',task_id=?,input_table=?,output_table=?,"
                            + "result_summary=?,model_b64=?,fit_params=?,finished_at=?,updated_at=? where id=?",
                    taskId, inputTable, outputTable, json(summary), modelB64, fitParams, now(), now(), nodeRunId);
            if (CanvasOperatorRegistry.isTrain(node.componentCode) && notBlank(modelB64)) {
                registerModelFromTrainNode(canvas, node, modelB64, sandboxId, graph, runId);
            }
            audit("CANVAS_NODE_SUCCEEDED", "COMPUTE_NODE_RUN", nodeRunId,
                    "node=" + node.id + " op=" + node.componentCode + " rows=" + rows.size()
                            + (notBlank(modelB64) ? " modelRegistered=true" : ""), true);
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

    /** 训练节点产物自动注册：ds_dev_artifact(PYTHON) + 版本(predict 脚本) + ds_model(APPROVED，幂等)。 */
    private void registerModelFromTrainNode(Map<String, Object> canvas, Node node, String modelB64, String sandboxId,
            GraphModel graph, String runId) {
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

    private String resolveInputTable(Node node, GraphModel graph, String canvasId, String sandboxId) {
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
            table = opTableName(canvasId, source.id);
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

    private static String opTableName(String canvasId, String nodeId) {
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
