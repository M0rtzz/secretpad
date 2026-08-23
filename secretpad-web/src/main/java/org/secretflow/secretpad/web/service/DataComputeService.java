/*
 * Copyright 2026 Ant Group Co., Ltd.
 * Licensed under the Apache License, Version 2.0.
 */
package org.secretflow.secretpad.web.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.secretflow.secretpad.common.dto.UserContextDTO;
import org.secretflow.secretpad.common.util.UserContext;
import org.secretflow.secretpad.web.service.canvas.CanvasOperatorRegistry;
import org.secretflow.secretpad.web.service.sandbox.SandboxApprovalService;
import org.secretflow.secretpad.web.service.storage.SandboxDbService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/** Sandbox-scoped facade for computation, mounting, custom components, canvases and reports. */
@Service
public class DataComputeService {
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final SandboxApprovalService approvals;
    private final DataAssetService assets;
    private final SandboxDbService sandboxDb;
    private final SandboxDataControlService dataControl;

    public DataComputeService(@Qualifier("jdbcTemplate") JdbcTemplate jdbc,
                              ObjectMapper mapper,
                              SandboxApprovalService approvals,
                              DataAssetService assets,
                              SandboxDbService sandboxDb,
                              SandboxDataControlService dataControl) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.approvals = approvals;
        this.assets = assets;
        this.sandboxDb = sandboxDb;
        this.dataControl = dataControl;
    }

    public List<Map<String, Object>> overview() {
        String node = nodeId();
        List<Map<String, Object>> projects = jdbc.queryForList(
                "select distinct p.project_id,p.name,p.compute_mode,p.development_modes,p.gmt_create "
                        + "from project p join project_node pn on pn.project_id=p.project_id and pn.is_deleted=0 "
                        + "where p.is_deleted=0 and pn.node_id=? order by p.gmt_modified desc", node);
        for (Map<String, Object> project : projects) {
            String projectId = string(project.get("project_id"));
            List<Map<String, Object>> sandboxes = jdbc.queryForList(
                    "select s.*,i.name image_name,(select count(1) from ds_sandbox_dataset_mount m where m.sandbox_id=s.id and m.deleted=0 and m.status='READY') mount_count,"
                            + "(select count(1) from ds_dev_task t where t.sandbox_id=s.id and t.deleted=0) task_count "
                            + "from ds_sandbox s left join ds_sandbox_image i on i.id=s.image_id "
                            + "where s.project_id=? and s.deleted=0 order by s.created_at desc", projectId);
            sandboxes.forEach(s -> {
                boolean creator = matchesNode(string(s.get("owner_id"))) && Objects.equals(actor(), string(s.get("created_by")));
                boolean expired = "EXPIRED".equals(string(s.get("status")));
                s.put("usable", creator && !Set.of("DESTROYED", "EXPIRED").contains(string(s.get("status"))));
                s.put("readOnlyReason", expired ? "沙箱已过期，请先续期" : creator ? "" : "沙箱仅创建人可使用");
            });
            project.put("sandboxes", sandboxes);
        }
        return projects;
    }

    public Map<String, Object> context(String sandboxId) {
        Map<String, Object> sandbox = requireSandbox(sandboxId);
        String projectId = string(sandbox.get("project_id"));
        requireProjectMember(projectId);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("sandbox", sandbox);
        result.put("project", row("select project_id,name,compute_mode,development_modes,gmt_create from project where project_id=? and is_deleted=0", projectId));
        result.put("mounts", assets.sandboxMounts(sandboxId));
        result.put("availableAssets", assets.projectAssets(projectId).stream()
                .filter(asset -> "ACTIVE".equals(string(asset.get("status"))))
                .filter(asset -> "PROCESSED".equals(string(asset.get("data_stage"))))
                .filter(asset -> string(asset.get("valid_until")).isBlank()
                        || string(asset.get("valid_until")).compareTo(now()) >= 0)
                .toList());
        result.put("canUse", !"EXPIRED".equals(string(sandbox.get("status")))
                && matchesNode(string(sandbox.get("owner_id")))
                && Objects.equals(actor(), string(sandbox.get("created_by"))));
        return result;
    }

    /** Returns the two data sets visible inside a sandbox workspace. */
    public Map<String, Object> workspaceData(String sandboxId) {
        Map<String, Object> context = context(sandboxId);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("mounts", context.get("mounts"));
        result.put("results", jdbc.queryForList(
                "select t.id task_id,t.name task_name,t.exec_type,t.result_rows,t.finished_at,"
                        + "t.result_asset_id,a.name asset_name,a.modality,a.data_stage,a.datatable_id,"
                        + "a.storage_uri,a.metadata_json,a.provider_node_id "
                        + "from ds_dev_task t left join ds_data_asset a on a.id=t.result_asset_id and a.deleted=0 "
                        + "where t.sandbox_id=? and t.status='SUCCEEDED' and t.deleted=0 "
                        + "order by t.finished_at desc", sandboxId));
        return result;
    }

    /* ------------------------------ 沙箱权威库数据目录（Stage 3） ------------------------------ */

    /** 沙箱数据目录（MOUNT + RESULT），仅创建人。 */
    public Map<String, Object> sandboxDbDirectory(String sandboxId) {
        requireUsableSandbox(sandboxId, true);
        return dataControl.enrichDirectory(sandboxDb.directory(sandboxId));
    }

    /** 沙箱表预览（schema + 前 limit 行），仅创建人。 */
    public Map<String, Object> sandboxDbTablePreview(String sandboxId, String tableName, int limit) {
        requireUsableSandbox(sandboxId, true);
        dataControl.requireTablePreview(sandboxId, tableName);
        return sandboxDb.previewTable(sandboxId, tableName, limit);
    }

    /** 沙箱开发结果 CSV 导出；挂载数据不可导出。 */
    public byte[] sandboxDbTableExport(String sandboxId, String tableName) {
        requireUsableSandbox(sandboxId, true);
        dataControl.requireResultExport(sandboxId, tableName);
        return sandboxDb.readTableCsv(sandboxId, tableName);
    }

    public List<Map<String, Object>> resultControls(String sandboxId) {
        requireUsableSandbox(sandboxId, true);
        return dataControl.resultControls(sandboxId);
    }

    public Map<String, Object> saveResultControl(Map<String, Object> request) {
        requireUsableSandbox(required(request, "sandboxId"), true);
        return dataControl.saveResultControl(request);
    }

    public Map<String, Object> requestMount(Map<String, Object> request) {
        String sandboxId = required(request, "sandboxId");
        Map<String, Object> sandbox = requireSandbox(sandboxId);
        requireProjectMember(string(sandbox.get("project_id")));
        Map<String, Object> approval = new LinkedHashMap<>(request);
        approval.put("approvalType", "DATA_CHANGE");
        approval.put("projectId", sandbox.get("project_id"));
        approval.put("sandboxId", sandboxId);
        approval.putIfAbsent("reason", "数据计算沙箱挂载数据");
        return approvals.submit(approval);
    }

    public List<Map<String, Object>> mountRequests(String status) {
        List<Map<String, Object>> rows = approvals.listApprovals(status, "DATA_CHANGE", "");
        rows.forEach(r -> r.put("payload", parse(string(r.get("payload_json")))));
        return rows;
    }

    public List<Map<String, Object>> components(String sandboxId) {
        Map<String, Object> sandbox = requireSandbox(sandboxId);
        requireProjectMember(string(sandbox.get("project_id")));
        List<Map<String, Object>> rows = new ArrayList<>(builtInComponents());
        rows.addAll(jdbc.queryForList(
                "select code,name,'自定义组件' category,runtime_type runtime_app,code runtime_code,'1.0.0' version,'' description,params_schema parameter_schema_json,'{}' default_params_json,'CUSTOM' source,id,model_id "
                        + "from ds_custom_component where deleted=0 and status='ENABLED' and project_id=? order by updated_at desc",
                sandbox.get("project_id")));
        return rows;
    }

    private List<Map<String, Object>> builtInComponents() {
        // 智能建模算子超市：元数据 + 参数 schema + 输入/输出 schema + 资源配额统一由 CanvasOperatorRegistry 维护
        return new ArrayList<>(CanvasOperatorRegistry.builtInComponents());
    }

    @Transactional
    public Map<String, Object> publishComponent(Map<String, Object> request) {
        String modelId = required(request, "modelId");
        Map<String, Object> model = row("select m.*,a.name artifact_name,a.type artifact_type,v.params_schema from ds_model m "
                + "join ds_dev_artifact a on a.id=m.artifact_id join ds_dev_artifact_version v on v.id=m.artifact_version_id "
                + "where m.id=? and m.deleted=0", modelId);
        if (!Set.of("APPROVED", "PUBLISHED").contains(string(model.get("status")))) {
            throw new IllegalStateException("自定义算法审批通过后才能发布为组件");
        }
        if (!Set.of("JAR", "PYTHON").contains(string(model.get("artifact_type")))) {
            throw new IllegalArgumentException("仅 JAR/Python 算法可发布为组件");
        }
        List<Map<String, Object>> existing = jdbc.queryForList("select * from ds_custom_component where model_id=? and deleted=0", modelId);
        if (!existing.isEmpty()) return existing.get(0);
        String id = "cc-" + shortId();
        String code = "custom." + modelId.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_.-]", "-");
        String now = now();
        jdbc.update("insert into ds_custom_component(id,model_id,project_id,sandbox_id,code,name,artifact_id,artifact_version_id,runtime_type,input_schema,output_schema,params_schema,status,created_by,created_at,updated_at,deleted) values(?,?,?,?,?,?,?,?,?,?,?,?, 'ENABLED',?,?,?,0)",
                id, modelId, model.get("project_id"), model.get("sandbox_id"), code,
                value(request, "name", string(model.get("name"))), model.get("artifact_id"), model.get("artifact_version_id"),
                model.get("artifact_type"), string(model.get("input_schema")), string(model.get("output_schema")),
                string(model.get("params_schema")), actor(), now, now);
        return row("select * from ds_custom_component where id=?", id);
    }

    public List<Map<String, Object>> canvases(String sandboxId) {
        requireUsableSandbox(sandboxId, false);
        return jdbc.queryForList("select * from ds_compute_canvas where sandbox_id=? and deleted=0 order by updated_at desc", sandboxId);
    }

    @Transactional
    public Map<String, Object> saveCanvas(Map<String, Object> request) {
        String sandboxId = required(request, "sandboxId");
        Map<String, Object> sandbox = requireUsableSandbox(sandboxId, true);
        String graph = json(request.getOrDefault("graph", Map.of("nodes", List.of(), "edges", List.of())));
        String id = string(request.get("id"));
        String name = required(request, "name").trim();
        String now = now();
        // 画布拖拽/连线等自动保存时前端传 snapshot=false，避免版本快照爆炸；显式「保存」才生成版本记录
        boolean snapshot = !"false".equals(String.valueOf(request.get("snapshot")));
        if (id.isBlank()) {
            requireUniqueCanvasName(sandboxId, name, "");
            id = "canvas-" + shortId();
            jdbc.update("insert into ds_compute_canvas(id,project_id,sandbox_id,name,description,graph_json,version,status,created_by,created_at,updated_at,deleted) values(?,?,?,?,?,?,1,'DRAFT',?,?,?,0)",
                    id, sandbox.get("project_id"), sandboxId, name, string(request.get("description")), graph, actor(), now, now);
            if (snapshot) snapshotCanvasVersion(id, 1, name, graph, actor(), now);
        } else {
            Map<String, Object> old = row("select * from ds_compute_canvas where id=? and deleted=0", id);
            if (!Objects.equals(sandboxId, string(old.get("sandbox_id")))) {
                throw new IllegalArgumentException("画布不属于当前沙箱");
            }
            if (!Objects.equals(actor(), string(old.get("created_by")))) throw new SecurityException("仅画布创建人可编辑");
            requireUniqueCanvasName(sandboxId, name, id);
            int newVersion = intValue(old.get("version"), 0) + 1;
            jdbc.update("update ds_compute_canvas set name=?,description=?,graph_json=?,version=?,updated_at=? where id=? and sandbox_id=? and deleted=0",
                    name, string(request.get("description")), graph, newVersion, now, id, sandboxId);
            if (snapshot) snapshotCanvasVersion(id, newVersion, name, graph, actor(), now);
        }
        return row("select * from ds_compute_canvas where id=? and sandbox_id=?", id, sandboxId);
    }

    @Transactional
    public Map<String, Object> deleteCanvas(Map<String, Object> request) {
        String id = required(request, "id");
        String sandboxId = required(request, "sandboxId");
        requireUsableSandbox(sandboxId, true);
        Map<String, Object> canvas = row(
                "select * from ds_compute_canvas where id=? and sandbox_id=? and deleted=0", id, sandboxId);
        if (!Objects.equals(actor(), string(canvas.get("created_by")))) {
            throw new SecurityException("仅画布创建人可删除");
        }
        String now = now();
        jdbc.update("update ds_compute_canvas set deleted=1,updated_at=? where id=? and sandbox_id=? and deleted=0",
                now, id, sandboxId);
        jdbc.update("update ds_compute_canvas_version set deleted=1 where canvas_id=? and deleted=0", id);
        canvas.put("deleted", 1);
        canvas.put("updated_at", now);
        return canvas;
    }

    public List<Map<String, Object>> reports(String sandboxId, String type) {
        requireUsableSandbox(sandboxId, false);
        List<Map<String, Object>> result = new ArrayList<>(jdbc.queryForList(
                "select * from ds_compute_report where sandbox_id=? and deleted=0 order by created_at desc", sandboxId));
        for (Map<String, Object> task : jdbc.queryForList("select t.*,nr.run_id canvas_run_id,nr.canvas_id compute_canvas_id,"
                        + "nr.node_id component_id,nr.component_code,nr.result_summary node_result_summary "
                        + "from ds_dev_task t left join ds_compute_node_run nr on nr.task_id=t.id and nr.deleted=0 "
                        + "where t.sandbox_id=? and t.status='SUCCEEDED' and t.deleted=0 order by t.finished_at desc",
                sandboxId)) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("execType", task.get("exec_type"));
            payload.put("runMode", task.get("run_mode"));
            payload.put("sourceRows", task.get("source_rows"));
            payload.put("resultRows", task.get("result_rows"));
            payload.put("resultNodeId", task.get("result_node_id"));
            payload.put("resultDatatableId", task.get("result_datatable_id"));
            payload.put("channel", task.get("channel"));
            payload.put("componentCode", task.get("component_code"));
            payload.put("preview", taskPreview(task));
            String canvasId = string(task.get("compute_canvas_id"));
            String runId = value(task, "canvas_run_id", string(task.get("id")));
            result.add(reportRow("task-report-" + task.get("id"), task.get("project_id"), sandboxId,
                    canvasId, runId, string(task.get("component_id")), "PROGRAM_RESULT",
                    string(task.get("name")) + " - 运行结果",
                    json(payload), json(List.of(task.get("source_asset_id"), task.get("source_mount_id"))),
                    string(task.get("artifact_id")) + ":" + string(task.get("version")), task.get("created_by"), task.get("finished_at")));
        }
        for (Map<String, Object> test : jdbc.queryForList(
                "select t.*,m.project_id,m.sandbox_id,m.name model_name,m.version model_version from ds_model_test t "
                        + "join ds_model m on m.id=t.model_id and m.deleted=0 where m.sandbox_id=? and t.status='SUCCEEDED' and t.deleted=0 order by t.finished_at desc",
                sandboxId)) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("metrics", parse(string(test.get("metrics"))));
            payload.put("inputSummary", parse(string(test.get("input_summary"))));
            payload.put("outputSummary", parse(string(test.get("output_summary"))));
            payload.put("resultPreview", parse(string(test.get("result_preview"))));
            result.add(reportRow("model-report-" + test.get("id"), test.get("project_id"), sandboxId,
                    "", test.get("id"), string(test.get("model_id")), "MODEL_EVALUATION",
                    string(test.get("model_name")) + " - 模型评估",
                    json(payload), "[]", "v" + test.get("model_version"), test.get("created_by"), test.get("finished_at")));
        }
        if (!type.isBlank()) result.removeIf(row -> !type.equals(string(row.get("report_type"))));
        result.sort((left, right) -> string(right.get("created_at")).compareTo(string(left.get("created_at"))));
        return result;
    }

    private Map<String, Object> taskPreview(Map<String, Object> task) {
        Object value = parse(string(task.get("result_preview")));
        if (!(value instanceof Map<?, ?> raw)) return Map.of();
        Map<String, Object> preview = new LinkedHashMap<>();
        raw.forEach((key, item) -> preview.put(String.valueOf(key), item));
        if ("canvas".equals(string(task.get("channel"))) && preview.get("rows") instanceof List<?> rawRows) {
            List<Object> rows = new ArrayList<>(rawRows);
            while (!rows.isEmpty() && isCanvasMarker(rows.get(rows.size() - 1))) {
                rows.remove(rows.size() - 1);
            }
            preview.put("rows", rows);
            Map<String, Object> summary = parseMap(string(task.get("node_result_summary")));
            preview.put("resultRows", summary.getOrDefault("rowCount", rows.size()));
        }
        return preview;
    }

    private boolean isCanvasMarker(Object row) {
        return row instanceof List<?> cells && !cells.isEmpty()
                && Set.of("MODELB64:", "PREPROC:").contains(string(cells.get(0)));
    }

    private Map<String, Object> reportRow(String id, Object projectId, String sandboxId,
                                           Object canvasId, Object runId, Object componentId,
                                           String type, String name, String payload, String inputs,
                                           String algorithmVersion, Object createdBy, Object createdAt) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", id);
        row.put("project_id", projectId);
        row.put("sandbox_id", sandboxId);
        row.put("canvas_id", canvasId);
        row.put("run_id", runId);
        row.put("component_id", componentId);
        row.put("report_type", type);
        row.put("name", name);
        row.put("payload_json", payload);
        row.put("input_versions_json", inputs);
        row.put("algorithm_version", algorithmVersion);
        row.put("created_by", createdBy);
        row.put("created_at", createdAt);
        return row;
    }

    private Map<String, Object> requireUsableSandbox(String id, boolean creatorRequired) {
        Map<String, Object> sandbox = requireSandbox(id);
        requireProjectMember(string(sandbox.get("project_id")));
        if (creatorRequired && (!matchesNode(string(sandbox.get("owner_id"))) || !Objects.equals(actor(), string(sandbox.get("created_by"))))) {
            throw new SecurityException("沙箱仅创建人可使用");
        }
        if (Set.of("DESTROYED", "EXPIRED").contains(string(sandbox.get("status")))) throw new IllegalStateException("沙箱已失效");
        return sandbox;
    }

    private void requireUniqueCanvasName(String sandboxId, String name, String excludedId) {
        String sql = "select count(1) from ds_compute_canvas where sandbox_id=? and deleted=0 "
                + "and lower(name)=lower(?)";
        List<Object> args = new ArrayList<>(List.of(sandboxId, name));
        if (!excludedId.isBlank()) {
            sql += " and id<>?";
            args.add(excludedId);
        }
        Long count = jdbc.queryForObject(sql, Long.class, args.toArray());
        if (count != null && count > 0) {
            throw new IllegalArgumentException("同一沙箱内画布名称不能重复: " + name);
        }
    }

    private Map<String, Object> requireSandbox(String id) { return row("select * from ds_sandbox where id=? and deleted=0", id); }
    private void requireProjectMember(String projectId) {
        Long n = jdbc.queryForObject("select count(1) from project_node where project_id=? and node_id=? and is_deleted=0", Long.class, projectId, nodeId());
        if (n == null || n == 0) throw new SecurityException("当前节点不是项目参与方");
    }
    private Map<String, Object> row(String sql, Object... args) {
        List<Map<String, Object>> rows = jdbc.queryForList(sql, args);
        if (rows.isEmpty()) throw new IllegalArgumentException("记录不存在");
        return new LinkedHashMap<>(rows.get(0));
    }
    private String nodeId() {
        UserContextDTO user = UserContext.getUserOrNotExist();
        if (user == null) return "kuscia-system";
        return user.getPlatformNodeId() != null && !user.getPlatformNodeId().isBlank() ? user.getPlatformNodeId() : string(user.getOwnerId());
    }
    private boolean matchesNode(String candidate) {
        UserContextDTO user = UserContext.getUserOrNotExist();
        return Objects.equals(nodeId(), candidate) || (user != null && Objects.equals(string(user.getOwnerId()), candidate));
    }
    private String actor() { UserContextDTO u=UserContext.getUserOrNotExist(); return u==null||u.getName()==null?"system":u.getName(); }
    private String required(Map<String, Object> m,String key){String v=string(m.get(key));if(v.isBlank())throw new IllegalArgumentException(key+" 不能为空");return v;}
    private String value(Map<String, Object> m,String key,String fallback){String v=string(m.get(key));return v.isBlank()?fallback:v;}
    private String string(Object v){return v==null?"":String.valueOf(v);}
    private String now(){return LocalDateTime.now().toString();}
    private String shortId(){return UUID.randomUUID().toString().replace("-","").substring(0,12);}
    private String json(Object value){try{return mapper.writeValueAsString(value);}catch(Exception e){throw new IllegalArgumentException("JSON 格式错误",e);}}
    @SuppressWarnings("unchecked") private Object parse(String value){try{return mapper.readValue(value,Map.class);}catch(Exception e){return Map.of();}}
    private Map<String, Object> parseMap(String value){
        Object parsed = parse(value);
        if (!(parsed instanceof Map<?, ?> raw)) return Map.of();
        Map<String, Object> result = new LinkedHashMap<>();
        raw.forEach((key, item) -> result.put(String.valueOf(key), item));
        return result;
    }
    private static int intValue(Object v, int d){if(v instanceof Number n){return n.intValue();}if(v==null||String.valueOf(v).isBlank()){return d;}try{return Integer.parseInt(String.valueOf(v).trim());}catch(NumberFormatException e){return d;}}

    /** 画布保存版本快照：ds_compute_canvas_version（供回滚/对比）。 */
    private void snapshotCanvasVersion(String canvasId, int version, String name, String graph, String createdBy, String now) {
        jdbc.update("insert into ds_compute_canvas_version(id,canvas_id,version,name,graph_json,created_by,created_at,deleted) values(?,?,?,?,?,?,?,0)",
                "cv-" + shortId(), canvasId, version, name, graph, createdBy, now);
    }
}
