/*
 * Copyright 2026 Ant Group Co., Ltd.
 * Licensed under the Apache License, Version 2.0.
 */
package org.secretflow.secretpad.web.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.secretflow.secretpad.common.dto.UserContextDTO;
import org.secretflow.secretpad.common.util.UserContext;
import org.secretflow.secretpad.web.service.sandbox.SandboxApprovalService;
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

    public DataComputeService(@Qualifier("jdbcTemplate") JdbcTemplate jdbc,
                              ObjectMapper mapper,
                              SandboxApprovalService approvals,
                              DataAssetService assets) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.approvals = approvals;
        this.assets = assets;
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
                s.put("usable", creator && !Set.of("DESTROYED", "EXPIRED").contains(string(s.get("status"))));
                s.put("readOnlyReason", creator ? "" : "沙箱仅创建人可使用");
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
        result.put("mounts", jdbc.queryForList(
                "select m.*,a.name asset_name,a.data_stage,a.modality,a.datatable_id,a.processor_node_id,a.metadata_json,a.valid_until,n.name provider_node_name "
                        + "from ds_sandbox_dataset_mount m join ds_data_asset a on a.id=m.asset_id "
                        + "left join node n on (n.node_id=a.provider_node_id or n.inst_id=a.provider_node_id) and n.is_deleted=0 "
                        + "where m.sandbox_id=? and m.deleted=0 order by m.created_at", sandboxId));
        result.put("availableAssets", assets.projectAssets(projectId).stream()
                .filter(asset -> "ACTIVE".equals(string(asset.get("status"))))
                .filter(asset -> "PROCESSED".equals(string(asset.get("data_stage"))))
                .filter(asset -> string(asset.get("valid_until")).isBlank()
                        || string(asset.get("valid_until")).compareTo(now()) >= 0)
                .toList());
        result.put("canUse", matchesNode(string(sandbox.get("owner_id"))) && Objects.equals(actor(), string(sandbox.get("created_by"))));
        return result;
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
        return List.of(
                component("data.table", "数据资源", "数据输入", "读取当前沙箱已挂载的数据"),
                component("preprocessing.psi", "数据对齐", "数据处理", "隐私集合求交与样本对齐"),
                component("preprocessing.feature_align", "特征对齐", "数据处理", "参与方字段与特征语义对齐"),
                component("preprocessing.outlier", "异常值处理", "数据处理", "检测并处理异常值"),
                component("preprocessing.fillna", "缺失值处理", "数据处理", "缺失值填充或删除"),
                component("preprocessing.unique", "唯一值筛选", "数据处理", "筛除常量和低信息量特征"),
                component("preprocessing.binning", "特征分箱", "特征工程", "等频、等距及自定义分箱"),
                component("preprocessing.woe", "WOE 转换", "特征工程", "分箱后的证据权重转换"),
                component("preprocessing.standardize", "标准化", "特征工程", "数值特征归一化和标准化"),
                component("stats.correlation", "相关系数", "统计分析", "计算特征相关性矩阵"),
                component("ml.knn", "KNN", "机器学习", "K 近邻分类或回归"),
                component("ml.kmeans", "KMeans", "机器学习", "无监督聚类"),
                component("ml.dnn", "DNN", "深度学习", "深度神经网络训练与预测"),
                component("ml.logistic_regression", "逻辑回归", "机器学习", "二分类逻辑回归"),
                component("ml.linear_regression", "线性回归", "机器学习", "线性回归训练与预测"),
                component("ml.binary_classification", "二分类评估", "模型评估", "准确率、精确率、召回率和 AUC 评估"));
    }

    private Map<String, Object> component(String code, String name, String category, String description) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("code", code);
        item.put("name", name);
        item.put("category", category);
        item.put("runtime_app", "SecretFlow");
        item.put("runtime_code", code);
        item.put("version", "1.0.0");
        item.put("description", description);
        item.put("parameter_schema_json", "[]");
        item.put("default_params_json", "{}");
        item.put("source", "BUILT_IN");
        return item;
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
        String now = now();
        if (id.isBlank()) {
            id = "canvas-" + shortId();
            jdbc.update("insert into ds_compute_canvas(id,project_id,sandbox_id,name,description,graph_json,version,status,created_by,created_at,updated_at,deleted) values(?,?,?,?,?,?,1,'DRAFT',?,?,?,0)",
                    id, sandbox.get("project_id"), sandboxId, required(request, "name"), string(request.get("description")), graph, actor(), now, now);
        } else {
            Map<String, Object> old = row("select * from ds_compute_canvas where id=? and deleted=0", id);
            if (!Objects.equals(actor(), string(old.get("created_by")))) throw new SecurityException("仅画布创建人可编辑");
            jdbc.update("update ds_compute_canvas set name=?,description=?,graph_json=?,version=version+1,updated_at=? where id=? and deleted=0",
                    required(request, "name"), string(request.get("description")), graph, now, id);
        }
        return row("select * from ds_compute_canvas where id=?", id);
    }

    public List<Map<String, Object>> reports(String sandboxId, String type) {
        requireUsableSandbox(sandboxId, false);
        List<Map<String, Object>> result = new ArrayList<>(jdbc.queryForList(
                "select * from ds_compute_report where sandbox_id=? and deleted=0 order by created_at desc", sandboxId));
        for (Map<String, Object> task : jdbc.queryForList(
                "select * from ds_dev_task where sandbox_id=? and status='SUCCEEDED' and deleted=0 order by finished_at desc", sandboxId)) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("execType", task.get("exec_type"));
            payload.put("runMode", task.get("run_mode"));
            payload.put("sourceRows", task.get("source_rows"));
            payload.put("resultRows", task.get("result_rows"));
            payload.put("resultNodeId", task.get("result_node_id"));
            payload.put("resultDatatableId", task.get("result_datatable_id"));
            payload.put("preview", parse(string(task.get("result_preview"))));
            result.add(reportRow("task-report-" + task.get("id"), task.get("project_id"), sandboxId,
                    task.get("id"), "PROGRAM_RESULT", string(task.get("name")) + " - 运行结果",
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
                    test.get("id"), "MODEL_EVALUATION", string(test.get("model_name")) + " - 模型评估",
                    json(payload), "[]", "v" + test.get("model_version"), test.get("created_by"), test.get("finished_at")));
        }
        if (!type.isBlank()) result.removeIf(row -> !type.equals(string(row.get("report_type"))));
        result.sort((left, right) -> string(right.get("created_at")).compareTo(string(left.get("created_at"))));
        return result;
    }

    private Map<String, Object> reportRow(String id, Object projectId, String sandboxId, Object runId,
                                           String type, String name, String payload, String inputs,
                                           String algorithmVersion, Object createdBy, Object createdAt) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", id);
        row.put("project_id", projectId);
        row.put("sandbox_id", sandboxId);
        row.put("canvas_id", "");
        row.put("run_id", runId);
        row.put("component_id", "");
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
}
