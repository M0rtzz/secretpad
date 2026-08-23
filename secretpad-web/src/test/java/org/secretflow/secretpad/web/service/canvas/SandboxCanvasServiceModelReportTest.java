/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package org.secretflow.secretpad.web.service.canvas;

import org.secretflow.secretpad.web.service.DataSandboxMvpService;
import org.secretflow.secretpad.web.service.SandboxDataControlService;
import org.secretflow.secretpad.web.service.dev.DataDevService;
import org.secretflow.secretpad.web.service.dev.DevJobExecutor;
import org.secretflow.secretpad.web.service.model.ModelApprovalService;
import org.secretflow.secretpad.web.service.storage.SandboxDbService;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SandboxCanvasServiceModelReportTest {

    @Mock private JdbcTemplate jdbc;
    @Mock private DataDevService dataDevService;
    @Mock private DevJobExecutor devJobExecutor;
    @Mock private SandboxDbService sandboxDb;
    @Mock private DataSandboxMvpService mvp;
    @Mock private ModelApprovalService modelApprovalService;
    @Mock private SandboxDataControlService dataControl;

    private SandboxCanvasService service;

    @BeforeEach
    void setUp() {
        service = new SandboxCanvasService(jdbc, new ObjectMapper(), dataDevService, devJobExecutor,
                sandboxDb, mvp, modelApprovalService, dataControl);
        when(jdbc.queryForObject(anyString(), eq(Long.class), eq("project-1"), eq("kuscia-system")))
                .thenReturn(1L);
    }

    @Test
    void shouldBuildReportFromBoundRun() {
        String graph = """
                {"nodes":[
                  {"id":"data","data":{"componentCode":"data.table","name":"数据资源","params":{"table":"asset_people"}}},
                  {"id":"fill","data":{"componentCode":"preprocessing.fillna","name":"缺失值处理","params":{"columns":["age"],"method":"mean"}}},
                  {"id":"train","data":{"componentCode":"ml.linear_regression","name":"线性回归","params":{"features":["age","income"],"label":"y","fit_intercept":true}}}
                ],"edges":[{"source":"data","target":"fill"},{"source":"fill","target":"train"}]}
                """;
        stubCanvasModel(row(
                "id", "cm-1", "canvas_id", "canvas-1", "canvas_version", 3,
                "model_id", "model-1", "source_node_id", "train", "source_run_id", "run-1",
                "source_task_id", "task-train", "name", "回归模型", "status", "READY",
                "graph_json", graph, "created_by", "alice", "created_at", "2026-08-23T20:00:00"));
        when(jdbc.queryForList(
                "select id from ds_compute_run where id=? and canvas_id=? and deleted=0",
                "run-1", "canvas-1")).thenReturn(List.of(Map.of("id", "run-1")));
        Map<String, Object> trainRun = row(
                "id", "nr-train", "run_id", "run-1", "node_id", "train",
                "model_id", "model-1", "task_id", "task-train", "status", "SUCCEEDED",
                "input_table", "op_run_fill", "output_table", "op_run_train");
        when(jdbc.queryForList(
                "select * from ds_compute_node_run where run_id=? and model_id=? "
                        + "and status='SUCCEEDED' and deleted=0 order by finished_at desc limit 1",
                "run-1", "model-1")).thenReturn(List.of(trainRun));
        when(jdbc.queryForList(
                "select * from ds_compute_node_run where run_id=? and deleted=0 order by created_at asc",
                "run-1")).thenReturn(List.of(
                row("node_id", "fill", "status", "SUCCEEDED", "input_table", "asset_people",
                        "output_table", "op_run_fill", "fit_params", "{\"values\":{\"age\":42}}"),
                trainRun));
        when(sandboxDb.hasTable("sandbox-1", "asset_people")).thenReturn(true);
        when(sandboxDb.previewTable("sandbox-1", "asset_people", 1)).thenReturn(Map.of(
                "schema", List.of(column("age", "INTEGER"), column("income", "DOUBLE"),
                        column("id", "STRING"), column("y", "DOUBLE"))));
        when(sandboxDb.previewTable("sandbox-1", "op_run_fill", 1)).thenReturn(Map.of(
                "schema", List.of(column("age", "DOUBLE"), column("income", "DOUBLE"),
                        column("id", "STRING"), column("y", "DOUBLE"))));
        when(jdbc.queryForList(
                "select * from ds_model_test where model_id=? and status='SUCCEEDED' and deleted=0 "
                        + "order by finished_at desc,created_at desc limit 1",
                "model-1")).thenReturn(List.of(row(
                "id", "test-1", "run_mode", "DEV", "metric_type", "regression",
                "metrics", "{\"metricType\":\"regression\",\"mae\":1.2,\"rmse\":1.5,\"r2\":0.9}",
                "input_summary", "{\"rowCount\":20,\"columnCount\":4}",
                "output_summary", "{\"rowCount\":20,\"columnCount\":1}",
                "result_preview", "{\"header\":[\"pred\"],\"rows\":[[12.5]]}",
                "finished_at", "2026-08-23T20:01:00")));
        when(jdbc.queryForList(
                "select id,status,metric_type,run_mode,created_at,finished_at from ds_model_test "
                        + "where model_id=? and status='SUCCEEDED' and deleted=0 order by finished_at desc,created_at desc",
                "model-1")).thenReturn(List.of(Map.of("id", "test-1", "status", "SUCCEEDED")));

        Map<String, Object> report = service.modelReport("cm-1", "");

        assertThat(report).containsEntry("reportStatus", "AVAILABLE")
                .containsEntry("sourceRunId", "run-1")
                .containsEntry("runBinding", "EXACT");
        assertThat((List<?>) report.get("features")).hasSize(2);
        assertThat((List<?>) report.get("preprocessingSteps")).hasSize(1);
        assertThat((List<?>) report.get("excludedFields")).hasSize(1);
        assertThat(((Map<?, ?>) report.get("evaluation")).get("testId")).isEqualTo("test-1");
    }

    @Test
    void shouldReturnUnavailableReportForDraftModel() {
        stubCanvasModel(row(
                "id", "cm-draft", "canvas_id", "canvas-1", "canvas_version", 1,
                "model_id", "", "name", "草稿模型", "status", "DRAFT", "graph_json", "{}"));

        Map<String, Object> report = service.modelReport("cm-draft", "");

        assertThat(report).containsEntry("reportStatus", "UNAVAILABLE");
    }

    private void stubCanvasModel(Map<String, Object> model) {
        when(jdbc.queryForList(
                "select cm.*,m.status model_status,m.version model_version,m.artifact_id,m.artifact_version_id "
                        + "from ds_compute_canvas_model cm left join ds_model m on m.id=cm.model_id and m.deleted=0 "
                        + "where cm.id=? and cm.deleted=0",
                model.get("id"))).thenReturn(List.of(model));
        when(jdbc.queryForList("select * from ds_compute_canvas where id=? and deleted=0", "canvas-1"))
                .thenReturn(List.of(Map.of("id", "canvas-1", "sandbox_id", "sandbox-1")));
        when(jdbc.queryForList("select * from ds_sandbox where id=? and deleted=0", "sandbox-1"))
                .thenReturn(List.of(Map.of("id", "sandbox-1", "project_id", "project-1")));
    }

    private static Map<String, Object> column(String name, String type) {
        return Map.of("name", name, "type", type);
    }

    private static Map<String, Object> row(Object... values) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            row.put(String.valueOf(values[i]), values[i + 1]);
        }
        return row;
    }
}
