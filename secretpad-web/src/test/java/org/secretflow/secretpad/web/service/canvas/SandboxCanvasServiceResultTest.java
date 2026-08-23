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
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SandboxCanvasServiceResultTest {

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
    void shouldReturnOwnTaskPreviewForHistoricalLegacyRun() {
        stubCommonRows(Map.of(
                "id", "nr-old",
                "run_id", "run-old",
                "component_code", "ml.predict",
                "output_table", "op_canvas_1_node_1",
                "task_id", "task-old",
                "result_summary", "{\"rowCount\":2}"));
        when(jdbc.queryForList(
                "select id from ds_compute_node_run where canvas_id=? and node_id=? "
                        + "and status='SUCCEEDED' and deleted=0 order by finished_at desc,created_at desc limit 1",
                "canvas-1", "node-1")).thenReturn(List.of(Map.of("id", "nr-new")));
        when(jdbc.queryForList(
                "select result_preview,result_rows from ds_dev_task where id=? and status='SUCCEEDED' and deleted=0",
                "task-old")).thenReturn(List.of(Map.of(
                        "result_preview", "{\"header\":[\"id\",\"score\"],\"rows\":[[1,93],[2,85],[\"MODELB64:\",\"secret\"]]}",
                        "result_rows", 3)));

        Map<String, Object> result = service.nodeOutput("canvas-1", "node-1", "run-old", 50);

        assertThat(result)
                .containsEntry("available", true)
                .containsEntry("runId", "run-old")
                .containsEntry("snapshotSource", "TASK_PREVIEW")
                .containsEntry("previewOnly", true)
                .containsEntry("totalRows", 2);
        assertThat(result.get("rows"))
                .isEqualTo(List.of(List.of(1, 93), List.of(2, 85)));
        verify(sandboxDb, never()).previewTable(anyString(), anyString(), ArgumentMatchers.anyInt());
    }

    @Test
    void shouldNotSubstituteLatestTableWhenHistoricalPreviewIsMissing() {
        stubCommonRows(Map.of(
                "id", "nr-old",
                "run_id", "run-old",
                "component_code", "stats.describe",
                "output_table", "op_canvas_1_node_1",
                "task_id", "task-old"));
        when(jdbc.queryForList(
                "select id from ds_compute_node_run where canvas_id=? and node_id=? "
                        + "and status='SUCCEEDED' and deleted=0 order by finished_at desc,created_at desc limit 1",
                "canvas-1", "node-1")).thenReturn(List.of(Map.of("id", "nr-new")));
        when(jdbc.queryForList(
                "select result_preview,result_rows from ds_dev_task where id=? and status='SUCCEEDED' and deleted=0",
                "task-old")).thenReturn(List.of(Map.of("result_preview", "", "result_rows", 0)));

        Map<String, Object> result = service.nodeOutput("canvas-1", "node-1", "run-old", 50);

        assertThat(result)
                .containsEntry("available", false)
                .containsEntry("runId", "run-old");
        assertThat(String.valueOf(result.get("message"))).contains("没有可恢复的任务预览");
        verify(sandboxDb, never()).previewTable(anyString(), anyString(), ArgumentMatchers.anyInt());
    }

    private void stubCommonRows(Map<String, Object> nodeRun) {
        when(jdbc.queryForList("select * from ds_compute_canvas where id=? and deleted=0", "canvas-1"))
                .thenReturn(List.of(Map.of("id", "canvas-1", "sandbox_id", "sandbox-1")));
        when(jdbc.queryForList("select * from ds_sandbox where id=? and deleted=0", "sandbox-1"))
                .thenReturn(List.of(Map.of("id", "sandbox-1", "project_id", "project-1")));
        when(jdbc.queryForList("select * from ds_compute_run where id=? and deleted=0", "run-old"))
                .thenReturn(List.of(Map.of("id", "run-old", "canvas_id", "canvas-1")));
        when(jdbc.queryForList(
                "select * from ds_compute_node_run where run_id=? and node_id=? and deleted=0 limit 1",
                "run-old", "node-1")).thenReturn(List.of(nodeRun));
    }
}
