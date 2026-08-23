/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package org.secretflow.secretpad.web.service;

import org.secretflow.secretpad.web.service.sandbox.SandboxApprovalService;
import org.secretflow.secretpad.web.service.storage.SandboxDbService;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DataComputeServiceReportTest {

    @Mock private JdbcTemplate jdbc;
    @Mock private SandboxApprovalService approvals;
    @Mock private DataAssetService assets;
    @Mock private SandboxDbService sandboxDb;
    @Mock private SandboxDataControlService dataControl;

    private final ObjectMapper mapper = new ObjectMapper();
    private DataComputeService service;

    @BeforeEach
    void setUp() {
        service = new DataComputeService(jdbc, mapper, approvals, assets, sandboxDb, dataControl);
        when(jdbc.queryForList("select * from ds_sandbox where id=? and deleted=0", "sandbox-1"))
                .thenReturn(List.of(Map.of("id", "sandbox-1", "project_id", "project-1")));
        when(jdbc.queryForObject(anyString(), eq(Long.class), eq("project-1"), eq("kuscia-system")))
                .thenReturn(1L);
        when(jdbc.queryForList(
                "select * from ds_compute_report where sandbox_id=? and deleted=0 order by created_at desc",
                "sandbox-1")).thenReturn(List.of());
        when(jdbc.queryForList(startsWith("select t.*,nr.run_id canvas_run_id"), eq("sandbox-1")))
                .thenReturn(List.of());
    }

    @Test
    void shouldExposeRealModelResultRowsInReportPayload() throws Exception {
        when(jdbc.queryForList(startsWith("select t.*,m.project_id"), eq("sandbox-1")))
                .thenReturn(List.of(Map.ofEntries(
                        Map.entry("id", "test-1"),
                        Map.entry("model_id", "model-1"),
                        Map.entry("project_id", "project-1"),
                        Map.entry("sandbox_id", "sandbox-1"),
                        Map.entry("model_name", "credit-model"),
                        Map.entry("model_version", 3),
                        Map.entry("metrics", "{\"accuracy\":0.9}"),
                        Map.entry("input_summary", "{}"),
                        Map.entry("output_summary", "{}"),
                        Map.entry("result_preview", "{\"header\":[\"id\",\"score\"],\"rows\":[[1,93],[2,85]],\"resultRows\":2}"),
                        Map.entry("created_by", "alice"),
                        Map.entry("finished_at", "2026-08-23 10:00:00"))));

        List<Map<String, Object>> reports = service.reports("sandbox-1", "MODEL_EVALUATION");

        assertThat(reports).hasSize(1);
        assertThat(reports.get(0))
                .containsEntry("run_id", "test-1")
                .containsEntry("component_id", "model-1")
                .containsEntry("report_type", "MODEL_EVALUATION");
        Map<String, Object> payload = mapper.readValue(
                String.valueOf(reports.get(0).get("payload_json")), new TypeReference<>() {});
        assertThat(payload.get("resultPreview")).isEqualTo(Map.of(
                "header", List.of("id", "score"),
                "rows", List.of(List.of(1, 93), List.of(2, 85)),
                "resultRows", 2));
    }
}
