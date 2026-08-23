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

import org.secretflow.secretpad.web.service.sandbox.SandboxApprovalGate;
import org.secretflow.secretpad.web.service.storage.SandboxDbService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ModelApiServiceProviderApprovalTest {

    private static final String DATA_DIR_SQL =
            "select asset_id,name from ds_sandbox_data_dir where sandbox_id=? and table_name=? "
                    + "and kind='MOUNT' and deleted=0 limit 1";
    private static final String ASSET_SQL =
            "select id,name,provider_node_id from ds_data_asset where id=? and deleted=0";

    @Mock private JdbcTemplate jdbc;
    @Mock private SandboxApprovalGate gate;
    @Mock private SandboxDbService sandboxDb;

    private ModelApiService service;

    @BeforeEach
    void setUp() {
        service = new ModelApiService();
        ReflectionTestUtils.setField(service, "jdbc", jdbc);
        ReflectionTestUtils.setField(service, "gate", gate);
        ReflectionTestUtils.setField(service, "sandboxDb", sandboxDb);
    }

    @Test
    void shouldResolveRemoteProviderFromAuthoritativeSandboxMount() {
        stubCanvasContext();
        when(jdbc.queryForList(DATA_DIR_SQL, "sandbox-1", "asset_remote_table"))
                .thenReturn(List.of(Map.of(
                        "asset_id", "asset-remote", "name", "remote sample")));
        when(jdbc.queryForList(
                startsWith("select provider_node_id from ds_sandbox_dataset_mount"),
                eq("sandbox-1"), eq("asset-remote")))
                .thenReturn(List.of(Map.of("provider_node_id", "provider-b")));
        when(gate.matchesCurrentNode("provider-b")).thenReturn(false);
        when(sandboxDb.previewTable("sandbox-1", "asset_remote_table", 100))
                .thenReturn(Map.of("totalRows", 3));

        List<Map<String, Object>> providers = providerData("model-1");

        assertThat(providers).hasSize(1);
        assertThat(providers.get(0))
                .containsEntry("assetId", "asset-remote")
                .containsEntry("name", "remote sample")
                .containsEntry("providerNodeId", "provider-b")
                .containsEntry("preview", Map.of("totalRows", 3));
        verify(gate).matchesCurrentNode("provider-b");
    }

    @Test
    void shouldTraceLegacySyncedAssetWhenMountProviderIsMissing() {
        stubCanvasContext();
        when(jdbc.queryForList(DATA_DIR_SQL, "sandbox-1", "asset_remote_table"))
                .thenReturn(List.of(Map.of(
                        "asset_id", "asset-source", "name", "remote sample")));
        when(jdbc.queryForList(
                startsWith("select provider_node_id from ds_sandbox_dataset_mount"),
                eq("sandbox-1"), eq("asset-source")))
                .thenReturn(List.of());
        when(jdbc.queryForList(ASSET_SQL, "asset-source")).thenReturn(List.of());
        when(jdbc.queryForList(
                startsWith("select local_asset_id from ds_asset_sync_record"),
                eq("asset-source")))
                .thenReturn(List.of(Map.of("local_asset_id", "asset-local-copy")));
        when(jdbc.queryForList(ASSET_SQL, "asset-local-copy"))
                .thenReturn(List.of(Map.of(
                        "id", "asset-local-copy",
                        "name", "remote sample",
                        "provider_node_id", "provider-b")));

        List<Map<String, Object>> providers = providerData("model-1");

        assertThat(providers).hasSize(1);
        assertThat(providers.get(0)).containsEntry("providerNodeId", "provider-b");
    }

    @Test
    void legacyCreateEndpointShouldReusePublishApprovalGate() {
        ModelApiService spy = spy(new ModelApiService());
        doReturn(Map.of("status", "PENDING", "approvalRequired", true))
                .when(spy).publish(anyMap());

        Map<String, Object> result = spy.create(Map.of(
                "modelId", "model-1",
                "name", "credit-api",
                "authorizedUsers", List.of("alice")));

        assertThat(result).containsEntry("approvalRequired", true);
        verify(spy).publish(org.mockito.ArgumentMatchers.argThat(request ->
                "MODEL".equals(request.get("sourceType"))
                        && "model-1".equals(request.get("sourceId"))
                        && "credit-api".equals(request.get("apiName"))));
    }

    @Test
    void shouldRejectEnableAndSecretRotationWhileProviderApprovalIsPending() {
        when(jdbc.queryForList(
                "select * from ds_model_api where id=? and deleted=0", "api-1"))
                .thenReturn(List.of(Map.of(
                        "id", "api-1",
                        "status", "PENDING",
                        "approval_id", "approval-1")));

        assertThatThrownBy(() -> service.enable("api-1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(ModelErrors.MODEL_STATE_CONFLICT);
        assertThatThrownBy(() -> service.regenerateSecret("api-1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(ModelErrors.MODEL_STATE_CONFLICT);
    }

    private void stubCanvasContext() {
        when(jdbc.queryForList(startsWith("select cm.model_id"), eq("model-1")))
                .thenReturn(List.of(Map.ofEntries(
                        Map.entry("model_id", "model-1"),
                        Map.entry("model_name", "credit model"),
                        Map.entry("graph_json", "{}"),
                        Map.entry("input_table", "asset_remote_table"),
                        Map.entry("input_columns", "[]"),
                        Map.entry("status", "READY"),
                        Map.entry("sandbox_id", "sandbox-1"),
                        Map.entry("project_id", "project-1"))));
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> providerData(String modelId) {
        return (List<Map<String, Object>>) ReflectionTestUtils.invokeMethod(
                service, "resolveProviderData", modelId);
    }
}
