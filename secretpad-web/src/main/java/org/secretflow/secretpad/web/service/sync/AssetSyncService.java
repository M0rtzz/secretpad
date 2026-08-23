/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.service.sync;

import org.secretflow.secretpad.web.service.MinioAssetStorage;
import org.secretflow.secretpad.web.service.governance.CsvUtil;
import org.secretflow.secretpad.web.service.storage.NodeDatasetStore;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.UUID;

/**
 * 跨节点资产同步（Stage 2）：授权时自动物理拉取（决策 #4）。
 *
 * <p>黄金法则：数据物理归属本地节点。跨节点 PROCESSED 资产授权到项目后，请求方节点经既有
 * P2P 内部通道（本机 gateway + {@code Host: secretpad.{provider}.svc} +
 * {@code kuscia-origin-source}，同 {@code DbSyncUtil}/P2P 数据同步）拉取真实行并本地物化；
 * 跨节点 RAW 源数据绝不跨网传真实行，仅记录 SCHEMA 同步。每次同步写入
 * {@code ds_asset_sync_record} 幂等去重。</p>
 */
@Service
public class AssetSyncService {

    private static final Logger log = LoggerFactory.getLogger(AssetSyncService.class);
    private static final String DOWNLOAD_PATH = "/api/v1alpha1/data-assets/sync/download";

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final MinioAssetStorage storage;
    private final NodeDatasetStore nodeDatasetStore;
    private final String gateway;
    private final String localNodeId;

    public AssetSyncService(@Qualifier("jdbcTemplate") JdbcTemplate jdbc, ObjectMapper objectMapper,
            MinioAssetStorage storage, NodeDatasetStore nodeDatasetStore,
            @Value("${secretpad.gateway:127.0.0.1:80}") String gateway,
            @Value("${secretpad.node-id:kuscia-system}") String localNodeId) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.storage = storage;
        this.nodeDatasetStore = nodeDatasetStore;
        this.gateway = gateway.contains(":") ? gateway : gateway + ":80";
        this.localNodeId = localNodeId;
    }

    /** 一次下载（provider 侧返回；bytes 为 CSV 原文，sha256 供请求方端到端校验）。 */
    public record AssetDownload(byte[] bytes, String sha256) {
    }

    /* ------------------------------ provider 侧 ------------------------------ */

    /**
     * 提供方下载端点：仅 PROCESSED 表格数据可下载；请求方必须是持有该资产项目映射的参与节点。
     * 优先从本地权威库导出（含已同步再分发的场景），否则回退 MinIO 原件。
     */
    public AssetDownload download(String assetId, String requesterNodeId) {
        Map<String, Object> asset = localAsset(assetId);
        if (asset == null) {
            throw new NoSuchElementException("数据不存在: " + assetId);
        }
        if (!"PROCESSED".equals(string(asset.get("data_stage")))) {
            throw new SecurityException("仅抽样脱敏后的数据可以跨节点同步");
        }
        if (!"TABULAR".equals(string(asset.get("modality")))) {
            throw new SecurityException("仅表格数据可以跨节点同步");
        }
        authorizeRequester(assetId, requesterNodeId);
        nodeDatasetStore.ensureMaterialized(assetId);
        byte[] bytes = nodeDatasetStore.exportTableCsv(assetId);
        if (bytes == null) {
            try (InputStream in = storage.open(string(asset.get("storage_uri")))) {
                bytes = in.readAllBytes();
            } catch (IOException e) {
                throw new IllegalStateException("读取数据失败: " + assetId, e);
            }
        }
        // 同步契约：校验和必须对应当前实际下发的字节。
        // 资产物化为 SQLite 表后经 CsvUtil 重序列化的 CSV 与原始上传文件字节并不逐字节一致，
        // 若沿用原始文件的 metadata sha256 会导致请求方下载后校验失败（"校验和不一致"）。
        return new AssetDownload(bytes, sha256(bytes));
    }

    private void authorizeRequester(String assetId, String requesterNodeId) {
        if (requesterNodeId == null || requesterNodeId.isBlank()) {
            throw new SecurityException("缺少请求方节点标识");
        }
        Long count = jdbc.queryForObject(
                "select count(1) from ds_project_asset pa "
                        + "join project_node pn on pn.project_id=pa.project_id and pn.node_id=? and pn.is_deleted=0 "
                        + "where pa.asset_id=? and pa.deleted=0 and coalesce(pa.is_deleted,0)=0",
                Long.class, requesterNodeId, assetId);
        if (count == null || count == 0) {
            throw new SecurityException("请求方节点未获授权访问该资产");
        }
    }

    /* ------------------------------ requester 侧 ------------------------------ */

    /**
     * 自动同步入口（授权/挂载/读取路径共用，幂等）：跨节点 PROCESSED → 物理拉取；
     * 跨节点 RAW → 仅 SCHEMA；本节点资产 → 确保本地物化。统一返回 {@code syncMode}：
     * {@code LOCAL | PHYSICAL | SCHEMA}。
     */
    public Map<String, Object> ensureSynced(String projectId, String assetId) {
        Map<String, Object> meta = projectAssetMeta(projectId, assetId);
        String provider = string(meta.get("provider_node_id"));
        String stage = string(meta.get("data_stage"));
        if (Objects.equals(provider, localNodeId)) {
            Map<String, Object> local = localAsset(assetId);
            if (local != null) {
                nodeDatasetStore.ensureMaterialized(assetId);
            }
            return Map.of("syncMode", "LOCAL");
        }
        if ("PROCESSED".equals(stage)) {
            Map<String, Object> result = pullOnAuthorization(projectId, assetId);
            if (result == null) {
                return Map.of("syncMode", "SCHEMA");
            }
            if (result.containsKey("syncMode")) {
                return result; // SCHEMA 分支已带 syncMode
            }
            return Map.of("syncMode", "PHYSICAL");
        }
        return recordSchemaOnly(projectId, assetId);
    }

    /** 跨节点 PROCESSED 物理拉取（幂等：已 SYNCED 直接返回本地副本；FAILED 重拉）。 */
    public Map<String, Object> pullOnAuthorization(String projectId, String assetId) {
        Map<String, Object> record = findSyncRecord(projectId, assetId);
        if (record != null && "SYNCED".equals(string(record.get("status")))) {
            String localId = string(record.get("local_asset_id"));
            if ("PHYSICAL".equals(string(record.get("sync_mode"))) && notBlank(localId)) {
                return localAsset(localId);
            }
            return Map.of("syncMode", "SCHEMA", "assetId", assetId);
        }
        Map<String, Object> meta = projectAssetMeta(projectId, assetId);
        String provider = string(meta.get("provider_node_id"));
        if (Objects.equals(provider, localNodeId)) {
            Map<String, Object> local = localAsset(assetId);
            if (local != null) {
                nodeDatasetStore.ensureMaterialized(assetId);
                return local;
            }
            return Map.of("syncMode", "LOCAL");
        }
        String stage = string(meta.get("data_stage"));
        if (!"PROCESSED".equals(stage)) {
            writeSyncRecord(projectId, assetId, "", provider, "SCHEMA", "SYNCED", "");
            return Map.of("syncMode", "SCHEMA", "assetId", assetId);
        }
        try {
            AssetDownload dl = httpDownload(provider, assetId);
            if (notBlank(dl.sha256())) {
                String actual = sha256(dl.bytes());
                if (!dl.sha256().equalsIgnoreCase(actual)) {
                    throw new IllegalStateException("校验和不一致 provider=" + dl.sha256() + " 本地=" + actual);
                }
            }
            List<List<String>> parsed = CsvUtil.parse(new String(dl.bytes(), StandardCharsets.UTF_8));
            if (parsed.isEmpty()) {
                throw new IllegalStateException("同步数据表头为空");
            }
            List<String> header = new ArrayList<>(parsed.get(0));
            List<List<String>> data = parsed.size() > 1
                    ? new ArrayList<>(parsed.subList(1, parsed.size()))
                    : new ArrayList<>();
            String localId = "asset-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
            String tableName = NodeDatasetStore.assetTableName(localId);
            String uri = "node-data://" + tableName;
            Map<String, Object> metadata = new LinkedHashMap<>();
            metadata.put("contentType", "text/csv");
            metadata.put("sizeBytes", dl.bytes().length);
            metadata.put("sha256", dl.sha256());
            metadata.put("sourceAssetId", assetId);
            metadata.put("providerNodeId", provider);
            jdbc.update("insert into ds_data_asset"
                            + "(id,name,provider_node_id,processor_node_id,ingestion_type,modality,data_stage,source_asset_id,datatable_id,storage_uri,metadata_json,created_by,created_at,updated_at,version,status,deleted)"
                            + " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,'ACTIVE',0)",
                    localId, string(meta.getOrDefault("name", assetId)), provider, localNodeId,
                    "SYNCED", "TABULAR", "PROCESSED", assetId, localId, uri, json(metadata),
                    "system", now(), now());
            nodeDatasetStore.materializeExternal(localId, provider, assetId, header, data, dl.sha256());
            writeSyncRecord(projectId, assetId, localId, provider, "PHYSICAL", "SYNCED", "");
            return localAsset(localId);
        } catch (Exception e) {
            log.warn("跨节点同步失败 projectId={} assetId={} provider={}: {}", projectId, assetId, provider, e.getMessage());
            writeSyncRecord(projectId, assetId, "", provider, "PHYSICAL", "FAILED", truncate(e.getMessage(), 900));
            throw new IllegalStateException("跨节点同步失败: " + e.getMessage(), e);
        }
    }

    /** 跨节点 RAW：不传真实行，仅记录 SCHEMA 同步。 */
    public Map<String, Object> recordSchemaOnly(String projectId, String assetId) {
        Map<String, Object> meta = projectAssetMeta(projectId, assetId);
        String provider = string(meta.get("provider_node_id"));
        writeSyncRecord(projectId, assetId, "", provider, "SCHEMA", "SYNCED", "");
        return Map.of("syncMode", "SCHEMA", "assetId", assetId);
    }

    /** 沙箱挂载用：返回已本地同步的物理表名（{@code asset_xxx}）；未物理同步返回 {@code null}。 */
    public String localPhysicalTable(String projectId, String assetId) {
        Map<String, Object> record = findSyncRecord(projectId, assetId);
        if (record != null && "PHYSICAL".equals(string(record.get("sync_mode")))
                && "SYNCED".equals(string(record.get("status"))) && notBlank(record.get("local_asset_id"))) {
            return NodeDatasetStore.assetTableName(string(record.get("local_asset_id")));
        }
        return null;
    }

    /** 返回已本地物化的同步副本资产行；未同步返回 {@code null}。 */
    public Map<String, Object> localSyncedAsset(String projectId, String assetId) {
        String table = localPhysicalTable(projectId, assetId);
        if (table == null) {
            return null;
        }
        Map<String, Object> record = findSyncRecord(projectId, assetId);
        return localAsset(string(record.get("local_asset_id")));
    }

    /* ------------------------------ HTTP ------------------------------ */

    private AssetDownload httpDownload(String providerNodeId, String assetId) {
        String url = "http://" + gateway + DOWNLOAD_PATH + "?assetId=" + assetId;
        String host = "secretpad." + providerNodeId + ".svc";
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            var builder = RequestBuilder.create("GET").setUri(url)
                    .setConfig(RequestConfig.custom()
                            .setConnectTimeout(10_000)
                            .setConnectionRequestTimeout(10_000)
                            .setSocketTimeout(120_000)
                            .build())
                    .setHeader("Host", host)
                    .setHeader("kuscia-origin-source", localNodeId);
            try (CloseableHttpResponse response = client.execute(builder.build())) {
                if (response.getStatusLine().getStatusCode() != 200) {
                    throw new IllegalStateException("provider " + providerNodeId + " 返回 "
                            + response.getStatusLine().getStatusCode());
                }
                if (response.getEntity() == null) {
                    throw new IllegalStateException("provider 返回空数据");
                }
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                response.getEntity().writeTo(buffer);
                String sha = response.getFirstHeader("X-Asset-Sha256") == null
                        ? "" : response.getFirstHeader("X-Asset-Sha256").getValue();
                return new AssetDownload(buffer.toByteArray(), sha);
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("跨节点同步请求失败: " + e.getMessage(), e);
        }
    }

    /* ------------------------------ 内部工具 ------------------------------ */

    private Map<String, Object> findSyncRecord(String projectId, String assetId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select * from ds_asset_sync_record where project_id=? and asset_id=? order by synced_at desc limit 1",
                projectId, assetId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private void writeSyncRecord(String projectId, String assetId, String localAssetId,
            String providerNodeId, String mode, String status, String error) {
        String id = "syn-" + sha256short(projectId + ":" + assetId);
        String syncedAt = "SYNCED".equals(status) ? now() : "";
        jdbc.update("insert or ignore into ds_asset_sync_record"
                        + "(id,project_id,asset_id,local_asset_id,provider_node_id,sync_mode,status,synced_at,error_message)"
                        + " values(?,?,?,?,?,?,?,?,?)",
                id, projectId, assetId, localAssetId, providerNodeId, mode, status, syncedAt, error);
        jdbc.update("update ds_asset_sync_record set local_asset_id=?,provider_node_id=?,sync_mode=?,"
                        + "status=?,synced_at=?,error_message=? where id=?",
                localAssetId, providerNodeId, mode, status, syncedAt, error, id);
    }

    private Map<String, Object> localAsset(String assetId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select * from ds_data_asset where id=? and deleted=0", assetId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> projectAssetMeta(String projectId, String assetId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select asset_json,provider_node_id from ds_project_asset "
                        + "where project_id=? and asset_id=? and deleted=0 and coalesce(is_deleted,0)=0",
                projectId, assetId);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("资产未挂载到所选项目: " + assetId);
        }
        Map<String, Object> row = rows.get(0);
        Map<String, Object> meta;
        try {
            meta = new LinkedHashMap<>(objectMapper.readValue(string(row.get("asset_json")), Map.class));
        } catch (Exception e) {
            meta = new LinkedHashMap<>();
        }
        meta.put("provider_node_id", row.get("provider_node_id"));
        return meta;
    }

    private String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (Exception e) {
            throw new IllegalStateException("计算校验和失败", e);
        }
    }

    private String sha256short(String text) {
        return sha256(text.getBytes(StandardCharsets.UTF_8)).substring(0, 20);
    }

    private String truncate(String s, int n) {
        if (s == null) {
            return "";
        }
        return s.length() <= n ? s : s.substring(0, n) + "...";
    }

    private String string(Object o) {
        return o == null ? "" : String.valueOf(o);
    }

    private boolean notBlank(Object o) {
        return o != null && !String.valueOf(o).isBlank();
    }

    private String now() {
        return LocalDateTime.now().toString();
    }

    private String json(Object o) {
        try {
            return objectMapper.writeValueAsString(o);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }
}
