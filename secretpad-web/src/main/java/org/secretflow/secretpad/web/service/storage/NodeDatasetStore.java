/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.service.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.secretflow.secretpad.web.service.MinioAssetStorage;
import org.secretflow.secretpad.web.service.governance.CsvUtil;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 节点级权威存储（Stage 1）：每个节点一个 SQLite 物理库 {@code node_data.db}。
 *
 * <p>结构化资产（TABULAR）按表 {@code asset_{assetId}} 落地到节点库；非结构化资产（IMAGE）
 * 在 MinIO 留原件，平台库 {@code ds_node_dataset} 仅写 MINIO_OBJECT manifest 行。平台库
 * {@code ds_node_dataset} 是本节点的物理索引，{@code provenance_json} 记录来源
 * （{@code {sourceAssetId, providerNodeId, syncedAt}}），供跨节点同步溯源。</p>
 *
 * <p>三件事做对：路径带 canonical 守卫（nodeId 收敛、禁止越级）；物化幂等
 * （{@code insert or ignore} + update，确定性主键 {@code nd-{nodeId}-{assetId}}）；
 * 懒回填读 MinIO 原件（导入即入库为 eager，存量资产读时补）。</p>
 */
@Service
public class NodeDatasetStore {

    private final JdbcTemplate jdbc;
    private final MinioAssetStorage storage;
    private final ObjectMapper mapper;
    private final String dataDirPath;
    private final String localNodeId;

    public NodeDatasetStore(@Qualifier("jdbcTemplate") JdbcTemplate jdbc, MinioAssetStorage storage,
            ObjectMapper mapper,
            @Value("${secretpad.data.dir-path:/app/data/}") String dataDirPath,
            @Value("${secretpad.node-id:kuscia-system}") String localNodeId) {
        this.jdbc = jdbc;
        this.storage = storage;
        this.mapper = mapper;
        this.dataDirPath = dataDirPath;
        this.localNodeId = localNodeId;
    }

    /* ------------------------------ 路径 ------------------------------ */

    /** 本地节点权威库路径。 */
    public Path localNodeDbPath() {
        return nodeDbPath(localNodeId);
    }

    /**
     * 节点权威库路径：{@code {data.dir-path}/node-sqlite/{nodeId}/node_data.db}。
     * nodeId 收敛到 {@code [a-zA-Z0-9_-]} 且禁止 {@code ..} 越级，根目录 canonical 守卫。
     */
    public Path nodeDbPath(String nodeId) {
        String safe = sanitizeNodeId(nodeId);
        Path nodeRoot = Path.of(dataDirPath).toAbsolutePath().normalize().resolve("node-sqlite").normalize();
        Path db = nodeRoot.resolve(safe).resolve("node_data.db").toAbsolutePath().normalize();
        if (!db.startsWith(nodeRoot)) {
            throw new IllegalArgumentException("非法节点路径: " + nodeId);
        }
        return db;
    }

    private String sanitizeNodeId(String nodeId) {
        String s = nodeId == null ? "" : nodeId.trim();
        if (s.isEmpty() || ".".equals(s) || "..".equals(s) || s.contains("/") || s.contains("\\")) {
            throw new IllegalArgumentException("非法节点标识: " + nodeId);
        }
        return s;
    }

    /** 资产在节点库中的表名（{@code asset_xxx}）。 */
    public static String assetTableName(String assetId) {
        return SqliteTableLoader.sanitizeTableName(assetId);
    }

    /* ------------------------------ 物化 ------------------------------ */

    /**
     * 幂等 + 懒回填：确保资产在本地节点库中物化。
     * TABULAR → 从 MinIO 原件读 CSV 落地为 SQLITE_TABLE；IMAGE → 写 MINIO_OBJECT manifest 行。
     *
     * @return {@code ds_node_dataset} 索引行；资产不存在返回 {@code null}
     */
    @Transactional
    public Map<String, Object> ensureMaterialized(String assetId) {
        List<Map<String, Object>> assets = jdbc.queryForList(
                "select * from ds_data_asset where id=? and deleted=0", assetId);
        if (assets.isEmpty()) {
            return null;
        }
        Map<String, Object> asset = assets.get(0);
        Map<String, Object> existing = findIndex(assetId);
        if (existing != null) {
            if (!"0".equals(String.valueOf(existing.get("deleted")))) {
                jdbc.update("update ds_node_dataset set deleted=0, updated_at=? where asset_id=?", now(), assetId);
            }
            return existing;
        }
        String modality = String.valueOf(asset.get("modality"));
        String uri = String.valueOf(asset.getOrDefault("storage_uri", ""));
        String checksum = metadataChecksum(asset);
        if ("IMAGE".equals(modality)) {
            return upsertIndex(assetId, "IMAGE", "MINIO_OBJECT", "", List.of(), 1,
                    uri, checksum, Map.of("sourceAssetId", assetId, "providerNodeId",
                            String.valueOf(asset.get("provider_node_id")), "syncedAt", now()));
        }
        if (uri.isBlank()) {
            throw new IllegalStateException("资产缺少存储引用: " + assetId);
        }
        try (InputStream in = storage.open(uri)) {
            byte[] bytes = in.readAllBytes();
            String csvText = new String(bytes, StandardCharsets.UTF_8);
            if (checksum.isBlank()) {
                checksum = sha256(bytes);
            }
            List<List<String>> parsed = CsvUtil.parse(csvText);
            if (parsed.isEmpty()) {
                throw new IllegalStateException("数据文件表头为空: " + assetId);
            }
            List<String> header = new ArrayList<>(parsed.get(0));
            List<List<String>> data = parsed.size() > 1
                    ? new ArrayList<>(parsed.subList(1, parsed.size()))
                    : new ArrayList<>();
            Path db = localNodeDbPath();
            String table = assetTableName(assetId);
            SqliteTableLoader.dropTableIfExists(db, table);
            SqliteTableLoader.Materialized m = SqliteTableLoader.materializeToFile(
                    db, table, header, data, false);
            return upsertIndex(assetId, "TABULAR", "SQLITE_TABLE", m.tableName(), m.columns(),
                    m.rowCount(), db.toAbsolutePath().toString(), checksum,
                    Map.of("sourceAssetId", assetId,
                            "providerNodeId", String.valueOf(asset.get("provider_node_id")),
                            "syncedAt", now()));
        } catch (IOException e) {
            throw new IllegalStateException("读取数据源失败: " + assetId, e);
        }
    }

    /**
     * 写入跨节点同步的外部数据（Stage 2/3 用）：物化到本地节点库并登记索引。
     * 不创建 {@code ds_data_asset} 目录行——由 {@link org.secretflow.secretpad.web.service.sync.AssetSyncService}
     * 负责目录登记，本方法只负责物理层。
     */
    @Transactional
    public Map<String, Object> materializeExternal(String assetId, String providerNodeId,
            String sourceAssetId, List<String> header, List<List<String>> rows, String checksum) {
        Path db = localNodeDbPath();
        String table = assetTableName(assetId);
        SqliteTableLoader.dropTableIfExists(db, table);
        SqliteTableLoader.Materialized m = SqliteTableLoader.materializeToFile(
                db, table, header, rows, false);
        return upsertIndex(assetId, "TABULAR", "SQLITE_TABLE", m.tableName(), m.columns(),
                m.rowCount(), db.toAbsolutePath().toString(), checksum,
                Map.of("sourceAssetId", sourceAssetId,
                        "providerNodeId", providerNodeId,
                        "syncedAt", now()));
    }

    /* ------------------------------ 读取 ------------------------------ */

    /** 节点库索引行（asset_id 维度）。 */
    public Map<String, Object> findIndex(String assetId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select * from ds_node_dataset where asset_id=? and deleted=0", assetId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    /** 读资产物化表前 limit 行（含表头行）；未物化返回 {@code null}。 */
    public List<List<String>> readTableRows(String assetId, int limit) {
        Map<String, Object> index = findIndex(assetId);
        if (index == null || !"SQLITE_TABLE".equals(String.valueOf(index.get("physical_kind")))) {
            return null;
        }
        Path db = Path.of(String.valueOf(index.get("storage_ref")));
        return SqliteTableLoader.readRows(db, String.valueOf(index.get("table_name")), limit);
    }

    /** 读资产物化表 schema（PRAGMA table_info）；未物化返回 {@code null}。 */
    public List<Map<String, Object>> readTableSchema(String assetId) {
        Map<String, Object> index = findIndex(assetId);
        if (index == null || !"SQLITE_TABLE".equals(String.valueOf(index.get("physical_kind")))) {
            return null;
        }
        Path db = Path.of(String.valueOf(index.get("storage_ref")));
        return SqliteTableLoader.tableSchema(db, String.valueOf(index.get("table_name")));
    }

    /** 导出资产物化表为 CSV 全文；未物化返回 {@code null}（供跨节点同步 provider 侧流式下载）。 */
    public byte[] exportTableCsv(String assetId) {
        Map<String, Object> index = findIndex(assetId);
        if (index == null || !"SQLITE_TABLE".equals(String.valueOf(index.get("physical_kind")))) {
            return null;
        }
        Path db = Path.of(String.valueOf(index.get("storage_ref")));
        List<List<String>> rows = SqliteTableLoader.readRows(db, String.valueOf(index.get("table_name")), Integer.MAX_VALUE);
        if (rows.isEmpty()) {
            return new byte[0];
        }
        return CsvUtil.toCsv(rows.get(0), rows.subList(1, rows.size())).getBytes(StandardCharsets.UTF_8);
    }

    /** 删除资产物化（删除目录行时同步清理节点库表与索引）。 */
    @Transactional
    public void remove(String assetId) {
        Map<String, Object> index = findIndex(assetId);
        if (index != null) {
            if ("SQLITE_TABLE".equals(String.valueOf(index.get("physical_kind")))) {
                try {
                    SqliteTableLoader.dropTableIfExists(
                            Path.of(String.valueOf(index.get("storage_ref"))),
                            String.valueOf(index.get("table_name")));
                } catch (Exception ignored) {
                    // 节点库文件可能已随目录删除，忽略
                }
            }
            jdbc.update("update ds_node_dataset set deleted=1, updated_at=? where asset_id=?", now(), assetId);
        }
    }

    /* ------------------------------ 内部 ------------------------------ */

    private Map<String, Object> upsertIndex(String assetId, String modality, String physicalKind,
            String tableName, List<String> columns, long rowCount, String storageRef,
            String checksum, Map<String, Object> provenance) {
        String id = indexId(assetId);
        jdbc.update("insert or ignore into ds_node_dataset"
                        + "(id,asset_id,node_id,modality,physical_kind,table_name,table_columns_json,row_count,storage_ref,checksum,provenance_json,created_at,updated_at,deleted)"
                        + " values(?,?,?,?,?,?,?,?,?,?,?,?,?,0)",
                id, assetId, localNodeId, modality, physicalKind, tableName, json(columns),
                rowCount, storageRef, checksum, json(provenance), now(), now());
        jdbc.update("update ds_node_dataset set"
                        + " modality=?,physical_kind=?,table_name=?,table_columns_json=?,row_count=?,"
                        + "storage_ref=?,checksum=?,provenance_json=?,updated_at=?,deleted=0 where id=?",
                modality, physicalKind, tableName, json(columns), rowCount, storageRef, checksum,
                json(provenance), now(), id);
        return findIndex(assetId);
    }

    private String indexId(String assetId) {
        return "nd-" + localNodeId + "-" + assetId;
    }

    private String metadataChecksum(Map<String, Object> asset) {
        try {
            String metadata = String.valueOf(asset.getOrDefault("metadata_json", "{}"));
            if (metadata.isBlank() || "{}".equals(metadata)) {
                return "";
            }
            Map<?, ?> map = mapper.readValue(metadata, Map.class);
            Object sha = map.get("sha256");
            return sha == null ? "" : String.valueOf(sha);
        } catch (Exception e) {
            return "";
        }
    }

    private String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (Exception e) {
            throw new IllegalStateException("计算校验和失败", e);
        }
    }

    private String now() {
        return LocalDateTime.now().toString();
    }

    private String json(Object o) {
        try {
            return mapper.writeValueAsString(o);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }
}
