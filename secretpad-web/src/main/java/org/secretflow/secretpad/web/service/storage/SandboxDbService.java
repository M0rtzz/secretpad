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

import org.secretflow.secretpad.web.service.MinioAssetStorage;
import org.secretflow.secretpad.web.service.governance.CsvUtil;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 * 沙箱级权威存储（Stage 3）：每个沙箱一个后端权威 SQLite 库 {@code sandbox_data.db}。
 *
 * <p>路径 {@code {data.dir-path}/sandbox-db/{sandboxId}/sandbox_data.db}，带 canonical 守卫。
 * 仅物化已授权 PROCESSED 数据（本节点 + 跨节点已同步），RAW 源数据绝不进沙箱（黄金法则）。
 * 库内维护 {@code _sandbox_manifest}（表清单）+ {@code _sandbox_files}（非结构化清单，预留）。
 * 挂载变更（{@code syncDatasetMounts}）与 START 时 {@link #rebuild} 幂等重建：
 * 资产表 {@code asset_{assetId}} 重建，{@code result_*} 结果表保留（只重写清单）。</p>
 */
@Service
public class SandboxDbService {

    private static final Logger log = LoggerFactory.getLogger(SandboxDbService.class);
    private static final String NODE_DATA_PREFIX = "node-data://";

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final MinioAssetStorage storage;
    private final NodeDatasetStore nodeDatasetStore;
    private final String dataDirPath;
    private final String localNodeId;

    public SandboxDbService(@Qualifier("jdbcTemplate") JdbcTemplate jdbc, ObjectMapper mapper,
            MinioAssetStorage storage, NodeDatasetStore nodeDatasetStore,
            @Value("${secretpad.data.dir-path:/app/data/}") String dataDirPath,
            @Value("${secretpad.node-id:kuscia-system}") String localNodeId) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.storage = storage;
        this.nodeDatasetStore = nodeDatasetStore;
        this.dataDirPath = dataDirPath;
        this.localNodeId = localNodeId;
    }

    /* ------------------------------ 路径 ------------------------------ */

    /**
     * 沙箱权威库路径：{@code {data.dir-path}/sandbox-db/{sandboxId}/sandbox_data.db}。
     * sandboxId 收敛到 {@code [a-zA-Z0-9_-]} 且禁止 {@code ..} 越级，根目录 canonical 守卫。
     */
    public Path sandboxDbPath(String sandboxId) {
        String safe = sanitizeSandboxId(sandboxId);
        Path root = Path.of(dataDirPath).toAbsolutePath().normalize().resolve("sandbox-db").normalize();
        Path db = root.resolve(safe).resolve("sandbox_data.db").toAbsolutePath().normalize();
        if (!db.startsWith(root)) {
            throw new IllegalArgumentException("非法沙箱路径: " + sandboxId);
        }
        return db;
    }

    private String sanitizeSandboxId(String sandboxId) {
        String s = sandboxId == null ? "" : sandboxId.trim();
        if (s.isEmpty() || ".".equals(s) || "..".equals(s) || s.contains("/") || s.contains("\\")) {
            throw new IllegalArgumentException("非法沙箱标识: " + sandboxId);
        }
        return s;
    }

    /* ------------------------------ 重建 ------------------------------ */

    /**
     * 幂等重建沙箱权威库：挂载表重建（asset_{assetId}，仅 PROCESSED），结果表保留。
     * 完成后刷新 {@code ds_sandbox_data_dir}（MOUNT+RESULT）与 {@code ds_sandbox_db}。
     */
    @Transactional
    public void rebuild(String sandboxId) {
        String safeId = sanitizeSandboxId(sandboxId);
        Path db = sandboxDbPath(safeId);
        long start = System.currentTimeMillis();
        List<Map<String, Object>> mounts = jdbc.queryForList(
                "select * from ds_sandbox_dataset_mount where sandbox_id=? and deleted=0 and status='READY'",
                safeId);
        // 结果表与既有清单保留（挂载重建不丢失开发产出）
        List<Map<String, Object>> manifest = new ArrayList<>(preservedResultEntries(db));
        Set<String> dropped = new HashSet<>();
        for (Map<String, Object> mount : mounts) {
            String assetId = string(mount.get("asset_id"));
            Map<String, Object> asset = dsAsset(assetId);
            if (asset == null) {
                // 跨节点物理同步副本：挂载记录沿用源资产 id，解析到本地同步副本（ds_asset_sync_record.local_asset_id）
                String localId = jdbc.query("select local_asset_id from ds_asset_sync_record where asset_id=? and status='SYNCED' and local_asset_id<>'' order by synced_at desc limit 1",
                        rs -> rs.next() ? rs.getString(1) : null, assetId);
                if (localId != null && !localId.isBlank()) {
                    asset = dsAsset(localId);
                }
            }
            if (asset == null) {
                continue;
            }
            if (!"PROCESSED".equals(string(asset.get("data_stage")))) {
                // 黄金法则：RAW 源数据绝不进沙箱，仅保留挂载元数据
                log.info("沙箱 {} 跳过 RAW 资产 {}（源数据禁入沙箱）", safeId, assetId);
                continue;
            }
            List<List<String>> rows = resolveRows(mount, asset);
            if (rows == null || rows.isEmpty()) {
                log.warn("沙箱 {} 资产 {} 无可注入数据，跳过", safeId, assetId);
                continue;
            }
            List<String> header = rows.get(0);
            List<List<String>> data = rows.size() > 1
                    ? new ArrayList<>(rows.subList(1, rows.size()))
                    : new ArrayList<>();
            String table = NodeDatasetStore.assetTableName(assetId);
            SqliteTableLoader.dropTableIfExists(db, table);
            SqliteTableLoader.Materialized m = SqliteTableLoader.materializeToFile(db, table, header, data, false);
            dropped.add(table);
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("table_name", m.tableName());
            entry.put("asset_id", assetId);
            entry.put("name", string(asset.getOrDefault("name", assetId)));
            entry.put("kind", "MOUNT");
            entry.put("source", mountSource(mount));
            entry.put("row_count", m.rowCount());
            manifest.add(entry);
        }
        writeManifest(db, manifest);
        refreshDataDir(safeId, manifest);
        refreshDbRecord(safeId, db, manifest.size(), manifestRowSum(manifest));
        log.info("沙箱 {} 权威库重建完成: {} 表, {} 行, {}ms", safeId, manifest.size(),
                manifestRowSum(manifest), System.currentTimeMillis() - start);
    }

    /** 从既有库读取 kind=RESULT/OPERATOR 的清单行（结果表/画布节点输出保留在库中，只回写其清单）。 */
    private List<Map<String, Object>> preservedResultEntries(Path db) {
        List<Map<String, Object>> preserved = new ArrayList<>();
        if (!Files.exists(db)) {
            return preserved;
        }
        for (Map<String, Object> e : readManifest(db)) {
            String kind = string(e.get("kind"));
            if ("RESULT".equals(kind) || "OPERATOR".equals(kind)) {
                preserved.add(e);
            }
        }
        return preserved;
    }

    /** 解析挂载数据：node-data:// 表 → 节点权威库；s3:// 或普通 URI → 节点库/MinIO 回退。 */
    private List<List<String>> resolveRows(Map<String, Object> mount, Map<String, Object> asset) {
        String stagingUri = string(mount.get("staging_uri"));
        if (stagingUri.startsWith(NODE_DATA_PREFIX)) {
            String table = stagingUri.substring(NODE_DATA_PREFIX.length());
            List<List<String>> rows = SqliteTableLoader.readRows(
                    nodeDatasetStore.localNodeDbPath(), table, Integer.MAX_VALUE);
            if (rows == null || rows.isEmpty()) {
                throw new IllegalStateException("节点权威库缺少表: " + table);
            }
            return rows;
        }
        String assetId = string(asset.get("id"));
        String uri = string(asset.getOrDefault("storage_uri", stagingUri));
        if (uri.startsWith("s3://")) {
            // 懒回填兜底：存量未物化资产读时补
            nodeDatasetStore.ensureMaterialized(assetId);
        }
        List<List<String>> rows = nodeDatasetStore.readTableRows(assetId, Integer.MAX_VALUE);
        if (rows != null && !rows.isEmpty()) {
            return rows;
        }
        try (InputStream in = storage.open(uri)) {
            return CsvUtil.parse(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new IllegalStateException("读取挂载数据失败: " + assetId, e);
        }
    }

    private String mountSource(Map<String, Object> mount) {
        return Objects.equals(string(mount.get("provider_node_id")), localNodeId) ? "LOCAL" : "SYNCED";
    }

    /* ------------------------------ 读取（前端数据目录/预览） ------------------------------ */

    /** 沙箱数据目录：{@code ds_sandbox_data_dir}（MOUNT + RESULT）。 */
    public Map<String, Object> directory(String sandboxId) {
        String safeId = sanitizeSandboxId(sandboxId);
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select * from ds_sandbox_data_dir where sandbox_id=? and deleted=0 "
                        + "order by case kind when 'MOUNT' then 0 else 1 end, created_at",
                safeId);
        List<Map<String, Object>> items = new ArrayList<>();
        for (Map<String, Object> r : rows) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("tableName", r.get("table_name"));
            item.put("assetId", r.get("asset_id"));
            item.put("name", r.get("name"));
            item.put("kind", r.get("kind"));
            item.put("source", r.get("source"));
            item.put("rowCount", r.get("row_count"));
            item.put("modality", r.get("modality"));
            item.put("columns", columnsJson(r.get("columns_json")));
            items.add(item);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("sandboxId", safeId);
        result.put("items", items);
        return result;
    }

    /** 沙箱表全量读取（header + rows，供计算任务输入源）；仅允许清单内已知表。 */
    public Map<String, Object> readTable(String sandboxId, String tableName) {
        String safeId = sanitizeSandboxId(sandboxId);
        String safeTable = SqliteTableLoader.sanitizeTableName(tableName);
        Path db = sandboxDbPath(safeId);
        if (!Files.exists(db)) {
            throw new NoSuchElementException("沙箱数据库不存在");
        }
        requireKnownTable(db, safeTable);
        List<List<String>> rows = SqliteTableLoader.readRows(db, safeTable, Integer.MAX_VALUE);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("tableName", safeTable);
        result.put("header", rows.isEmpty() ? List.of() : rows.get(0));
        result.put("rows", rows.size() > 1 ? new ArrayList<>(rows.subList(1, rows.size())) : new ArrayList<>());
        return result;
    }

    /** 沙箱表预览（schema + 前 limit 行）；仅允许清单内已知表。 */
    public Map<String, Object> previewTable(String sandboxId, String tableName, int limit) {
        String safeId = sanitizeSandboxId(sandboxId);
        String safeTable = SqliteTableLoader.sanitizeTableName(tableName);
        Path db = sandboxDbPath(safeId);
        if (!Files.exists(db)) {
            throw new NoSuchElementException("沙箱数据库不存在");
        }
        requireKnownTable(db, safeTable);
        List<List<String>> rows = SqliteTableLoader.readRows(db, safeTable, Math.max(1, Math.min(limit, 500)));
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("tableName", safeTable);
        result.put("schema", SqliteTableLoader.tableSchema(db, safeTable));
        result.put("rows", rows.isEmpty() ? List.of() : rows.subList(1, rows.size()));
        result.put("totalRows", SqliteTableLoader.countRows(db, safeTable));
        return result;
    }

    /** 沙箱单表 CSV 导出（header + 全量行 → UTF-8 CSV 字节）；仅允许清单内已知表。 */
    public byte[] readTableCsv(String sandboxId, String tableName) {
        Map<String, Object> table = readTable(sandboxId, tableName);
        @SuppressWarnings("unchecked")
        List<String> header = (List<String>) table.get("header");
        @SuppressWarnings("unchecked")
        List<List<String>> rows = (List<List<String>>) table.get("rows");
        return CsvUtil.toCsv(header, rows).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Creates a short-lived SQLite database containing only one authorized input table.
     * Callers must delete the returned file in a finally block.
     */
    public Path createExecutionSnapshot(String sandboxId, Set<String> tableNames) {
        if (tableNames == null || tableNames.isEmpty()) throw new IllegalArgumentException("执行快照至少需要一个输入表");
        try {
            Path snapshot = Files.createTempFile("sandbox-exec-", ".db");
            for (String tableName : tableNames) {
                String safeTable = SqliteTableLoader.sanitizeTableName(tableName);
                Map<String, Object> table = readTable(sandboxId, safeTable);
                @SuppressWarnings("unchecked")
                List<String> header = (List<String>) table.get("header");
                @SuppressWarnings("unchecked")
                List<List<String>> rows = (List<List<String>>) table.get("rows");
                SqliteTableLoader.materializeToFile(snapshot, safeTable, header, rows, false);
            }
            return snapshot;
        } catch (IOException e) {
            throw new IllegalStateException("创建受限执行快照失败", e);
        }
    }

    /** Serializes and removes a single-table execution snapshot. */
    public byte[] executionSnapshotBytes(String sandboxId, Set<String> tableNames) {
        Path snapshot = createExecutionSnapshot(sandboxId, tableNames);
        try {
            return Files.readAllBytes(snapshot);
        } catch (IOException e) {
            throw new IllegalStateException("读取受限执行快照失败", e);
        } finally {
            try { Files.deleteIfExists(snapshot); }
            catch (IOException e) { log.warn("删除受限执行快照失败: {}", snapshot, e); }
        }
    }

    /** 下载沙箱权威库文件（仅沙箱创建人，由控制器校验）。 */
    public byte[] downloadBytes(String sandboxId) {
        Path db = sandboxDbPath(sandboxId);
        if (!Files.exists(db)) {
            throw new NoSuchElementException("沙箱数据库不存在");
        }
        try {
            return Files.readAllBytes(db);
        } catch (IOException e) {
            throw new IllegalStateException("读取沙箱数据库失败: " + sandboxId, e);
        }
    }

    /* ------------------------------ 结果回填（Stage 4 供 DataDevService 调） ------------------------------ */

    /**
     * 任务 SUCCEEDED 后把产出表写入沙箱库（{@code result_{taskId}}，kind=RESULT）并登记清单与数据目录。
     */
    @Transactional
    public Map<String, Object> backfillResultTable(String sandboxId, String taskId, String name,
            List<String> header, List<List<String>> data) {
        return backfillTable(sandboxId, "result_" + SqliteTableLoader.sanitizeTableName(taskId),
                "RESULT", name, header, data);
    }

    /**
     * 画布节点输出回填：写入沙箱库 {@code op_{runId}_{nodeId}}（kind=OPERATOR）。
     *
     * <p>op_* 表仅画布内部消费（下游节点输入 + 节点输出查看/导出），遵循「结果不能被沙箱消费」边界：
     * 不允许作为 data-dev 任务源表（{@link #isOperatorTable}），不允许挂载项目。重建/挂载变更时保留。</p>
     */
    @Transactional
    public Map<String, Object> backfillOperatorTable(String sandboxId, String runId, String nodeId,
            String name, List<String> header, List<List<String>> data) {
        String table = "op_" + SqliteTableLoader.sanitizeTableName(runId)
                + "_" + SqliteTableLoader.sanitizeTableName(nodeId);
        return backfillTable(sandboxId, table, "OPERATOR", name, header, data);
    }

    /**
     * 通用产出表回填：写入沙箱库指定表名并登记清单（kind 由调用方指定 RESULT/OPERATOR）。
     */
    @Transactional
    public Map<String, Object> backfillTable(String sandboxId, String table, String kind, String name,
            List<String> header, List<List<String>> data) {
        String safeId = sanitizeSandboxId(sandboxId);
        Path db = sandboxDbPath(safeId);
        String safeTable = SqliteTableLoader.sanitizeTableName(table);
        SqliteTableLoader.dropTableIfExists(db, safeTable);
        SqliteTableLoader.Materialized m = SqliteTableLoader.materializeToFile(db, safeTable, header, data, false);
        List<Map<String, Object>> manifest = readManifest(db);
        manifest.removeIf(e -> safeTable.equals(string(e.get("table_name"))));
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("table_name", safeTable);
        entry.put("asset_id", "");
        entry.put("name", name == null || name.isBlank() ? safeTable : name);
        entry.put("kind", kind);
        entry.put("source", "LOCAL");
        entry.put("row_count", m.rowCount());
        manifest.add(entry);
        writeManifest(db, manifest);
        refreshDataDir(safeId, manifest);
        refreshDbRecord(safeId, db, manifest.size(), manifestRowSum(manifest));
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("tableName", safeTable);
        result.put("rowCount", m.rowCount());
        result.put("columns", m.columns());
        return result;
    }

    /** 沙箱库内是否存在某表（清单维度）。 */
    public boolean hasTable(String sandboxId, String tableName) {
        String safeTable = SqliteTableLoader.sanitizeTableName(tableName);
        return readManifest(sandboxDbPath(sanitizeSandboxId(sandboxId))).stream()
                .anyMatch(e -> safeTable.equals(string(e.get("table_name"))));
    }

    /**
     * 是否为沙箱计算结果表（RESULT）。结果表只能预览/导出，不能作为沙箱计算源、不能挂载到项目。
     * 判定依据：清单 kind=RESULT，或以 {@code result_} 为前缀（结果表命名约定）。
     */
    public boolean isResultTable(String sandboxId, String tableName) {
        String safeTable = SqliteTableLoader.sanitizeTableName(tableName);
        if (safeTable.startsWith("result_")) {
            return true;
        }
        return readManifest(sandboxDbPath(sanitizeSandboxId(sandboxId))).stream()
                .anyMatch(e -> safeTable.equals(string(e.get("table_name"))) && "RESULT".equals(string(e.get("kind"))));
    }

    /**
     * 是否为画布节点输出表（OPERATOR）。op_* 表仅画布内部消费（下游节点 + 预览/导出），
     * 不允许作为 data-dev 任务源表、不允许挂载项目。
     */
    public boolean isOperatorTable(String sandboxId, String tableName) {
        String safeTable = SqliteTableLoader.sanitizeTableName(tableName);
        if (safeTable.startsWith("op_")) {
            return true;
        }
        return readManifest(sandboxDbPath(sanitizeSandboxId(sandboxId))).stream()
                .anyMatch(e -> safeTable.equals(string(e.get("table_name"))) && "OPERATOR".equals(string(e.get("kind"))));
    }

    /** 沙箱清单中表的显示名（画布中间结果友好名）；清单无 name 时回退为表名本身。 */
    public String tableDisplayName(String sandboxId, String tableName) {
        String safeTable = SqliteTableLoader.sanitizeTableName(tableName);
        return readManifest(sandboxDbPath(sanitizeSandboxId(sandboxId))).stream()
                .filter(e -> safeTable.equals(string(e.get("table_name"))))
                .map(e -> string(e.get("name")))
                .filter(n -> !n.isBlank())
                .findFirst()
                .orElse(tableName);
    }

    /* ------------------------------ 清单/系统表 ------------------------------ */

    private void requireKnownTable(Path db, String table) {
        boolean known = readManifest(db).stream()
                .anyMatch(e -> table.equals(string(e.get("table_name"))));
        if (!known) {
            throw new IllegalArgumentException("沙箱内无此表: " + table);
        }
    }

    private List<Map<String, Object>> readManifest(Path db) {
        List<Map<String, Object>> entries = new ArrayList<>();
        if (!Files.exists(db)) {
            return entries;
        }
        try (var conn = SqliteTableLoader.open(db)) {
            try (var stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS _sandbox_manifest "
                        + "(table_name TEXT PRIMARY KEY, asset_id TEXT, name TEXT, kind TEXT, source TEXT, row_count INTEGER)");
            }
            try (var stmt = conn.createStatement();
                 var rs = stmt.executeQuery("SELECT * FROM _sandbox_manifest")) {
                while (rs.next()) {
                    Map<String, Object> e = new LinkedHashMap<>();
                    e.put("table_name", rs.getString("table_name"));
                    e.put("asset_id", rs.getString("asset_id"));
                    e.put("name", rs.getString("name"));
                    e.put("kind", rs.getString("kind"));
                    e.put("source", rs.getString("source"));
                    e.put("row_count", rs.getLong("row_count"));
                    entries.add(e);
                }
            }
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException("读沙箱清单失败: " + e.getMessage(), e);
        }
        return entries;
    }

    /** 重写清单表（{@code _sandbox_manifest} + {@code _sandbox_files} 预置空）。 */
    private void writeManifest(Path db, List<Map<String, Object>> manifest) {
        try (var conn = SqliteTableLoader.open(db)) {
            try (var stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS _sandbox_manifest");
                stmt.execute("DROP TABLE IF EXISTS _sandbox_files");
                stmt.execute("CREATE TABLE _sandbox_manifest "
                        + "(table_name TEXT PRIMARY KEY, asset_id TEXT, name TEXT, kind TEXT, source TEXT, row_count INTEGER)");
                stmt.execute("CREATE TABLE _sandbox_files "
                        + "(id TEXT PRIMARY KEY, filename TEXT, content_type TEXT, local_cache_path TEXT, minio_key TEXT)");
            }
            try (var ps = conn.prepareStatement(
                    "INSERT INTO _sandbox_manifest(table_name,asset_id,name,kind,source,row_count) VALUES(?,?,?,?,?,?)")) {
                for (Map<String, Object> e : manifest) {
                    ps.setString(1, string(e.get("table_name")));
                    ps.setString(2, string(e.get("asset_id")));
                    ps.setString(3, string(e.get("name")));
                    ps.setString(4, string(e.get("kind")));
                    ps.setString(5, string(e.get("source")));
                    ps.setLong(6, longValue(e.get("row_count")));
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException("写沙箱清单失败: " + e.getMessage(), e);
        }
    }

    /* ------------------------------ 平台库刷新 ------------------------------ */

    private void refreshDataDir(String sandboxId, List<Map<String, Object>> manifest) {
        jdbc.update("delete from ds_sandbox_data_dir where sandbox_id=?", sandboxId);
        for (Map<String, Object> e : manifest) {
            String tableName = string(e.get("table_name"));
            List<Map<String, Object>> schema = SqliteTableLoader.tableSchema(
                    sandboxDbPath(sandboxId), tableName);
            List<String> cols = new ArrayList<>();
            for (Map<String, Object> c : schema) {
                cols.add(string(c.get("name")));
            }
            String id = "dd-" + sandboxId + "-" + tableName;
            jdbc.update("insert or ignore into ds_sandbox_data_dir"
                            + "(id,sandbox_id,kind,asset_id,table_name,name,modality,row_count,columns_json,source,created_at,updated_at,deleted)"
                            + " values(?,?,?,?,?,?,?,?,?,?,?,?,0)",
                    id, sandboxId, string(e.get("kind")), string(e.get("asset_id")), tableName,
                    string(e.get("name")), "TABULAR", longValue(e.get("row_count")), json(cols),
                    string(e.get("source")), now(), now());
        }
    }

    private void refreshDbRecord(String sandboxId, Path db, int tableCount, long rowCount) {
        long fileSize = 0;
        String checksum = "";
        if (Files.exists(db)) {
            try {
                fileSize = Files.size(db);
                checksum = sha256(Files.readAllBytes(db));
            } catch (IOException ignored) {
                // 文件读取失败不阻断，仅记录 0
            }
        }
        String abs = db.toAbsolutePath().toString();
        jdbc.update("insert or ignore into ds_sandbox_db"
                        + "(sandbox_id,db_path,file_size,checksum,table_count,row_count,built_at,status)"
                        + " values(?,?,?,?,?,?,?,?)",
                sandboxId, abs, fileSize, checksum, tableCount, rowCount, now(), "READY");
        jdbc.update("update ds_sandbox_db set db_path=?,file_size=?,checksum=?,table_count=?,"
                        + "row_count=?,built_at=?,status=? where sandbox_id=?",
                abs, fileSize, checksum, tableCount, rowCount, now(), "READY", sandboxId);
    }

    /* ------------------------------ 内部工具 ------------------------------ */

    private Map<String, Object> dsAsset(String assetId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select * from ds_data_asset where id=? and deleted=0", assetId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    @SuppressWarnings("unchecked")
    private List<String> columnsJson(Object json) {
        if (json == null || String.valueOf(json).isBlank()) {
            return List.of();
        }
        try {
            return new ArrayList<>((List<String>) mapper.readValue(String.valueOf(json), List.class));
        } catch (Exception e) {
            return List.of();
        }
    }

    private long manifestRowSum(List<Map<String, Object>> manifest) {
        long sum = 0;
        for (Map<String, Object> e : manifest) {
            sum += longValue(e.get("row_count"));
        }
        return sum;
    }

    private long longValue(Object o) {
        if (o == null) {
            return 0;
        }
        if (o instanceof Number n) {
            return n.longValue();
        }
        try {
            return Long.parseLong(String.valueOf(o).trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (Exception e) {
            throw new IllegalStateException("计算校验和失败", e);
        }
    }

    private String string(Object o) {
        return o == null ? "" : String.valueOf(o);
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
