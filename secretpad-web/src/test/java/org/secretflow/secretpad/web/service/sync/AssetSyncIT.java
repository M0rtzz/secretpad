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

import org.secretflow.secretpad.web.SecretPadApplication;
import org.secretflow.secretpad.web.service.governance.CsvUtil;
import org.secretflow.secretpad.web.service.storage.NodeDatasetStore;
import org.secretflow.secretpad.web.service.storage.SqliteTableLoader;
import org.secretflow.secretpad.web.service.util.TestSchemaMigrator;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stage 6 集成测试：跨节点资产同步（AssetSyncService）。
 *
 * <p>覆盖：requester 侧 PROCESSED 物理拉取（mock provider 下载端点，sha256 校验 + 本地物化 +
 * 同步记录 SYNCED）、RAW 仅 SCHEMA 不传真实行、校验和失败 → FAILED 可重拉、provider 侧 download()
 * 仅 PROCESSED+TABULAR 且请求方须为项目参与节点、LOCAL 路径幂等。P2P 内部通道 Headers
 * （{@code Host: secretpad.{provider}.svc} + {@code kuscia-origin-source}）经 mock server 断言。</p>
 */
@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
@ActiveProfiles("test")
@SpringBootTest(classes = SecretPadApplication.class)
@TestPropertySource(properties = {
        "kuscia.nodes=",
        "secretpad.data-sandbox.kuscia.enabled=true",
        "secretpad.node-id=kuscia-system",
        "spring.datasource.default.jdbc-url=jdbc:sqlite:${java.io.tmpdir}/ds-asset-sync-it.sqlite",
        "spring.datasource.quartz.jdbc-url=jdbc:h2:${java.io.tmpdir}/ds-asset-sync-it-quartz.mv.db;DB_CLOSE_ON_EXIT=FALSE",
        "secretpad.data.dir-path=${java.io.tmpdir}/ds-asset-sync-data/",
})
public class AssetSyncIT {

    private static final String DATA_ROOT = System.getProperty("java.io.tmpdir") + "/ds-asset-sync-data";
    private static final List<String> HEADER = List.of("id", "name", "score");
    private static final List<List<String>> ROWS = List.of(
            List.of("1", "alice", "90"),
            List.of("2", "bob", "55"),
            List.of("3", "carol", "70"));
    private static final String REMOTE_CSV = CsvUtil.toCsv(HEADER, ROWS);

    /** mock provider：模拟对端节点 data-assets/sync/download 端点。 */
    private static final MockProvider PROVIDER = new MockProvider();

    @DynamicPropertySource
    static void syncProperties(DynamicPropertyRegistry registry) {
        registry.add("flyway.default.locations", () -> TestSchemaMigrator.dedupedLocation("center"));
        registry.add("secretpad.gateway", () -> "127.0.0.1:" + PROVIDER.port());
        registry.add("secretpad.node-id", () -> "kuscia-system");
    }

    @Resource
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbc;

    @Resource
    private AssetSyncService assetSync;

    @Resource
    private NodeDatasetStore nodeStore;

    /* ------------------------------- 生命周期 ------------------------------- */

    @BeforeEach
    public void reset() throws IOException {
        for (String t : new String[]{"ds_asset_sync_record", "ds_node_dataset", "project_datatable"}) {
            jdbc.update("delete from " + t);
        }
        jdbc.update("delete from ds_data_asset where id like 'ast-%' or id like 'asset-%'");
        jdbc.update("delete from node where node_id in ('alice','carol','eve')");
        jdbc.update("delete from project_node where project_id in ('p1','p2')");
        jdbc.update("delete from project where project_id in ('p1','p2')");
        jdbc.update("delete from ds_project_asset where project_id in ('p1','p2')");
        deleteRecursively(Path.of(DATA_ROOT));
        PROVIDER.reset();
        PROVIDER.assets().put("ast-remote-proc", REMOTE_CSV);
        insertBase();
    }

    private void insertBase() {
        String now = LocalDateTime.now().toString();
        jdbc.update("insert into project(project_id,name,owner_id,is_deleted) values('p1','IT Project','alice',0)");
        jdbc.update("insert into project(project_id,name,owner_id,is_deleted) values('p2','P2 Project','alice',0)");
        jdbc.update("insert into project_node(project_id,node_id,is_deleted) values('p1','alice',0)");
        jdbc.update("insert into project_node(project_id,node_id,is_deleted) values('p2','carol',0)");
        jdbc.update("insert into node(node_id,name,control_node_id,type,mode) values('alice','alice-node','master','normal',0)");
        jdbc.update("insert into node(node_id,name,control_node_id,type,mode) values('carol','carol-node','master','normal',0)");
        // requester 侧：远程项目资产（本节点 = kuscia-system，provider = carol）
        insertProjectAsset("p1", "ast-remote-proc", "carol",
                "{\"name\":\"跨节点脱敏样本\",\"data_stage\":\"PROCESSED\",\"modality\":\"TABULAR\"}");
        insertProjectAsset("p1", "ast-remote-raw", "carol",
                "{\"name\":\"对方原始数据\",\"data_stage\":\"RAW\",\"modality\":\"TABULAR\"}");
        // 本节点资产（LOCAL 路径）
        insertProjectAsset("p1", "ast-local-proc", "kuscia-system",
                "{\"name\":\"本节点脱敏样本\",\"data_stage\":\"PROCESSED\",\"modality\":\"TABULAR\"}");
        insertAsset("ast-local-proc", "本节点脱敏样本", "kuscia-system", "PROCESSED", now);
        nodeStore.materializeExternal("ast-local-proc", "kuscia-system", "src", HEADER, ROWS, "");
        // provider 侧（download 授权测试）：ast-provided-proc 物化，ast-provided-raw 仅 RAW
        insertProjectAsset("p2", "ast-provided-proc", "alice",
                "{\"name\":\"本地脱敏样本\",\"data_stage\":\"PROCESSED\",\"modality\":\"TABULAR\"}");
        insertProjectAsset("p2", "ast-provided-raw", "alice",
                "{\"name\":\"本地原始数据\",\"data_stage\":\"RAW\",\"modality\":\"TABULAR\"}");
        insertAsset("ast-provided-proc", "本地脱敏样本", "alice", "PROCESSED", now);
        insertAsset("ast-provided-raw", "本地原始数据", "alice", "RAW", now);
        nodeStore.materializeExternal("ast-provided-proc", "alice", "src", HEADER, ROWS, "");
    }

    private void insertProjectAsset(String projectId, String assetId, String provider, String assetJson) {
        jdbc.update("insert into ds_project_asset(project_id,asset_id,provider_node_id,attached_by,attached_at,"
                        + "expires_at,deleted,asset_json,is_deleted) values(?,?,?,?,?,?,0,?,0)",
                projectId, assetId, provider, "alice", LocalDateTime.now().toString(), "", assetJson);
    }

    private void insertAsset(String id, String name, String provider, String stage, String now) {
        jdbc.update("insert into ds_data_asset(id,name,provider_node_id,processor_node_id,ingestion_type,"
                        + "modality,data_stage,datatable_id,storage_uri,metadata_json,created_by,created_at,"
                        + "updated_at,version,status,deleted) values(?,?,?,?,?,?,?,?,?,?,?,?,?,1,'ACTIVE',0)",
                id, name, provider, provider, "IMPORTED", "TABULAR", stage, id, "s3://it/" + id, "{}",
                "alice", now, now);
    }

    private long count(String sql, Object... args) {
        Long value = jdbc.queryForObject(sql, Long.class, args);
        return value == null ? 0 : value;
    }

    private Map<String, Object> syncRecord(String projectId, String assetId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select * from ds_asset_sync_record where project_id=? and asset_id=? order by synced_at desc limit 1",
                projectId, assetId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var walk = Files.walk(root)) {
            for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(p);
            }
        }
    }

    /* ------------------------------- 用例 ------------------------------- */

    /** 1. requester 物理拉取：P2P 内部通道、sha 校验、本地 SYNCED 资产 + 节点物化 + 同步记录。 */
    @Test
    public void requesterPhysicalPull() {
        Map<String, Object> result = assetSync.pullOnAuthorization("p1", "ast-remote-proc");
        assertNotNull(result);
        assertEquals("asset-", String.valueOf(result.get("id")).substring(0, 6));
        assertTrue(count("select count(1) from ds_asset_sync_record where project_id='p1' and asset_id='ast-remote-proc' "
                + "and sync_mode='PHYSICAL' and status='SYNCED' and local_asset_id<>''") == 1,
                "应写入 PHYSICAL/SYNCED 同步记录");
        String localId = String.valueOf(result.get("id"));
        // 本地 SYNCED 资产
        Map<String, Object> local = jdbc.queryForMap("select * from ds_data_asset where id=?", localId);
        assertEquals("SYNCED", String.valueOf(local.get("ingestion_type")));
        assertEquals("PROCESSED", String.valueOf(local.get("data_stage")));
        assertEquals("carol", String.valueOf(local.get("provider_node_id")));
        assertEquals("ast-remote-proc", String.valueOf(local.get("source_asset_id")));
        assertTrue(String.valueOf(local.get("storage_uri")).startsWith("node-data://"));
        // 节点物化（readTableRows 含表头行：header + 3 数据行）
        List<List<String>> table = nodeStore.readTableRows(localId, 100);
        assertEquals(HEADER, table.get(0));
        assertEquals(ROWS, table.subList(1, table.size()));
        assertTrue(table.get(1).contains("alice"));
        // P2P 内部通道 headers
        assertEquals("secretpad.carol.svc", PROVIDER.lastHost(), "Host 应指向 provider 内部域名");
        assertEquals("kuscia-system", PROVIDER.lastOrigin(), "kuscia-origin-source 应为本节点");
        assertEquals(1, PROVIDER.requests().get());
        // 幂等：已 SYNCED 直接返回本地副本，不重复 HTTP
        Map<String, Object> again = assetSync.ensureSynced("p1", "ast-remote-proc");
        assertEquals("PHYSICAL", String.valueOf(again.get("syncMode")));
        assertEquals(1, PROVIDER.requests().get(), "幂等不应再次请求 provider");
        assertEquals(NodeDatasetStore.assetTableName(localId), assetSync.localPhysicalTable("p1", "ast-remote-proc"));
    }

    /** 2. requester RAW：绝不传真实行，仅 SCHEMA 同步记录；无本地资产、无 HTTP。 */
    @Test
    public void requesterRawSchemaOnly() {
        int before = PROVIDER.requests().get();
        Map<String, Object> result = assetSync.recordSchemaOnly("p1", "ast-remote-raw");
        assertEquals("SCHEMA", String.valueOf(result.get("syncMode")));
        Map<String, Object> rec = syncRecord("p1", "ast-remote-raw");
        assertEquals("SCHEMA", String.valueOf(rec.get("sync_mode")));
        assertEquals("SYNCED", String.valueOf(rec.get("status")));
        assertEquals("", String.valueOf(rec.get("local_asset_id")));
        assertEquals(0, count("select count(1) from ds_data_asset where source_asset_id='ast-remote-raw'"));
        assertEquals(0, count("select count(1) from ds_asset_sync_record where project_id='p1' "
                + "and asset_id='ast-remote-raw' and local_asset_id<>''"));
        assertEquals(before, PROVIDER.requests().get(), "RAW 绝不发起下载请求");
        assertNull(assetSync.localPhysicalTable("p1", "ast-remote-raw"));
        assertEquals("SCHEMA", String.valueOf(assetSync.ensureSynced("p1", "ast-remote-raw").get("syncMode")));
    }

    /** 3. 校验和失败：同步记录 FAILED，异常上抛。 */
    @Test
    public void shaMismatchFailsAndRecords() {
        PROVIDER.shaOverride().put("ast-remote-proc", "0000000000000000000000000000000000000000000000000000000000000000");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> assetSync.pullOnAuthorization("p1", "ast-remote-proc"));
        assertTrue(String.valueOf(e.getMessage()).contains("校验和不一致"));
        Map<String, Object> rec = syncRecord("p1", "ast-remote-proc");
        assertEquals("PHYSICAL", String.valueOf(rec.get("sync_mode")));
        assertEquals("FAILED", String.valueOf(rec.get("status")));
        assertEquals(0, count("select count(1) from ds_data_asset where source_asset_id='ast-remote-proc'"));
    }

    /** 4. 拉取后可重试成功（FAILED → SYNCED）。 */
    @Test
    public void retryAfterFailure() {
        PROVIDER.shaOverride().put("ast-remote-proc", "deadbeef");
        assertThrows(IllegalStateException.class, () -> assetSync.pullOnAuthorization("p1", "ast-remote-proc"));
        PROVIDER.shaOverride().remove("ast-remote-proc");
        Map<String, Object> result = assetSync.pullOnAuthorization("p1", "ast-remote-proc");
        assertNotNull(result);
        assertEquals("SYNCED", String.valueOf(syncRecord("p1", "ast-remote-proc").get("status")));
    }

    /** 5. provider download：PROCESSED+TABULAR 授权请求方可下载；内容与校验和齐备。 */
    @Test
    public void providerDownloadAuthorized() {
        AssetSyncService.AssetDownload dl = assetSync.download("ast-provided-proc", "carol");
        String csv = new String(dl.bytes(), StandardCharsets.UTF_8);
        List<List<String>> parsed = CsvUtil.parse(csv);
        assertEquals(4, parsed.size(), "表头 + 3 行");
        assertEquals(HEADER, parsed.get(0));
        assertEquals(ROWS, parsed.subList(1, parsed.size()));
        assertEquals(MockProvider.sha256(dl.bytes()), dl.sha256(), "校验和应等于内容 sha256");
    }

    /** 6. provider download：RAW 拒绝（仅抽样脱敏后可跨节点）。 */
    @Test
    public void providerDownloadRejectsRaw() {
        SecurityException e = assertThrows(SecurityException.class,
                () -> assetSync.download("ast-provided-raw", "carol"));
        assertTrue(String.valueOf(e.getMessage()).contains("仅抽样脱敏后的数据"));
    }

    /** 7. provider download：未授权请求方拒绝（非项目参与节点）。 */
    @Test
    public void providerDownloadRejectsUnauthorizedRequester() {
        SecurityException e = assertThrows(SecurityException.class,
                () -> assetSync.download("ast-provided-proc", "eve"));
        assertTrue(String.valueOf(e.getMessage()).contains("未获授权"));
    }

    /** 8. provider download：不存在的资产抛错。 */
    @Test
    public void providerDownloadRejectsUnknown() {
        assertThrows(NoSuchElementException.class, () -> assetSync.download("ast-ghost", "carol"));
    }

    /** 9. ensureSynced LOCAL 路径：本节点资产确保物化、不触发 HTTP。 */
    @Test
    public void ensureSyncedLocal() {
        int before = PROVIDER.requests().get();
        Map<String, Object> result = assetSync.ensureSynced("p1", "ast-local-proc");
        assertEquals("LOCAL", String.valueOf(result.get("syncMode")));
        assertEquals(before, PROVIDER.requests().get());
        assertTrue(SqliteTableLoader.tableExists(nodeStore.localNodeDbPath(),
                NodeDatasetStore.assetTableName("ast-local-proc")), "本节点资产应已物化");
    }

    /* ------------------------------- mock provider ------------------------------- */

    static class MockProvider {
        private final HttpServer server;
        private final Map<String, String> assets = new ConcurrentHashMap<>();
        private final Map<String, String> shaOverride = new ConcurrentHashMap<>();
        private final AtomicInteger requests = new AtomicInteger();
        private volatile String lastHost = "";
        private volatile String lastOrigin = "";

        MockProvider() {
            try {
                server = HttpServer.create(
                        new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
                server.createContext("/api/v1alpha1/data-assets/sync/download", this::handle);
                server.start();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        int port() {
            return server.getAddress().getPort();
        }

        Map<String, String> assets() {
            return assets;
        }

        Map<String, String> shaOverride() {
            return shaOverride;
        }

        AtomicInteger requests() {
            return requests;
        }

        String lastHost() {
            return lastHost;
        }

        String lastOrigin() {
            return lastOrigin;
        }

        void reset() {
            assets.clear();
            shaOverride.clear();
            requests.set(0);
            lastHost = "";
            lastOrigin = "";
        }

        void stop() {
            server.stop(0);
        }

        private void handle(HttpExchange exchange) throws IOException {
            try {
                requests.incrementAndGet();
                lastHost = exchange.getRequestHeaders().getFirst("Host");
                lastOrigin = exchange.getRequestHeaders().getFirst("kuscia-origin-source");
                String rawQuery = exchange.getRequestURI().getRawQuery();
                String assetId = null;
                if (rawQuery != null) {
                    for (String pair : rawQuery.split("&")) {
                        if (pair.startsWith("assetId=")) {
                            assetId = URLDecoder.decode(pair.substring("assetId=".length()), StandardCharsets.UTF_8);
                        }
                    }
                }
                String csv = assets.get(assetId);
                if (csv == null) {
                    exchange.sendResponseHeaders(404, -1);
                    return;
                }
                byte[] body = csv.getBytes(StandardCharsets.UTF_8);
                String sha = shaOverride.getOrDefault(assetId, sha256(body));
                exchange.getResponseHeaders().add("X-Asset-Sha256", sha);
                exchange.sendResponseHeaders(200, body.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(body);
                }
            } finally {
                exchange.close();
            }
        }

        static String sha256(byte[] bytes) {
            try {
                return HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
