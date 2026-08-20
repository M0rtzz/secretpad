/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.service.sandbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Collector tests: a real Prometheus endpoint served by a local {@link HttpServer} feeds
 * {@link ResourceCollector}, which must overwrite the single {@code ds_node_metric} row, compute
 * CPU/memory/storage/GPU correctly, mark the row STALE on endpoint failure and never crash.
 */
class ResourceCollectorTest {

    private static final String DB_FILE = System.getProperty("java.io.tmpdir")
            + "/ds-node-metric-test.sqlite";

    private static final String DDL = """
            create table if not exists ds_node_metric (
              id varchar(64) primary key,
              node_id varchar(128) not null,
              cpu_cores real not null default 0,
              cpu_usage_percent real not null default 0,
              memory_total_gb real not null default 0,
              memory_available_gb real not null default 0,
              memory_usage_percent real not null default 0,
              storage_total_gb real not null default 0,
              storage_available_gb real not null default 0,
              storage_usage_percent real not null default 0,
              gpu_utilization_percent real not null default -1,
              source varchar(32) not null default 'prometheus',
              status varchar(16) not null default 'FRESH',
              raw_json varchar(8192) default '',
              created_at varchar(32) not null
            );
            """;

    private JdbcTemplate jdbc;
    private ResourceCollector collector;
    private HttpServer server;
    private String metricsUrl;

    @BeforeEach
    void setUp() throws Exception {
        Files.deleteIfExists(Path.of(DB_FILE));
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                "jdbc:sqlite:" + DB_FILE, "", "");
        jdbc = new JdbcTemplate(dataSource);
        jdbc.execute(DDL);
        collector = new ResourceCollector(jdbc, new ObjectMapper());
        ReflectionTestUtils.setField(collector, "enabled", true);
        ReflectionTestUtils.setField(collector, "nodeId", "kuscia-system");
        ReflectionTestUtils.setField(collector, "intervalMs", 30000L);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (server != null) {
            server.stop(0);
        }
        Files.deleteIfExists(Path.of(DB_FILE));
    }

    private void startServer(String payload) throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/metrics", exchange -> {
            byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        metricsUrl = "http://127.0.0.1:" + server.getAddress().getPort();
        ReflectionTestUtils.setField(collector, "metricsUrl", metricsUrl);
    }

    private void startServer(AtomicInteger hits, String first, String second) throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/metrics", exchange -> {
            String payload = hits.incrementAndGet() == 1 ? first : second;
            byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        metricsUrl = "http://127.0.0.1:" + server.getAddress().getPort();
        ReflectionTestUtils.setField(collector, "metricsUrl", metricsUrl);
    }

    private static String nodePayload(double idle0, double user0, double idle1, double user1) {
        return """
                # TYPE node_cpu_seconds_total counter
                node_cpu_seconds_total{cpu="0",mode="idle"} %s
                node_cpu_seconds_total{cpu="0",mode="user"} %s
                node_cpu_seconds_total{cpu="1",mode="idle"} %s
                node_cpu_seconds_total{cpu="1",mode="user"} %s
                node_memory_MemTotal_bytes 10737418240
                node_memory_MemAvailable_bytes 5368709120
                node_filesystem_size_bytes{device="/dev/sda1",fstype="ext4",mountpoint="/"} 107374182400
                node_filesystem_avail_bytes{device="/dev/sda1",fstype="ext4",mountpoint="/"} 53687091200
                node_filesystem_size_bytes{device="overlay",fstype="overlay",mountpoint="/var/lib/docker/overlay2/x"} 966367641600
                node_filesystem_avail_bytes{device="overlay",fstype="overlay",mountpoint="/var/lib/docker/overlay2/x"} 536870912000
                DCGM_FI_DEV_GPU_UTIL{gpu="0"} 42.5
                DCGM_FI_DEV_GPU_UTIL{gpu="1"} 57.5
                """.formatted(idle0, user0, idle1, user1);
    }

    @Test
    void firstCollectionPersistsFreshRowWithComputedValues() throws Exception {
        startServer(nodePayload(90, 10, 190, 10));
        assertTrue(collector.collectNow());

        Map<String, Object> row = jdbc.queryForMap("select * from ds_node_metric");
        assertEquals("kuscia-system", row.get("node_id"));
        assertEquals("FRESH", row.get("status"));
        assertEquals(2d, ((Number) row.get("cpu_cores")).doubleValue(), 1e-9);
        // 第一次采集无差量，使用率为 -1（前端显示 N/A）
        assertEquals(-1d, ((Number) row.get("cpu_usage_percent")).doubleValue(), 1e-9);
        // 内存 10GiB 总量、5GiB 可用 → 50%
        assertEquals(10d, ((Number) row.get("memory_total_gb")).doubleValue(), 1e-9);
        assertEquals(50d, ((Number) row.get("memory_usage_percent")).doubleValue(), 1e-9);
        // 存储取最大真实挂载点（/dev/sda1，overlay 被过滤）→ 100GiB / 50GiB / 50%
        assertEquals(100d, ((Number) row.get("storage_total_gb")).doubleValue(), 1e-9);
        assertEquals(50d, ((Number) row.get("storage_usage_percent")).doubleValue(), 1e-9);
        // GPU 利用率 = (42.5+57.5)/2
        assertEquals(50d, ((Number) row.get("gpu_utilization_percent")).doubleValue(), 1e-9);
    }

    @Test
    void secondCollectionComputesCpuDeltaUsageAndOverwritesRow() throws Exception {
        AtomicInteger hits = new AtomicInteger(0);
        startServer(hits, nodePayload(90, 10, 190, 10), nodePayload(95, 15, 200, 10));
        assertTrue(collector.collectNow());
        assertTrue(collector.collectNow());

        Map<String, Object> row = jdbc.queryForMap("select * from ds_node_metric");
        // Δidle=15, Δtotal=20 → (1 - 15/20)*100 = 25%
        assertEquals(25d, ((Number) row.get("cpu_usage_percent")).doubleValue(), 1e-9);
        // 覆盖写入：仍然只有一行
        assertEquals(1L, jdbc.queryForObject("select count(1) from ds_node_metric", Long.class));
    }

    @Test
    void endpointFailureMarksStaleAndKeepsLastRow() throws Exception {
        startServer(nodePayload(90, 10, 190, 10));
        assertTrue(collector.collectNow());
        double memoryTotal = ((Number) jdbc.queryForMap("select * from ds_node_metric").get("memory_total_gb")).doubleValue();

        server.stop(0);
        server = null;
        ReflectionTestUtils.setField(collector, "metricsUrl", "http://127.0.0.1:9");

        assertFalse(collector.collectNow());
        Map<String, Object> row = jdbc.queryForMap("select * from ds_node_metric");
        assertEquals("STALE", row.get("status"));
        // 上次有效值保留
        assertEquals(memoryTotal, ((Number) row.get("memory_total_gb")).doubleValue(), 1e-9);
    }

    @Test
    void disabledCollectorIsNoOp() throws Exception {
        startServer(nodePayload(90, 10, 190, 10));
        ReflectionTestUtils.setField(collector, "enabled", false);
        assertFalse(collector.collectNow());
        assertEquals(0L, jdbc.queryForObject("select count(1) from ds_node_metric", Long.class));
    }

    @Test
    void noGpuSeriesYieldsNadashOne() throws Exception {
        String payload = """
                node_cpu_seconds_total{cpu="0",mode="idle"} 100
                node_memory_MemTotal_bytes 10737418240
                node_memory_MemAvailable_bytes 5368709120
                node_filesystem_size_bytes{device="/dev/sda1",mountpoint="/"} 107374182400
                node_filesystem_avail_bytes{device="/dev/sda1",mountpoint="/"} 53687091200
                """;
        startServer(payload);
        assertTrue(collector.collectNow());
        assertEquals(-1d, ((Number) jdbc.queryForMap("select * from ds_node_metric").get("gpu_utilization_percent")).doubleValue(), 1e-9);
    }
}
