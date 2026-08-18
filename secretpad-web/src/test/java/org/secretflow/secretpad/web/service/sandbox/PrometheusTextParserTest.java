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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the tolerant Prometheus text parser (node_exporter / DCGM style).
 */
class PrometheusTextParserTest {

    @Test
    void parsesNodeExporterStylePayload() {
        String payload = """
                # HELP node_cpu_seconds_total Seconds the cpus spent in each mode.
                # TYPE node_cpu_seconds_total counter
                node_cpu_seconds_total{cpu="0",mode="idle"} 1234.56
                node_cpu_seconds_total{cpu="0",mode="user"} 12.5
                node_cpu_seconds_total{cpu="1",mode="idle"} 5678.9
                node_cpu_seconds_total{cpu="1",mode="system"} 3.25
                node_memory_MemTotal_bytes 33554432000
                node_memory_MemAvailable_bytes 25165824000
                node_filesystem_size_bytes{device="/dev/sda1",fstype="ext4",mountpoint="/"} 102400000000
                node_filesystem_avail_bytes{device="/dev/sda1",fstype="ext4",mountpoint="/"} 51200000000
                node_filesystem_size_bytes{device="overlay",fstype="overlay",mountpoint="/var/lib/docker/overlay2/x"} 900000000000
                DCGM_FI_DEV_GPU_UTIL{gpu="0"} 42.5
                DCGM_FI_DEV_GPU_UTIL{gpu="1"} 57.5
                """;

        Map<String, List<PrometheusTextParser.Sample>> metrics = PrometheusTextParser.parse(payload);

        List<PrometheusTextParser.Sample> cpu = metrics.get("node_cpu_seconds_total");
        assertNotNull(cpu);
        assertEquals(4, cpu.size());
        assertEquals(1234.56, cpu.get(0).value(), 1e-9);
        assertEquals("0", cpu.get(0).labels().get("cpu"));
        assertEquals("idle", cpu.get(0).labels().get("mode"));

        assertEquals(33554432000d, metrics.get("node_memory_MemTotal_bytes").get(0).value(), 1e-9);
        assertEquals(2, metrics.get("node_filesystem_size_bytes").size());
        assertEquals(2, metrics.get("DCGM_FI_DEV_GPU_UTIL").size());
    }

    @Test
    void skipsCommentsBlankLinesAndNanSamples() {
        String payload = """
                # TYPE go_goroutines gauge
                go_goroutines 12

                some_counter{a="1"} NaN
                some_counter{a="2"} 7
                bad_line_no_value
                metric_after_timestamp 3.14 1720000000000
                """;
        Map<String, List<PrometheusTextParser.Sample>> metrics = PrometheusTextParser.parse(payload);
        assertEquals(12d, metrics.get("go_goroutines").get(0).value(), 1e-9);
        // NaN 样本被忽略，只保留有效样本
        assertEquals(1, metrics.get("some_counter").size());
        assertEquals(7d, metrics.get("some_counter").get(0).value(), 1e-9);
        // 带时间戳的样本取第一个 token 作为值
        assertEquals(3.14, metrics.get("metric_after_timestamp").get(0).value(), 1e-9);
    }

    @Test
    void handlesEscapedLabelValuesAndInfiniteValues() {
        String payload = """
                foo{label="a\\"quoted\\"value"} 1
                bar 5
                baz 42
                baz +Inf
                """;
        Map<String, List<PrometheusTextParser.Sample>> metrics = PrometheusTextParser.parse(payload);
        assertEquals("a\"quoted\"value", metrics.get("foo").get(0).labels().get("label"));
        assertEquals(Double.POSITIVE_INFINITY, metrics.get("baz").get(1).value(), 1e-9);
    }

    @Test
    void blankPayloadReturnsEmptyMap() {
        assertTrue(PrometheusTextParser.parse(null).isEmpty());
        assertTrue(PrometheusTextParser.parse("").isEmpty());
        assertTrue(PrometheusTextParser.parse("# only a comment\n\n").isEmpty());
    }

    @Test
    void mergeZeroAmountGpuBackfillIsIdempotent() {
        // 回归：GPU=0 的沙箱回填时不生成 GPU 分配行（由迁移 SQL 保证），解析器不做任何特殊处理
        Map<String, List<PrometheusTextParser.Sample>> metrics = PrometheusTextParser.parse("gpu 0");
        assertEquals(0d, metrics.get("gpu").get(0).value(), 1e-9);
    }
}
