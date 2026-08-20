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
import lombok.extern.slf4j.Slf4j;
import org.secretflow.secretpad.web.service.sandbox.PrometheusTextParser.Sample;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Periodic collector of real node resource metrics from the Kuscia Prometheus endpoint
 * ({@code http://<kuscia>:9091/metrics}, node_exporter / DCGM exporter).
 *
 * <p>Design rules:</p>
 * <ul>
 *   <li>Every successful collection <em>overwrites</em> the single {@code ds_node_metric} row for
 *       this node (last valid sample wins).</li>
 *   <li>A failed fetch/parse never crashes the scheduler and never drops the last valid row: the
 *       existing row is simply marked {@code STALE} (its values remain readable).</li>
 *   <li>GPU utilization is {@code -1} (rendered as N/A) when no DCGM metrics are exposed — this
 *       environment only keeps a GPU ledger/quota, there is no container GPU passthrough.</li>
 * </ul>
 */
@Slf4j
@Component
public class ResourceCollector {

    /** Mount points to ignore when aggregating storage (k3s/containerd/runtime internals). */
    static final List<String> IGNORED_MOUNT_PREFIXES = List.of(
            "/var/lib/docker", "/var/lib/containerd", "/var/lib/kubelet", "/var/lib/rancher",
            "/var/lib/cni", "/run", "/proc", "/sys", "/dev", "/etc", "/boot", "/tmp");

    static final List<String> IGNORED_DEVICES = List.of(
            "overlay", "tmpfs", "shm", "devtmpfs", "proc", "sysfs", "cgroup", "devpts", "mqueue");

    private static final double BYTES_PER_GB = 1024d * 1024d * 1024d;

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @Value("${secretpad.node-id:kuscia-system}")
    private String nodeId;

    @Value("${secretpad.data-sandbox.metrics.enabled:false}")
    private boolean enabled;

    @Value("${secretpad.data-sandbox.metrics.url:}")
    private String metricsUrl;

    @Value("${secretpad.data-sandbox.metrics.interval-ms:30000}")
    private long intervalMs;

    /** Previous CPU tick for the idle/total delta usage calculation (single-node collector). */
    private volatile double previousCpuIdle = -1;
    private volatile double previousCpuTotal = -1;

    public ResourceCollector(@Qualifier("jdbcTemplate") JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedDelayString = "${secretpad.data-sandbox.metrics.interval-ms:30000}")
    public void collect() {
        if (!enabled || metricsUrl == null || metricsUrl.isBlank()) {
            return;
        }
        collectNow();
    }

    /**
     * Run one collection cycle immediately. Returns {@code true} when a FRESH row was persisted,
     * {@code false} when the endpoint was unavailable (row marked STALE) or metrics were disabled.
     * Exposed for tests and manual refresh.
     */
    public boolean collectNow() {
        if (!enabled || metricsUrl == null || metricsUrl.isBlank()) {
            return false;
        }
        String body;
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(metricsUrl + "/metrics"))
                    .timeout(Duration.ofSeconds(8))
                    .header("Accept", "text/plain")
                    .GET().build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                markStale("metrics endpoint returned HTTP " + response.statusCode());
                return false;
            }
            body = response.body();
        } catch (Exception e) {
            markStale("metrics fetch failed: " + truncate(e.getMessage(), 240));
            return false;
        }
        Map<String, List<Sample>> metrics = PrometheusTextParser.parse(body);
        if (metrics.isEmpty()) {
            markStale("metrics payload contained no samples");
            return false;
        }
        return persist(compute(metrics));
    }

    /** Compute node metrics from parsed samples; never throws for missing series. */
    Map<String, Object> compute(Map<String, List<Sample>> metrics) {
        double cores = cpuCores(metrics);
        double cpuUsage = cpuUsagePercent(metrics);
        double memoryTotal = bytesToGb(sumSamples(metrics.get("node_memory_MemTotal_bytes")));
        double memoryAvailable = bytesToGb(sumSamples(metrics.get("node_memory_MemAvailable_bytes")));
        double memoryUsage = memoryTotal <= 0 ? 0 : Math.round((1 - memoryAvailable / memoryTotal) * 10000) / 100d;
        double[] storage = storageUsage(metrics);
        double gpu = gpuUtilization(metrics);

        Map<String, Object> row = new HashMap<>();
        row.put("node_id", nodeId);
        row.put("cpu_cores", cores);
        row.put("cpu_usage_percent", cpuUsage);
        row.put("memory_total_gb", Math.round(memoryTotal * 100) / 100d);
        row.put("memory_available_gb", Math.round(memoryAvailable * 100) / 100d);
        row.put("memory_usage_percent", memoryUsage);
        row.put("storage_total_gb", Math.round(storage[0] * 100) / 100d);
        row.put("storage_available_gb", Math.round(storage[1] * 100) / 100d);
        row.put("storage_usage_percent", storage[0] <= 0 ? 0 : Math.round((1 - storage[1] / storage[0]) * 10000) / 100d);
        row.put("gpu_utilization_percent", gpu);
        row.put("source", "prometheus");
        row.put("status", "FRESH");
        row.put("raw_json", json(Map.of("cpu_cores", cores, "cpu_usage_percent", cpuUsage,
                "memory_total_gb", row.get("memory_total_gb"), "memory_available_gb", row.get("memory_available_gb"),
                "storage_total_gb", row.get("storage_total_gb"), "storage_available_gb", row.get("storage_available_gb"),
                "gpu_utilization_percent", gpu)));
        return row;
    }

    private boolean persist(Map<String, Object> row) {
        try {
            Long existing = jdbc.queryForObject(
                    "select count(1) from ds_node_metric where node_id=?", Long.class, nodeId);
            if (existing != null && existing > 0) {
                jdbc.update("update ds_node_metric set cpu_cores=?,cpu_usage_percent=?,memory_total_gb=?,memory_available_gb=?,"
                                + "memory_usage_percent=?,storage_total_gb=?,storage_available_gb=?,storage_usage_percent=?,"
                                + "gpu_utilization_percent=?,source=?,status=?,raw_json=?,created_at=? where node_id=?",
                        row.get("cpu_cores"), row.get("cpu_usage_percent"), row.get("memory_total_gb"),
                        row.get("memory_available_gb"), row.get("memory_usage_percent"), row.get("storage_total_gb"),
                        row.get("storage_available_gb"), row.get("storage_usage_percent"), row.get("gpu_utilization_percent"),
                        row.get("source"), row.get("status"), row.get("raw_json"), now(), nodeId);
            } else {
                jdbc.update("insert into ds_node_metric(id,node_id,cpu_cores,cpu_usage_percent,memory_total_gb,memory_available_gb,"
                                + "memory_usage_percent,storage_total_gb,storage_available_gb,storage_usage_percent,"
                                + "gpu_utilization_percent,source,status,raw_json,created_at) "
                                + "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        "node-metric-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12), nodeId,
                        row.get("cpu_cores"), row.get("cpu_usage_percent"), row.get("memory_total_gb"),
                        row.get("memory_available_gb"), row.get("memory_usage_percent"), row.get("storage_total_gb"),
                        row.get("storage_available_gb"), row.get("storage_usage_percent"), row.get("gpu_utilization_percent"),
                        row.get("source"), row.get("status"), row.get("raw_json"), now());
            }
            return true;
        } catch (Exception e) {
            log.warn("Unable to persist node metrics: {}", e.getMessage());
            return false;
        }
    }

    private void markStale(String reason) {
        try {
            int updated = jdbc.update("update ds_node_metric set status='STALE',raw_json=? where node_id=?",
                    json(Map.of("error", reason)), nodeId);
            log.warn("Node metrics stale ({}), last valid row kept{}", reason,
                    updated > 0 ? "" : " (no previous row)");
        } catch (Exception e) {
            log.warn("Unable to mark node metrics stale: {}", e.getMessage());
        }
    }

    /* ------------------------------- compute helpers ------------------------------- */

    /** CPU core count = distinct {@code cpu} labels in {@code node_cpu_seconds_total}. */
    private double cpuCores(Map<String, List<Sample>> metrics) {
        List<Sample> samples = metrics.get("node_cpu_seconds_total");
        if (samples == null || samples.isEmpty()) {
            return 0;
        }
        Set<String> cpus = new HashSet<>();
        for (Sample sample : samples) {
            cpus.add(sample.labels().getOrDefault("cpu", ""));
        }
        return cpus.size();
    }

    /** CPU usage = 1 - Δidle / Δtotal across two collection ticks (first tick returns -1). */
    private double cpuUsagePercent(Map<String, List<Sample>> metrics) {
        double idle = sumByLabel(metrics.get("node_cpu_seconds_total"), "mode", "idle");
        double total = sumSamples(metrics.get("node_cpu_seconds_total"));
        if (total <= 0) {
            return -1;
        }
        if (previousCpuTotal < 0 || previousCpuIdle < 0 || total <= previousCpuTotal) {
            previousCpuIdle = idle;
            previousCpuTotal = total;
            return -1;
        }
        double deltaIdle = idle - previousCpuIdle;
        double deltaTotal = total - previousCpuTotal;
        previousCpuIdle = idle;
        previousCpuTotal = total;
        if (deltaTotal <= 0) {
            return -1;
        }
        double usage = (1 - deltaIdle / deltaTotal) * 100;
        return Math.round(Math.max(0, Math.min(100, usage)) * 100) / 100d;
    }

    /**
     * Storage usage over the largest non-virtual mount point: total and available GB.
     * Virtual/runtime filesystems are excluded to avoid k3s/containerd overlay noise.
     */
    private double[] storageUsage(Map<String, List<Sample>> metrics) {
        Map<String, Double> sizes = new HashMap<>();
        Map<String, Double> avail = new HashMap<>();
        for (Sample sample : samples(metrics.get("node_filesystem_size_bytes"))) {
            if (isIgnoredMount(sample.labels())) {
                continue;
            }
            sizes.merge(mountKey(sample.labels()), sample.value(), Double::sum);
        }
        for (Sample sample : samples(metrics.get("node_filesystem_avail_bytes"))) {
            if (isIgnoredMount(sample.labels())) {
                continue;
            }
            avail.merge(mountKey(sample.labels()), sample.value(), Double::sum);
        }
        String largest = null;
        double maxSize = 0;
        for (Map.Entry<String, Double> entry : sizes.entrySet()) {
            if (entry.getValue() > maxSize) {
                maxSize = entry.getValue();
                largest = entry.getKey();
            }
        }
        if (largest == null) {
            return new double[]{0, 0};
        }
        return new double[]{bytesToGb(maxSize), bytesToGb(avail.getOrDefault(largest, 0d))};
    }

    /** GPU utilization: average of DCGM util samples, or -1 when not exposed. */
    private double gpuUtilization(Map<String, List<Sample>> metrics) {
        List<Sample> samples = metrics.get("DCGM_FI_DEV_GPU_UTIL");
        if (samples == null || samples.isEmpty()) {
            return -1;
        }
        double total = 0;
        int count = 0;
        for (Sample sample : samples) {
            if (sample.value() >= 0 && sample.value() <= 100) {
                total += sample.value();
                count++;
            }
        }
        return count == 0 ? -1 : Math.round(total / count * 100) / 100d;
    }

    private static boolean isIgnoredMount(Map<String, String> labels) {
        String device = labels.getOrDefault("device", "");
        String fstype = labels.getOrDefault("fstype", "");
        String mountpoint = labels.getOrDefault("mountpoint", "");
        for (String ignored : IGNORED_DEVICES) {
            if (device.equals(ignored) || fstype.equals(ignored)) {
                return true;
            }
        }
        for (String prefix : IGNORED_MOUNT_PREFIXES) {
            if (mountpoint.startsWith(prefix)) {
                return true;
            }
        }
        return mountpoint.isEmpty();
    }

    private static String mountKey(Map<String, String> labels) {
        return labels.getOrDefault("device", labels.getOrDefault("mountpoint", ""));
    }

    private static double sumSamples(List<Sample> samples) {
        double total = 0;
        for (Sample sample : samples(samples)) {
            total += sample.value();
        }
        return total;
    }

    private static double sumByLabel(List<Sample> samples, String label, String value) {
        double total = 0;
        for (Sample sample : samples(samples)) {
            if (value.equals(sample.labels().get(label))) {
                total += sample.value();
            }
        }
        return total;
    }

    private static List<Sample> samples(List<Sample> samples) {
        return samples == null ? List.of() : samples;
    }

    private static double bytesToGb(double bytes) {
        return bytes / BYTES_PER_GB;
    }

    private String json(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            return String.valueOf(value);
        }
    }

    private static String now() {
        return LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS).toString();
    }

    private static String truncate(String value, int max) {
        if (value == null) {
            return "";
        }
        return value.length() <= max ? value : value.substring(0, max);
    }
}
