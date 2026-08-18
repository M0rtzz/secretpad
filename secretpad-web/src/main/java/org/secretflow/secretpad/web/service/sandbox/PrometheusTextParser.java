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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal tolerant parser for the Prometheus text exposition format
 * (as served by node_exporter / DCGM exporter on the Kuscia metrics endpoint).
 *
 * <p>The parser is deliberately lenient: HELP/TYPE comments, blank lines, NaN samples and
 * malformed lines are skipped instead of failing, so a drifted exporter never crashes the
 * collector. Only flat families with label sets are expected (no OpenMetrics histogram/bucket
 * support); histogram/bucket lines parse as ordinary samples and are simply ignored by callers.</p>
 */
public final class PrometheusTextParser {

    private PrometheusTextParser() {
    }

    /** One sample: metric name, label map and numeric value. */
    public record Sample(String name, Map<String, String> labels, double value) {
    }

    /**
     * Parse Prometheus text into {@code metric name -> samples}. Never returns {@code null};
     * a blank or fully-invalid payload yields an empty map.
     */
    public static Map<String, List<Sample>> parse(String text) {
        Map<String, List<Sample>> result = new LinkedHashMap<>();
        if (text == null || text.isBlank()) {
            return result;
        }
        for (String line : text.split("\\R")) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                continue;
            }
            Sample sample = parseLine(trimmed);
            if (sample != null) {
                result.computeIfAbsent(sample.name(), key -> new ArrayList<>()).add(sample);
            }
        }
        return result;
    }

    private static Sample parseLine(String line) {
        int brace = line.indexOf('{');
        String name;
        Map<String, String> labels = new LinkedHashMap<>();
        int valueStart;
        if (brace >= 0) {
            name = line.substring(0, brace).trim();
            int close = line.indexOf('}', brace);
            if (close < 0) {
                return null;
            }
            parseLabels(line.substring(brace + 1, close), labels);
            valueStart = line.indexOf(' ', close);
        } else {
            int space = line.indexOf(' ');
            if (space < 0) {
                return null;
            }
            name = line.substring(0, space).trim();
            valueStart = space;
        }
        if (name.isEmpty() || valueStart < 0 || valueStart >= line.length() - 1) {
            return null;
        }
        // 值之后可能还有采样时间戳（"value timestamp"），只取第一个 token
        String valueToken = line.substring(valueStart + 1).trim().split("\\s+")[0];
        double value;
        try {
            if ("NaN".equalsIgnoreCase(valueToken)) {
                return null;
            }
            if ("+Inf".equals(valueToken) || "Inf".equals(valueToken)) {
                value = Double.POSITIVE_INFINITY;
            } else if ("-Inf".equals(valueToken)) {
                value = Double.NEGATIVE_INFINITY;
            } else {
                value = Double.parseDouble(valueToken);
            }
        } catch (NumberFormatException e) {
            return null;
        }
        return new Sample(name, labels, value);
    }

    /**
     * Parse the label section between the braces, e.g. {@code cpu="0",mode="idle"}. Values may
     * contain escaped quotes ({@code \"}) and backslashes ({@code \\}); unquoted values are
     * tolerated as a fallback.
     */
    private static void parseLabels(String raw, Map<String, String> out) {
        int i = 0;
        int length = raw.length();
        while (i < length) {
            int eq = raw.indexOf('=', i);
            if (eq < 0) {
                break;
            }
            String key = raw.substring(i, eq).trim();
            i = eq + 1;
            while (i < length && (raw.charAt(i) == ' ' || raw.charAt(i) == '\t')) {
                i++;
            }
            if (i >= length) {
                break;
            }
            String value;
            if (raw.charAt(i) == '"') {
                StringBuilder sb = new StringBuilder();
                i++;
                while (i < length) {
                    char c = raw.charAt(i);
                    if (c == '\\' && i + 1 < length) {
                        char next = raw.charAt(i + 1);
                        if (next == '"' || next == '\\') {
                            sb.append(next);
                            i += 2;
                            continue;
                        }
                        sb.append(c);
                        i++;
                        continue;
                    }
                    if (c == '"') {
                        i++;
                        break;
                    }
                    sb.append(c);
                    i++;
                }
                value = sb.toString();
            } else {
                int comma = raw.indexOf(',', i);
                int end = comma < 0 ? length : comma;
                value = raw.substring(i, end).trim();
                i = end;
            }
            if (!key.isEmpty()) {
                out.put(key, value);
            }
            while (i < length && raw.charAt(i) == ',') {
                i++;
            }
        }
    }
}
