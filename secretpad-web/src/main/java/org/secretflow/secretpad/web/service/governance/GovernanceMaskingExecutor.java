/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.service.governance;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pure, in-process column desensitization executor for the Z-04 governance engine.
 *
 * <p>Stateless and dependency-free. {@link #apply} runs the given {@link MaskRule}s against the
 * header + rows and returns a new header/row pair. A {@code CLEAR} rule with
 * {@code params.mode = "drop"} removes the column entirely (清除列); every other rule keeps the
 * column and rewrites its cells. Unknown columns in rules are ignored. Input is never mutated.</p>
 *
 * <p>Rule parameter keys (all optional, defaults in parentheses):</p>
 * <ul>
 *   <li>MASK — {@code keepLeft} (3), {@code keepRight} (4), {@code maskChar} ("*"); values
 *       shorter than {@code keepLeft + keepRight} are returned untouched.</li>
 *   <li>REPLACE — {@code value} constant, or {@code mapping} (JSON object of exact match).</li>
 *   <li>HASH — {@code salt} (""); SHA-256 hex of {@code salt + value}.</li>
 *   <li>ROUND — {@code digits} (0, HALF_UP); non-numeric cells pass through.</li>
 *   <li>CLEAR — {@code mode} "drop" removes the column, otherwise cells become empty.</li>
 * </ul>
 */
public final class GovernanceMaskingExecutor {

    private GovernanceMaskingExecutor() {
    }

    /** One column-level masking rule. {@code params} is a JSON object of key/value strings. */
    public record MaskRule(String column, String method, Map<String, String> params) {

        public static MaskRule of(String column, String method) {
            return new MaskRule(column, method, Map.of());
        }
    }

    /** Result of masking: possibly filtered header plus the rewritten rows. */
    public record MaskResult(List<String> header, List<List<String>> rows) {
    }

    /** Apply rules to {@code rows}; returns a new header/rows pair, never mutates input. */
    public static MaskResult apply(List<String> header, List<List<String>> rows,
            List<MaskRule> rules) {
        List<String> safeHeader = header == null ? new ArrayList<>() : new ArrayList<>(header);
        List<List<String>> safeRows = rows == null ? new ArrayList<>() : new ArrayList<>(rows);
        if (rules == null || rules.isEmpty()) {
            return new MaskResult(safeHeader, safeRows);
        }
        Map<String, Integer> columnIndex = new HashMap<>();
        for (int i = 0; i < safeHeader.size(); i++) {
            columnIndex.put(safeHeader.get(i), i);
        }
        List<MaskRule> applicable = rules.stream()
                .filter(r -> r != null && r.column() != null && columnIndex.containsKey(r.column()))
                .toList();
        if (applicable.isEmpty()) {
            return new MaskResult(safeHeader, safeRows);
        }
        Set<String> dropped = new LinkedHashSet<>();
        for (MaskRule rule : applicable) {
            if (isDrop(rule)) {
                dropped.add(rule.column());
            }
        }
        List<String> newHeader = new ArrayList<>(safeHeader.size());
        for (String col : safeHeader) {
            if (!dropped.contains(col)) {
                newHeader.add(col);
            }
        }
        List<List<String>> newRows = new ArrayList<>(safeRows.size());
        for (List<String> row : safeRows) {
            List<String> rewritten = new ArrayList<>(row);
            for (MaskRule rule : applicable) {
                int idx = columnIndex.get(rule.column());
                String value = idx < rewritten.size() ? rewritten.get(idx) : "";
                rewritten.set(idx, rewrite(rule.method(), value, rule.params()));
            }
            if (dropped.isEmpty()) {
                newRows.add(rewritten);
            } else {
                List<String> filtered = new ArrayList<>(newHeader.size());
                for (int i = 0; i < safeHeader.size(); i++) {
                    if (!dropped.contains(safeHeader.get(i))) {
                        filtered.add(rewritten.get(i));
                    }
                }
                newRows.add(filtered);
            }
        }
        return new MaskResult(newHeader, newRows);
    }

    private static String rewrite(String method, String value, Map<String, String> params) {
        if (value == null) {
            value = "";
        }
        String upper = method == null ? "" : method.trim().toUpperCase(java.util.Locale.ROOT);
        switch (upper) {
            case "MASK" -> {
                return mask(value, params);
            }
            case "REPLACE" -> {
                return replace(value, params);
            }
            case "HASH" -> {
                return hash(value, params);
            }
            case "ROUND" -> {
                return round(value, params);
            }
            case "CLEAR" -> {
                return "";
            }
            default -> throw new IllegalArgumentException("unknown masking method: " + method);
        }
    }

    private static boolean isDrop(MaskRule rule) {
        String mode = rule.params() == null ? null : rule.params().get("mode");
        return "CLEAR".equalsIgnoreCase(rule.method()) && "drop".equalsIgnoreCase(mode);
    }

    private static String mask(String value, Map<String, String> params) {
        if (value.isEmpty()) {
            return value;
        }
        int keepLeft = intParam(params, "keepLeft", 3);
        int keepRight = intParam(params, "keepRight", 4);
        String maskChar = params == null ? "*" : params.getOrDefault("maskChar", "*");
        if (maskChar.isEmpty()) {
            maskChar = "*";
        }
        if (value.length() <= keepLeft + keepRight) {
            return value;
        }
        String prefix = value.substring(0, keepLeft);
        String suffix = value.substring(value.length() - keepRight);
        return prefix + maskChar.repeat(value.length() - keepLeft - keepRight) + suffix;
    }

    private static String replace(String value, Map<String, String> params) {
        if (params != null && params.get("mapping") != null) {
            String mappingJson = params.get("mapping");
            String mapped = jsonMapping(mappingJson).get(value);
            return mapped != null ? mapped : value;
        }
        return params == null ? "" : params.getOrDefault("value", "");
    }

    private static String hash(String value, Map<String, String> params) {
        String salt = params == null ? "" : params.getOrDefault("salt", "");
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest((salt + value).getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                sb.append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static String round(String value, Map<String, String> params) {
        if (value.isEmpty()) {
            return value;
        }
        int digits = intParam(params, "digits", 0);
        try {
            BigDecimal decimal = new BigDecimal(value.trim());
            return decimal.setScale(digits, RoundingMode.HALF_UP).toPlainString();
        } catch (NumberFormatException e) {
            return value;
        }
    }

    private static int intParam(Map<String, String> params, String key, int defaultValue) {
        if (params == null) {
            return defaultValue;
        }
        String raw = params.get(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /** Minimal JSON object parser for the REPLACE mapping parameter. */
    private static Map<String, String> jsonMapping(String json) {
        Map<String, String> mapping = new HashMap<>();
        String trimmed = json.trim();
        if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) {
            return mapping;
        }
        String body = trimmed.substring(1, trimmed.length() - 1);
        if (body.isBlank()) {
            return mapping;
        }
        for (String entry : body.split(",")) {
            int colon = entry.indexOf(':');
            if (colon < 0) {
                continue;
            }
            String key = unquote(entry.substring(0, colon).trim());
            String value = unquote(entry.substring(colon + 1).trim());
            mapping.put(key, value);
        }
        return mapping;
    }

    private static String unquote(String token) {
        if (token.length() >= 2 && token.startsWith("\"") && token.endsWith("\"")) {
            return token.substring(1, token.length() - 1).replace("\\\"", "\"").replace("\\\\", "\\");
        }
        return token;
    }
}
