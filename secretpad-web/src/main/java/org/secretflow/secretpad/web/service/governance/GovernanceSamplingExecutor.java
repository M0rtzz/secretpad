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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Pure, in-process sampling executor for the Z-04 governance engine.
 *
 * <p>Stateless and dependency-free: it maps an input header + rows + {@link SamplingParams}
 * onto an output row subset. Column names are resolved against {@code header} once, then all
 * grouping uses integer indexes. All randomness is seeded ({@link SamplingParams#seed()}) so a
 * given task is reproducible. Data never leaves the JVM here; the caller owns persistence,
 * permission checks and audit.</p>
 *
 * <p>Selection semantics (rows keep their original relative order):</p>
 * <ul>
 *   <li>RANDOM — pick {@code count} rows, or {@code ceil(n*ratio)} when only ratio is set.</li>
 *   <li>SYSTEMATIC — target {@code k}, step = {@code n/k}, take index {@code offset + j*step}.</li>
 *   <li>STRATIFIED — group by {@code strataColumns}; ratio: ceil(group*ratio) each;
 *       count: take up to {@code count} rows independently from every non-empty group.</li>
 *   <li>CLUSTER — whole clusters: group by {@code clusterColumn} values, or consecutive
 *       blocks of {@code blockSize}; pick {@code count} clusters or a {@code ratio} of them
 *       and include every row of the selected clusters.</li>
 * </ul>
 */
public final class GovernanceSamplingExecutor {

    private GovernanceSamplingExecutor() {
    }

    /** Parsed sampling parameters. All fields nullable; defaults applied by the executor. */
    public record SamplingParams(
            String method,
            Long count,
            Double ratio,
            List<String> strataColumns,
            String clusterColumn,
            Integer blockSize,
            Long seed,
            Integer limit) {

        public static SamplingParams of(String method) {
            return new SamplingParams(method, null, null, null, null, null, null, null);
        }
    }

    /** Sample {@code rows} per {@code params}; returns a new row list, never mutates input. */
    public static List<List<String>> sample(List<String> header, List<List<String>> rows,
            SamplingParams params) {
        if (rows == null || rows.isEmpty() || params == null || params.method() == null
                || params.method().isBlank()) {
            return rows == null ? new ArrayList<>() : new ArrayList<>(rows);
        }
        int n = rows.size();
        String method = params.method().trim().toUpperCase(Locale.ROOT);
        Random random = new Random(seedOf(params));
        List<List<String>> selected;
        switch (method) {
            case "RANDOM" -> selected = randomSample(rows, params, n, random);
            case "SYSTEMATIC" -> selected = systematicSample(rows, params, n, random);
            case "STRATIFIED" -> selected = stratifiedSample(header, rows, params, n, random);
            case "CLUSTER" -> selected = clusterSample(header, rows, params, n, random);
            default -> throw new IllegalArgumentException("unknown sampling method: " + method);
        }
        if (params.limit() != null && params.limit() > 0 && selected.size() > params.limit()) {
            return new ArrayList<>(selected.subList(0, params.limit()));
        }
        return selected;
    }

    private static List<List<String>> randomSample(List<List<String>> rows, SamplingParams p,
            int n, Random random) {
        int target = targetCount(n, p);
        if (target >= n) {
            return new ArrayList<>(rows);
        }
        List<Integer> indices = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            indices.add(i);
        }
        Collections.shuffle(indices, random);
        List<Integer> picked = indices.subList(0, target).stream().sorted().toList();
        return rowsAt(rows, picked);
    }

    private static List<List<String>> systematicSample(List<List<String>> rows, SamplingParams p,
            int n, Random random) {
        int k = targetCount(n, p);
        if (k <= 0) {
            return new ArrayList<>();
        }
        if (k >= n) {
            return new ArrayList<>(rows);
        }
        double step = (double) n / k;
        int offset = step < 1 ? 0 : random.nextInt((int) Math.ceil(step));
        Set<Integer> pickedSet = new LinkedHashSet<>();
        for (int j = 0; j < k && pickedSet.size() < k; j++) {
            int idx = Math.min(n - 1, (int) Math.floor(offset + j * step));
            pickedSet.add(idx);
        }
        return rowsAt(rows, pickedSet.stream().sorted().toList());
    }

    private static List<List<String>> stratifiedSample(List<String> header, List<List<String>> rows,
            SamplingParams p, int n, Random random) {
        List<Integer> strataIdx = columnIndexes(header, p.strataColumns());
        if (strataIdx.isEmpty()) {
            return randomSample(rows, p, n, random);
        }
        Map<String, List<Integer>> groups = new LinkedHashMap<>();
        for (int i = 0; i < rows.size(); i++) {
            String key = groupKey(rows.get(i), strataIdx);
            groups.computeIfAbsent(key, ignored -> new ArrayList<>()).add(i);
        }
        double ratio = p.ratio() == null || p.ratio() <= 0 ? 0 : p.ratio();
        boolean byCount = p.count() != null && p.count() > 0;
        long count = byCount ? p.count() : 0;
        List<Integer> picked = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> entry : groups.entrySet()) {
            List<Integer> group = entry.getValue();
            int groupSize = group.size();
            int take;
            if (!byCount) {
                take = Math.min(groupSize, (int) Math.ceil(groupSize * ratio));
            } else {
                take = (int) Math.min(groupSize, count);
            }
            if (take > 0) {
                if (take >= groupSize) {
                    picked.addAll(group);
                } else {
                    List<Integer> shuffled = new ArrayList<>(group);
                    Collections.shuffle(shuffled, random);
                    picked.addAll(shuffled.subList(0, take));
                }
            }
        }
        return rowsAt(rows, picked.stream().sorted().toList());
    }

    private static List<List<String>> clusterSample(List<String> header, List<List<String>> rows,
            SamplingParams p, int n, Random random) {
        if (p.clusterColumn() != null && !p.clusterColumn().isBlank()) {
            Integer colIdx = columnIndex(header, p.clusterColumn());
            if (colIdx == null) {
                throw new IllegalArgumentException("cluster column not found: " + p.clusterColumn());
            }
            Map<String, List<Integer>> clusters = new LinkedHashMap<>();
            for (int i = 0; i < rows.size(); i++) {
                String key = cell(rows.get(i), colIdx);
                clusters.computeIfAbsent(key, ignored -> new ArrayList<>()).add(i);
            }
            List<String> keys = new ArrayList<>(clusters.keySet());
            List<String> selected = pickClusterKeys(keys, p, random);
            List<Integer> picked = new ArrayList<>();
            for (String key : selected) {
                picked.addAll(clusters.get(key));
            }
            return rowsAt(rows, picked.stream().sorted().toList());
        }
        if (p.blockSize() != null && p.blockSize() > 0) {
            int size = p.blockSize();
            int blockCount = (n + size - 1) / size;
            List<Integer> blockIdx = new ArrayList<>(blockCount);
            for (int b = 0; b < blockCount; b++) {
                blockIdx.add(b);
            }
            int targetBlocks = pickCount(blockCount, p);
            Collections.shuffle(blockIdx, random);
            Set<Integer> selectedBlocks = new LinkedHashSet<>(
                    blockIdx.subList(0, Math.min(targetBlocks, blockCount)));
            List<Integer> picked = new ArrayList<>();
            for (int b = 0; b < blockCount; b++) {
                if (selectedBlocks.contains(b)) {
                    for (int i = b * size; i < Math.min(n, (b + 1) * size); i++) {
                        picked.add(i);
                    }
                }
            }
            return rowsAt(rows, picked);
        }
        throw new IllegalArgumentException("CLUSTER sampling requires clusterColumn or blockSize");
    }

    /** Select cluster keys by {@code count} (number of clusters) or {@code ratio} (fraction). */
    private static List<String> pickClusterKeys(List<String> keys, SamplingParams p, Random random) {
        int target = pickCount(keys.size(), p);
        if (target >= keys.size()) {
            return keys;
        }
        List<String> shuffled = new ArrayList<>(keys);
        Collections.shuffle(shuffled, random);
        return new ArrayList<>(shuffled.subList(0, Math.max(0, target)));
    }

    private static int pickCount(int total, SamplingParams p) {
        if (p.count() != null && p.count() > 0) {
            return (int) Math.min(total, p.count());
        }
        if (p.ratio() != null && p.ratio() > 0) {
            return Math.min(total, (int) Math.ceil(total * p.ratio()));
        }
        return total;
    }

    /** Absolute row count target: {@code count} wins over {@code ratio}; default = no sampling. */
    private static int targetCount(int n, SamplingParams p) {
        if (p.count() != null && p.count() > 0) {
            return (int) Math.min(n, p.count());
        }
        if (p.ratio() != null && p.ratio() > 0) {
            return Math.min(n, (int) Math.ceil(n * p.ratio()));
        }
        return n;
    }

    private static long seedOf(SamplingParams p) {
        return p.seed() == null ? 1L : p.seed();
    }

    private static List<Integer> columnIndexes(List<String> header, List<String> columns) {
        List<Integer> indexes = new ArrayList<>();
        if (columns == null) {
            return indexes;
        }
        for (String col : columns) {
            Integer idx = columnIndex(header, col);
            if (idx != null) {
                indexes.add(idx);
            }
        }
        return indexes;
    }

    private static Integer columnIndex(List<String> header, String column) {
        if (header == null || column == null) {
            return null;
        }
        for (int i = 0; i < header.size(); i++) {
            if (column.equals(header.get(i))) {
                return i;
            }
        }
        return null;
    }

    private static String groupKey(List<String> row, List<Integer> strataIdx) {
        StringBuilder sb = new StringBuilder();
        for (Integer idx : strataIdx) {
            sb.append(cell(row, idx)).append('');
        }
        return sb.toString();
    }

    private static String cell(List<String> row, int idx) {
        return idx < row.size() ? row.get(idx) : "";
    }

    private static List<List<String>> rowsAt(List<List<String>> rows, List<Integer> indices) {
        List<List<String>> out = new ArrayList<>(indices.size());
        for (Integer idx : indices) {
            out.add(rows.get(idx));
        }
        return out;
    }
}
