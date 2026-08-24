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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the pure Z-04 sampling executor (no Spring dependency).
 *
 * <p>Rows are {@code [id, category, value]}; categories cycle A/B/C. Count-based assertions
 * cover the four methods plus seed reproducibility, ratio semantics and error paths.</p>
 */
public class GovernanceSamplingExecutorTest {

    private static final List<String> HEADER = List.of("id", "category", "value");

    private static List<List<String>> rows(int n) {
        List<List<String>> rows = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            rows.add(List.of(String.valueOf(i), String.valueOf((char) ('A' + i % 3)),
                    String.valueOf(i * 10)));
        }
        return rows;
    }

    private static List<String> column(List<List<String>> rows, int idx) {
        return rows.stream().map(r -> r.get(idx)).toList();
    }

    @Test
    void randomPicksExactCount() {
        List<List<String>> out = GovernanceSamplingExecutor.sample(HEADER, rows(100),
                new GovernanceSamplingExecutor.SamplingParams("RANDOM", 10L, null, null, null,
                        null, 42L, null));
        assertEquals(10, out.size());
        assertTrue(rows(100).containsAll(out));
    }

    @Test
    void randomRatioCeilsAndSeedIsReproducible() {
        List<List<String>> a = GovernanceSamplingExecutor.sample(HEADER, rows(97),
                new GovernanceSamplingExecutor.SamplingParams("RANDOM", null, 0.1, null, null,
                        null, 7L, null));
        List<List<String>> b = GovernanceSamplingExecutor.sample(HEADER, rows(97),
                new GovernanceSamplingExecutor.SamplingParams("RANDOM", null, 0.1, null, null,
                        null, 7L, null));
        assertEquals(10, a.size()); // ceil(97 * 0.1) = 10
        assertEquals(a, b);
    }

    @Test
    void randomCountAboveSizeReturnsAll() {
        List<List<String>> out = GovernanceSamplingExecutor.sample(HEADER, rows(5),
                new GovernanceSamplingExecutor.SamplingParams("RANDOM", 100L, null, null, null,
                        null, 1L, null));
        assertEquals(5, out.size());
    }

    @Test
    void systematicSpacingMatchesStep() {
        List<List<String>> out = GovernanceSamplingExecutor.sample(HEADER, rows(100),
                new GovernanceSamplingExecutor.SamplingParams("SYSTEMATIC", 10L, null, null,
                        null, null, 1L, null));
        assertEquals(10, out.size());
        List<String> ids = column(out, 0);
        Set<String> distinct = new HashSet<>(ids);
        assertEquals(10, distinct.size());
        // 等距：相邻被选 id 间距应接近 10（容差 1，offset 随机在 [0,10)）
        for (int i = 1; i < ids.size(); i++) {
            int gap = Integer.parseInt(ids.get(i)) - Integer.parseInt(ids.get(i - 1));
            assertTrue(gap >= 9 && gap <= 11, "unexpected gap " + gap);
        }
    }

    @Test
    void stratifiedKeepsEveryGroup() {
        List<List<String>> out = GovernanceSamplingExecutor.sample(HEADER, rows(90),
                new GovernanceSamplingExecutor.SamplingParams("STRATIFIED", null, 0.5,
                        List.of("category"), null, null, 3L, null));
        assertEquals(45, out.size()); // 30/组 × ceil(0.5*30)=15 × 3 = 45
        Set<String> categories = new HashSet<>(column(out, 1));
        assertEquals(Set.of("A", "B", "C"), categories);
    }

    @Test
    void stratifiedCountAppliesToEveryGroup() {
        List<List<String>> out = GovernanceSamplingExecutor.sample(HEADER, rows(90),
                new GovernanceSamplingExecutor.SamplingParams("STRATIFIED", 6L, null,
                        List.of("category"), null, null, 5L, null));
        assertEquals(18, out.size()); // 3 组 × 每组 6 行
        Set<String> categories = new HashSet<>(column(out, 1));
        assertEquals(Set.of("A", "B", "C"), categories);
    }

    @Test
    void stratifiedCountTakesAllRowsFromSmallerGroups() {
        List<List<String>> input = List.of(
                List.of("1", "A", "10"),
                List.of("2", "A", "20"),
                List.of("3", "B", "30"),
                List.of("4", "B", "40"),
                List.of("5", "B", "50"),
                List.of("6", "B", "60"));
        List<List<String>> out = GovernanceSamplingExecutor.sample(HEADER, input,
                new GovernanceSamplingExecutor.SamplingParams("STRATIFIED", 3L, null,
                        List.of("category"), null, null, 5L, null));
        assertEquals(5, out.size()); // A 层仅 2 行全取，B 层抽取 3 行
        assertEquals(2, column(out, 1).stream().filter("A"::equals).count());
        assertEquals(3, column(out, 1).stream().filter("B"::equals).count());
    }

    @Test
    void clusterByColumnSelectsWholeClusters() {
        List<List<String>> out = GovernanceSamplingExecutor.sample(HEADER, rows(30),
                new GovernanceSamplingExecutor.SamplingParams("CLUSTER", 1L, null, null,
                        "category", null, 11L, null));
        Set<String> selected = new HashSet<>(column(out, 1));
        assertEquals(1, selected.size()); // 1 个整群
        assertEquals(10, out.size());      // 每群 10 行
    }

    @Test
    void clusterByBlockSizeSelectsWholeBlocks() {
        List<List<String>> out = GovernanceSamplingExecutor.sample(HEADER, rows(30),
                new GovernanceSamplingExecutor.SamplingParams("CLUSTER", 1L, null, null,
                        null, 10, 13L, null));
        assertEquals(10, out.size()); // 1 个连续块 = 10 行
        List<String> ids = column(out, 0);
        Set<String> distinct = new HashSet<>(ids);
        assertEquals(10, distinct.size());
    }

    @Test
    void limitCapsOutput() {
        List<List<String>> out = GovernanceSamplingExecutor.sample(HEADER, rows(100),
                new GovernanceSamplingExecutor.SamplingParams("RANDOM", 20L, null, null, null,
                        null, 1L, 5));
        assertEquals(5, out.size());
    }

    @Test
    void unknownMethodThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> GovernanceSamplingExecutor.sample(HEADER, rows(10),
                        GovernanceSamplingExecutor.SamplingParams.of("NOPE")));
    }

    @Test
    void clusterRequiresColumnOrBlockSize() {
        assertThrows(IllegalArgumentException.class,
                () -> GovernanceSamplingExecutor.sample(HEADER, rows(10),
                        GovernanceSamplingExecutor.SamplingParams.of("CLUSTER")));
    }

    @Test
    void emptyRowsPassThrough() {
        assertEquals(0, GovernanceSamplingExecutor.sample(HEADER, List.of(),
                new GovernanceSamplingExecutor.SamplingParams("RANDOM", 5L, null, null, null,
                        null, 1L, null)).size());
    }
}
