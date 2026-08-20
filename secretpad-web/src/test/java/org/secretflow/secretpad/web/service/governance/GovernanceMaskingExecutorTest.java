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

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the pure Z-04 column desensitization executor (no Spring dependency).
 */
public class GovernanceMaskingExecutorTest {

    private static final List<String> HEADER = List.of("id", "phone", "name", "amount");
    private static final List<List<String>> ROWS = List.of(
            List.of("1", "13800001234", "alice", "12.345"),
            List.of("2", "13911112222", "bob", "abc"));

    private static GovernanceMaskingExecutor.MaskResult apply(
            GovernanceMaskingExecutor.MaskRule... rules) {
        return GovernanceMaskingExecutor.apply(HEADER, ROWS, List.of(rules));
    }

    @Test
    void maskPhoneKeepsPrefixAndSuffix() {
        GovernanceMaskingExecutor.MaskResult result = apply(
                new GovernanceMaskingExecutor.MaskRule("phone", "MASK",
                        Map.of("keepLeft", "3", "keepRight", "4")));
        assertEquals("138****1234", result.rows().get(0).get(1));
        assertEquals("139****2222", result.rows().get(1).get(1));
        assertEquals(HEADER, result.header());
    }

    @Test
    void maskShortValueUntouched() {
        GovernanceMaskingExecutor.MaskResult result = apply(
                new GovernanceMaskingExecutor.MaskRule("name", "MASK",
                        Map.of("keepLeft", "3", "keepRight", "4")));
        assertEquals("alice", result.rows().get(0).get(2));
    }

    @Test
    void replaceConstant() {
        GovernanceMaskingExecutor.MaskResult result = apply(
                new GovernanceMaskingExecutor.MaskRule("name", "REPLACE",
                        Map.of("value", "匿名")));
        assertEquals("匿名", result.rows().get(0).get(2));
        assertEquals("匿名", result.rows().get(1).get(2));
    }

    @Test
    void replaceMapping() {
        GovernanceMaskingExecutor.MaskResult result = apply(
                new GovernanceMaskingExecutor.MaskRule("name", "REPLACE",
                        Map.of("mapping", "{\"alice\":\"A**\",\"bob\":\"B**\"}")));
        assertEquals("A**", result.rows().get(0).get(2));
        assertEquals("B**", result.rows().get(1).get(2));
    }

    @Test
    void hashIsDeterministicAndNonReversible() {
        GovernanceMaskingExecutor.MaskResult result = apply(
                new GovernanceMaskingExecutor.MaskRule("phone", "HASH", Map.of("salt", "s1")));
        GovernanceMaskingExecutor.MaskResult again = apply(
                new GovernanceMaskingExecutor.MaskRule("phone", "HASH", Map.of("salt", "s1")));
        String first = result.rows().get(0).get(1);
        assertEquals(first, again.rows().get(0).get(1));
        assertEquals(64, first.length());
        assertNotEquals("13800001234", first);
        // 不同盐 → 不同摘要
        GovernanceMaskingExecutor.MaskResult other = apply(
                new GovernanceMaskingExecutor.MaskRule("phone", "HASH", Map.of("salt", "s2")));
        assertNotEquals(first, other.rows().get(0).get(1));
    }

    @Test
    void roundDigitsHalfUp() {
        GovernanceMaskingExecutor.MaskResult result = apply(
                new GovernanceMaskingExecutor.MaskRule("amount", "ROUND", Map.of("digits", "1")));
        assertEquals("12.3", result.rows().get(0).get(3));
        assertEquals("abc", result.rows().get(1).get(3)); // 非数值原样
    }

    @Test
    void clearEmptiesCells() {
        GovernanceMaskingExecutor.MaskResult result = apply(
                new GovernanceMaskingExecutor.MaskRule("phone", "CLEAR", Map.of()));
        assertEquals("", result.rows().get(0).get(1));
        assertEquals(HEADER, result.header());
    }

    @Test
    void clearDropRemovesColumn() {
        GovernanceMaskingExecutor.MaskResult result = apply(
                new GovernanceMaskingExecutor.MaskRule("phone", "CLEAR", Map.of("mode", "drop")));
        assertEquals(List.of("id", "name", "amount"), result.header());
        assertEquals(3, result.rows().get(0).size());
        assertEquals("alice", result.rows().get(0).get(1));
    }

    @Test
    void unknownColumnIgnored() {
        GovernanceMaskingExecutor.MaskResult result = apply(
                new GovernanceMaskingExecutor.MaskRule("nope", "CLEAR", Map.of("mode", "drop")));
        assertEquals(HEADER, result.header());
        assertEquals(ROWS, result.rows());
    }

    @Test
    void noRulesReturnsSameData() {
        GovernanceMaskingExecutor.MaskResult result = apply();
        assertEquals(HEADER, result.header());
        assertEquals(ROWS, result.rows());
    }

    @Test
    void unknownMethodThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> apply(new GovernanceMaskingExecutor.MaskRule("id", "NOPE", Map.of())));
        assertTrue(true); // suppress unused warning path
    }
}
