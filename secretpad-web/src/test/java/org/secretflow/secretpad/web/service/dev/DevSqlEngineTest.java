/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.service.dev;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the pure Z-05 embedded-SQLite read-only executor (no Spring dependency).
 */
public class DevSqlEngineTest {

    private static final String CSV = """
            id,name,amount,score,category
            1,alice,12.5,88,A
            2,bob,3.14,55,B
            3,carol,7.0,70,A
            """;

    private static DevSqlEngine.SqlResult run(String sql) {
        return DevSqlEngine.execute(CSV, sql, Map.of(), 10, 5);
    }

    private static DevSqlEngine.SqlResult run(String sql, Map<String, Object> params, int limit) {
        return DevSqlEngine.execute(CSV, sql, params, limit, 5);
    }

    @Test
    void infersIntegerRealTextTypes() {
        DevSqlEngine.SqlResult result = run("SELECT id, amount, name FROM src WHERE category='A'");
        assertEquals(List.of("id", "amount", "name"), result.header());
        assertEquals(2, result.rows().size());
        // id INTEGER -> "1" ; amount REAL -> "12.5" ; name TEXT
        assertEquals("1", result.rows().get(0).get(0));
        assertEquals("12.5", result.rows().get(0).get(1));
        assertEquals("alice", result.rows().get(0).get(2));
        assertEquals(3, result.sourceRows());
        assertTrue(result.logLines().contains("query_only=ON"));
    }

    @Test
    void supportsWithClause() {
        DevSqlEngine.SqlResult result = run("WITH t AS (SELECT category, count(*) c FROM src GROUP BY category)"
                + " SELECT category, c FROM t ORDER BY category");
        assertEquals(2, result.rows().size());
        assertEquals("A", result.rows().get(0).get(0));
    }

    @Test
    void enforcesLimit() {
        DevSqlEngine.SqlResult result = run("SELECT * FROM src", Map.of(), 2);
        assertEquals(2, result.rows().size());
    }

    @Test
    void interpolatesParamsAsStringLiteral() {
        DevSqlEngine.SqlResult result = run("SELECT * FROM src WHERE name={{name}}", Map.of("name", "bob"), 10);
        assertEquals(1, result.rows().size());
        assertEquals("bob", result.rows().get(0).get(1));
    }

    @Test
    void rejectsMissingParam() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> run("SELECT * FROM src WHERE name={{nope}}", Map.of(), 10));
        assertTrue(e.getMessage().contains("DEV_PARAM_INVALID"));
    }

    @Test
    void rejectsMultiStatement() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> run("SELECT * FROM src; DELETE FROM src"));
        assertTrue(e.getMessage().contains("单条"));
    }

    @Test
    void rejectsWriteStatement() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> run("INSERT INTO src VALUES (9, 'x', 1.0, 1, 'A')"));
        assertTrue(e.getMessage().contains("DEV_PARAM_INVALID"));
    }

    @Test
    void rejectsBannedKeyword() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> run("ATTACH DATABASE 'file:/etc/passwd' AS x"));
        assertTrue(e.getMessage().contains("ATTACH"));
    }

    @Test
    void renderBoundedInterpolatesAndAppendsLimit() {
        String rendered = DevSqlEngine.renderBounded(
                "SELECT * FROM src WHERE name={{name}}", Map.of("name", "bob"), 2);
        assertTrue(rendered.contains("WHERE name='bob'"), rendered);
        assertTrue(rendered.trim().endsWith("LIMIT 2;"), rendered);
        // 渲染结果可直接执行
        DevSqlEngine.SqlResult result = execute(rendered, 10);
        assertEquals(1, result.rows().size());
        assertEquals("bob", result.rows().get(0).get(1));
    }

    @Test
    void renderBoundedKeepsExistingLimit() {
        String rendered = DevSqlEngine.renderBounded(
                "SELECT * FROM src LIMIT 1", Map.of(), 10);
        assertTrue(rendered.trim().endsWith("LIMIT 1;"), rendered);
    }

    @Test
    void renderBoundedRejectsMissingParam() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> DevSqlEngine.renderBounded("SELECT * FROM src WHERE name={{nope}}", Map.of(), 10));
        assertTrue(e.getMessage().contains("DEV_PARAM_INVALID"));
    }

    @Test
    void renderBoundedRejectsBlank() {
        assertThrows(IllegalArgumentException.class,
                () -> DevSqlEngine.renderBounded("  ", Map.of(), 10));
        assertThrows(IllegalArgumentException.class,
                () -> DevSqlEngine.renderBounded(null, Map.of(), 10));
    }

    private DevSqlEngine.SqlResult execute(String sql, int limit) {
        return DevSqlEngine.execute(CSV, sql, Map.of(), limit, 5);
    }

    @Test
    void rejectsEmptyCsv() {
        assertThrows(IllegalArgumentException.class,
                () -> DevSqlEngine.execute("", "SELECT 1", Map.of(), 10, 5));
    }

    @Test
    void rejectsSyntaxError() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> run("SELECT FROM WHERE"));
        assertTrue(e.getMessage().contains("DEV_PARAM_INVALID"));
    }

    // 注：sqlite-jdbc 3.42.0.0 的 Statement.setQueryTimeout 会被接受但不强制生效
    // （实测 1e9 递归 CTE 未被中断）。引擎保留 setQueryTimeout 作为尽力而为的防护，
    // 真正的执行上界是「输入 CSV 行/字节上限 + 强制 LIMIT」。已作为已知环境限制记录。
}
