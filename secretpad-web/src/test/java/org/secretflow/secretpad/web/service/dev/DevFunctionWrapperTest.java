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

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the Z-05 FUNCTION(UDF) Python wrapper generator (pure Java, no Spring).
 *
 * <p>断言包装器脚本结构：create_function 注册、只读 DB 打开（PRAGMA query_only=ON / mode=ro）、
 * 服务端渲染 SQL 的安全内嵌（引号/换行转义）、CSV 直通回退、结果 CSV 导出；以及函数名/参数个数/
 * 源码/SQL 的入参校验。</p>
 */
public class DevFunctionWrapperTest {

    private static final String RISK_SOURCE =
            "def risk_score(balance, trans_amount):\n"
                    + "    \"\"\"资金风险评分 3=高/2=中/1=低\"\"\"\n"
                    + "    score = 1\n"
                    + "    if balance < 20000:\n"
                    + "        score += 1\n"
                    + "    if trans_amount > 3000:\n"
                    + "        score += 1\n"
                    + "    return min(score, 3)\n";

    private static final String SQL =
            "SELECT account_no, risk_score(balance, trans_amount) AS risk_level FROM src "
                    + "WHERE category='A' ORDER BY risk_level DESC LIMIT 50";

    @Test
    void generatesWrapperWithUdfRegistrationAndReadOnlyDb() {
        String wrapper = DevFunctionWrapper.generate("risk_score", 2, RISK_SOURCE, SQL);
        // 前置 import + 用户函数体原样内嵌
        assertTrue(wrapper.contains("import argparse, csv, json, os, sqlite3"), wrapper);
        assertTrue(wrapper.contains("def risk_score(balance, trans_amount):"), wrapper);
        assertTrue(wrapper.contains("return min(score, 3)"), wrapper);
        // 只读 DB 打开 + UDF 注册
        assertTrue(wrapper.contains("conn = sqlite3.connect('file:' + db + '?mode=ro', uri=True)"), wrapper);
        assertTrue(wrapper.contains("conn.execute('PRAGMA query_only=ON')"), wrapper);
        assertTrue(wrapper.contains("conn.create_function('risk_score', 2, risk_score)"), wrapper);
        // runner 会透传 --input-table 等额外参数，包装器须用 parse_known_args 容忍未知参数（E2E 修复）
        assertTrue(wrapper.contains("a, _ = ap.parse_known_args()"), wrapper);
        // 服务端渲染 SQL 以 Python 字符串字面量内嵌（含 LIMIT 封顶）
        assertTrue(wrapper.contains("conn.execute(\"SELECT account_no, risk_score(balance, trans_amount) "
                + "AS risk_level FROM src WHERE category='A' ORDER BY risk_level DESC LIMIT 50\")"), wrapper);
        // CSV 直通回退 + 结果 CSV 导出（header + rows）
        assertTrue(wrapper.contains("r = csv.DictReader(f)"), wrapper);
        assertTrue(wrapper.contains("header = [d[0] for d in cur.description]"), wrapper);
        assertTrue(wrapper.contains("w.writerow(header)"), wrapper);
        assertTrue(wrapper.contains("w.writerows(rows)"), wrapper);
    }

    @Test
    void escapesEmbeddedSqlLiteral() {
        // SQL 含双引号（内嵌转义为 \"）与真实换行（转义为 \\n 字面量），不得破坏字符串边界
        String sql = "SELECT x FROM t WHERE note=\"ab\" LIMIT 3\nAND b=2";
        String wrapper = DevFunctionWrapper.generate("fn", 1, "def fn(x):\n    return x", sql);
        assertTrue(wrapper.contains("conn.execute(\"SELECT x FROM t WHERE note=\\\"ab\\\" LIMIT 3\\n"
                + "AND b=2\")"), wrapper);
    }

    @Test
    void rejectsInvalidFunctionName() {
        assertThrows(IllegalArgumentException.class,
                () -> DevFunctionWrapper.generate("1bad", 1, "def f():\n    pass", "SELECT 1"));
        assertThrows(IllegalArgumentException.class,
                () -> DevFunctionWrapper.generate("has space", 1, "def f():\n    pass", "SELECT 1"));
        assertThrows(IllegalArgumentException.class,
                () -> DevFunctionWrapper.generate("", 1, "def f():\n    pass", "SELECT 1"));
        assertThrows(IllegalArgumentException.class,
                () -> DevFunctionWrapper.generate(null, 1, "def f():\n    pass", "SELECT 1"));
    }

    @Test
    void rejectsInvalidNargs() {
        assertThrows(IllegalArgumentException.class,
                () -> DevFunctionWrapper.generate("f", -1, "def f():\n    pass", "SELECT 1"));
        assertThrows(IllegalArgumentException.class,
                () -> DevFunctionWrapper.generate("f", 128, "def f():\n    pass", "SELECT 1"));
    }

    @Test
    void rejectsBlankSourceOrSql() {
        assertThrows(IllegalArgumentException.class,
                () -> DevFunctionWrapper.generate("f", 1, "   ", "SELECT 1"));
        assertThrows(IllegalArgumentException.class,
                () -> DevFunctionWrapper.generate("f", 1, "def f():\n    pass", "  "));
        assertThrows(IllegalArgumentException.class,
                () -> DevFunctionWrapper.generate("f", 1, "def f():\n    pass", null));
    }

    @Test
    void embedsRenderedSqlFromEngine() {
        // 与 DevSqlEngine.renderBounded 集成：预渲染（参数插值 + LIMIT 封顶）后内嵌
        String rendered = DevSqlEngine.renderBounded(
                "SELECT fn(cat) FROM src WHERE cat={{cat}}", Map.of("cat", "A"), 5);
        String wrapper = DevFunctionWrapper.generate("fn", 1, "def fn(x):\n    return x", rendered);
        assertTrue(wrapper.contains("WHERE cat='A'"), wrapper);
        assertTrue(wrapper.contains("LIMIT 5"), wrapper);
    }
}
