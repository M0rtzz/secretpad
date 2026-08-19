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

import org.secretflow.secretpad.web.service.governance.CsvUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Z-05 内嵌 SQLite 只读 SQL 执行引擎（进程内）。
 *
 * <p>用户已确认决策：SQL 编辑/执行/调试在平台进程内用内嵌 SQLite 完成，无需新容器镜像。
 * 安全边界为多层防护：每任务独立 {@code jdbc:sqlite::memory:} 连接（用完即关，不动平台
 * {@code jdbcTemplate} 数据源）；{@code PRAGMA query_only=ON} 由 SQLite 自身硬阻断任何写操作；
 * 语句门禁仅放行单条只读语句（首关键字 SELECT/WITH，禁 PRAGMA/ATTACH/VACUUM/EXPLAIN，禁内嵌分号）；
 * {@link Statement#setQueryTimeout} 限时；结果行数强制 {@code LIMIT}。</p>
 */
public final class DevSqlEngine {

    private DevSqlEngine() {
    }

    /** 单次执行结果。 */
    public record SqlResult(
            List<String> header,
            List<List<String>> rows,
            long sourceRows,
            long resultRows,
            long elapsedMs,
            List<String> logLines) {
    }

    private static final Pattern PLACEHOLDER = Pattern.compile("\\{\\{(\\w+)\\}\\}");
    private static final Pattern TRAILING_LIMIT =
            Pattern.compile("(?i)\\bLIMIT\\s+(\\d+)\\s*$");
    private static final Pattern TRAILING_SEMIS = Pattern.compile("[;\\s]+$");
    private static final String[] BANNED_FIRST = {"PRAGMA", "ATTACH", "VACUUM", "EXPLAIN"};

    /**
     * 在独立内嵌 SQLite 内存库中执行只读 SQL。
     *
     * @param csvText        授权源 CSV 全文（首行表头）
     * @param sql            用户 SQL（单条只读语句，可含 {@code {{param}}} 占位符）
     * @param params         占位符参数（插值为 SQL 字符串字面量，单引号加倍）
     * @param maxResultRows  结果行数上限（强制 LIMIT）
     * @param timeoutSeconds SQL 执行超时秒数
     */
    public static SqlResult execute(String csvText, String sql,
            Map<String, Object> params, int maxResultRows, int timeoutSeconds) {
        if (csvText == null || csvText.isBlank()) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 源 CSV 为空");
        }
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": SQL 为空");
        }
        List<List<String>> parsed = CsvUtil.parse(csvText);
        if (parsed.isEmpty()) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 源 CSV 表头为空");
        }
        List<String> header = new ArrayList<>(parsed.get(0));
        List<List<String>> data = parsed.size() > 1
                ? new ArrayList<>(parsed.subList(1, parsed.size()))
                : new ArrayList<>();
        if (header.isEmpty()) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 源 CSV 表头为空");
        }

        long start = System.currentTimeMillis();
        List<String> logs = new ArrayList<>();
        String rendered = null;
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:")) {
            conn.setAutoCommit(true);
            List<String> safeCols = sanitizeColumns(header);
            createSourceTable(conn, safeCols, header, data, logs);
            try (Statement pragma = conn.createStatement()) {
                pragma.execute("PRAGMA query_only = ON");
            }
            logs.add("query_only=ON");
            assertReadOnly(sql);
            rendered = interpolate(sql, params);
            String bounded = ensureLimit(rendered, maxResultRows);
            logs.add("exec " + String.valueOf(timeoutSeconds) + "s timeout, limit=" + maxResultRows);

            try (Statement stmt = conn.createStatement()) {
                stmt.setQueryTimeout(timeoutSeconds);
                try (ResultSet rs = stmt.executeQuery(bounded)) {
                    ResultSetMetaData md = rs.getMetaData();
                    int cols = md.getColumnCount();
                    List<String> outHeader = new ArrayList<>(cols);
                    for (int i = 1; i <= cols; i++) {
                        outHeader.add(md.getColumnLabel(i));
                    }
                    List<List<String>> rows = new ArrayList<>();
                    while (rs.next() && rows.size() < maxResultRows) {
                        List<String> row = new ArrayList<>(cols);
                        for (int i = 1; i <= cols; i++) {
                            Object v = rs.getObject(i);
                            row.add(v == null ? "" : String.valueOf(v));
                        }
                        rows.add(row);
                    }
                    long elapsed = System.currentTimeMillis() - start;
                    return new SqlResult(outHeader, rows, data.size(), rows.size(), elapsed, logs);
                }
            }
        } catch (SQLException e) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": SQL 执行失败 - "
                    + e.getMessage() + "（" + (rendered == null ? "" : truncate(rendered, 200)) + "）");
        }
    }

    /* ------------------------------ 内部实现 ------------------------------ */

    private static List<String> sanitizeColumns(List<String> header) {
        List<String> result = new ArrayList<>(header.size());
        Set<String> used = new HashSet<>();
        for (int i = 0; i < header.size(); i++) {
            String raw = header.get(i) == null ? "" : header.get(i).trim();
            String base = raw.replaceAll("[^a-zA-Z0-9_]", "_");
            if (base.isEmpty()) {
                base = "col" + i;
            }
            if (Character.isDigit(base.charAt(0))) {
                base = "col_" + base;
            }
            String name = base;
            int n = 2;
            while (used.contains(name)) {
                name = base + "_" + n;
                n++;
            }
            used.add(name);
            result.add(name);
        }
        return result;
    }

    private static void createSourceTable(Connection conn, List<String> safeCols,
            List<String> header, List<List<String>> data, List<String> logs) throws SQLException {
        int cols = safeCols.size();
        List<String> types = new ArrayList<>(cols);
        for (int c = 0; c < cols; c++) {
            List<String> values = new ArrayList<>(data.size());
            for (List<String> row : data) {
                values.add(row.size() > c ? row.get(c) : "");
            }
            types.add(inferType(values));
        }
        StringBuilder ddl = new StringBuilder("CREATE TABLE src (");
        for (int c = 0; c < cols; c++) {
            if (c > 0) {
                ddl.append(", ");
            }
            ddl.append(safeCols.get(c)).append(' ').append(types.get(c));
        }
        ddl.append(')');
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(ddl.toString());
        }
        logs.add("created src table " + cols + " cols, " + data.size() + " rows");

        StringBuilder ins = new StringBuilder("INSERT INTO src (");
        for (int c = 0; c < cols; c++) {
            if (c > 0) {
                ins.append(", ");
            }
            ins.append(safeCols.get(c));
        }
        ins.append(") VALUES (");
        for (int c = 0; c < cols; c++) {
            if (c > 0) {
                ins.append(", ");
            }
            ins.append('?');
        }
        ins.append(')');
        try (PreparedStatement ps = conn.prepareStatement(ins.toString())) {
            for (List<String> row : data) {
                for (int c = 0; c < cols; c++) {
                    ps.setString(c + 1, row.size() > c ? row.get(c) : "");
                }
                ps.addBatch();
            }
            ps.executeBatch();
        }
        logs.add("loaded " + data.size() + " source rows");
    }

    /** 扫描前 100 行推断列类型：全部可解析为 long → INTEGER；否则全部可解析为 double → REAL；其余 TEXT。 */
    private static String inferType(List<String> values) {
        boolean allLong = true;
        boolean allDouble = true;
        int nonEmpty = 0;
        int n = Math.min(values.size(), 100);
        for (int i = 0; i < n; i++) {
            String v = values.get(i);
            if (v == null || v.isBlank()) {
                continue;
            }
            nonEmpty++;
            String t = v.trim();
            if (allLong) {
                try {
                    Long.parseLong(t);
                } catch (NumberFormatException e) {
                    allLong = false;
                }
            }
            if (allDouble) {
                try {
                    Double.parseDouble(t);
                } catch (NumberFormatException e) {
                    allDouble = false;
                }
            }
        }
        if (nonEmpty == 0) {
            return "TEXT";
        }
        if (allLong) {
            return "INTEGER";
        }
        if (allDouble) {
            return "REAL";
        }
        return "TEXT";
    }

    private static void assertReadOnly(String sql) {
        String body = TRAILING_SEMIS.matcher(sql).replaceFirst("");
        if (body.contains(";")) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 仅允许单条 SQL 语句");
        }
        String first = firstKeyword(body);
        if (!"SELECT".equals(first) && !"WITH".equals(first)) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 仅允许 SELECT/WITH 只读查询，收到 " + first);
        }
        for (String banned : BANNED_FIRST) {
            if (containsWord(body, banned)) {
                throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 禁止语句 " + banned);
            }
        }
    }

    private static String firstKeyword(String body) {
        String s = body;
        while (true) {
            s = s.stripLeading();
            if (s.startsWith("--")) {
                int nl = s.indexOf('\n');
                s = nl < 0 ? "" : s.substring(nl);
                continue;
            }
            if (s.startsWith("/*")) {
                int end = s.indexOf("*/");
                s = end < 0 ? "" : s.substring(end + 2);
                continue;
            }
            break;
        }
        int end = 0;
        while (end < s.length() && !Character.isWhitespace(s.charAt(end))) {
            end++;
        }
        return s.substring(0, end).toUpperCase(Locale.ROOT);
    }

    private static boolean containsWord(String text, String word) {
        return Pattern.compile("(?i)\\b" + Pattern.quote(word) + "\\b").matcher(text).find();
    }

    private static String interpolate(String sql, Map<String, Object> params) {
        Matcher m = PLACEHOLDER.matcher(sql);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String name = m.group(1);
            Object v = params == null ? null : params.get(name);
            if (v == null) {
                throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 参数 {{" + name + "}} 未提供");
            }
            m.appendReplacement(sb, Matcher.quoteReplacement(quoteLiteral(String.valueOf(v))));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private static String quoteLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    /** 未显式带数值 LIMIT 时追加 {@code LIMIT n}（结果行数强制有界）。 */
    private static String ensureLimit(String sql, int maxResultRows) {
        String body = TRAILING_SEMIS.matcher(sql).replaceFirst("");
        Matcher m = TRAILING_LIMIT.matcher(body);
        if (m.find()) {
            return body + ";";
        }
        return body + " LIMIT " + maxResultRows + ";";
    }

    private static String truncate(String s, int n) {
        return s.length() <= n ? s : s.substring(0, n) + "...";
    }
}
