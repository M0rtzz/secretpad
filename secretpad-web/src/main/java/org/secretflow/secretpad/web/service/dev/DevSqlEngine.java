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
import org.secretflow.secretpad.web.service.storage.SqliteTableLoader;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
        return execute(csvText, sql, params, maxResultRows, timeoutSeconds, "src");
    }

    /**
     * 同 {@link #execute}，但输入表名取自 SQL 的 FROM/JOIN 引用（{@link #detectTableName}）：
     * 使 API 进程内 SQL 调用无需强制书写 {@code src}，兼容「沙箱源表名」与「src」两种书写习惯。
     */
    public static SqlResult executeNamed(String csvText, String sql,
            Map<String, Object> params, int maxResultRows, int timeoutSeconds) {
        return execute(csvText, sql, params, maxResultRows, timeoutSeconds, detectTableName(sql));
    }

    /**
     * 提取 SQL 首个 FROM/JOIN 引用的表名（去掉字符串字面量后扫描，无显式引用默认 {@code src}）。
     * 供 API 函数/进程内 SQL 调用以正确表名装载调用方输入行。
     */
    public static String detectTableName(String sql) {
        if (sql == null || sql.isBlank()) {
            return "src";
        }
        String body = TRAILING_SEMIS.matcher(sql).replaceFirst("");
        String withoutLiterals = body.replaceAll("'([^']|'')*'", "''");
        Matcher m = TABLE_REF.matcher(withoutLiterals);
        return m.find() ? m.group(1) : "src";
    }

    private static SqlResult execute(String csvText, String sql,
            Map<String, Object> params, int maxResultRows, int timeoutSeconds, String tableName) {
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
            List<String> safeCols = SqliteTableLoader.sanitizeColumns(header);
            createSourceTable(conn, tableName, safeCols, data, logs);
            try (Statement pragma = conn.createStatement()) {
                pragma.execute("PRAGMA query_only = ON");
            }
            logs.add("query_only=ON");
            assertReadOnly(sql);
            assertNoResultConsumption(sql);
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

    /**
     * 在沙箱文件库 {@code sandbox_data.db} 上执行只读 SQL（Stage 4 沙箱计算契约）。
     *
     * <p>与 {@link #execute} 不同：不再新建 src 表，直接对预置表执行；连接以只读打开并叠加
     * {@code PRAGMA query_only=ON} 双重防护（文件存在才打开，杜绝误建文件）。语句门禁
     * （SELECT/WITH、禁 PRAGMA/ATTACH/VACUUM/EXPLAIN）、{{@code param}} 插值与强制 LIMIT 与
     * 内存版完全一致。</p>
     */
    public static SqlResult executeOnDb(Path dbFile, String sql,
            Map<String, Object> params, int maxResultRows, int timeoutSeconds) {
        if (dbFile == null || !Files.exists(dbFile)) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 沙箱数据库不存在");
        }
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": SQL 为空");
        }
        long start = System.currentTimeMillis();
        List<String> logs = new ArrayList<>();
        String rendered = null;
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbFile.toAbsolutePath().normalize())) {
            try (Statement pragma = conn.createStatement()) {
                pragma.execute("PRAGMA query_only = ON");
            }
            logs.add("query_only=ON, db=" + dbFile.getFileName());
            assertReadOnly(sql);
            assertNoResultConsumption(sql);
            rendered = interpolate(sql, params);
            String bounded = ensureLimit(rendered, maxResultRows);
            logs.add("exec " + timeoutSeconds + "s timeout, limit=" + maxResultRows);

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
                    return new SqlResult(outHeader, rows, 0, rows.size(), elapsed, logs);
                }
            }
        } catch (SQLException e) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": SQL 执行失败 - "
                    + e.getMessage() + "（" + (rendered == null ? "" : truncate(rendered, 200)) + "）");
        }
    }

    /**
     * 服务端预渲染有界 SQL：先插值 {@code {{param}}}，再封顶输出（供 FUNCTION 任务包装器内嵌）。
     *
     * <p>FUNCTION（UDF）任务由后端生成 Python 包装脚本，脚本内嵌本方法渲染后的 SQL 原文；
     * 服务端提前完成参数插值（引号加倍防注入）与 LIMIT 封顶，包装器内只执行不再处理占位符，
     * 保证服务端与 pod 内执行的 SQL 完全一致。</p>
     */
    public static String renderBounded(String sql, Map<String, Object> params, int maxResultRows) {
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": SQL 为空");
        }
        String rendered = interpolate(sql, params);
        return ensureLimit(rendered, maxResultRows);
    }

    /* ------------------------------ 内部实现 ------------------------------ */

    private static void createSourceTable(Connection conn, String tableName, List<String> safeCols,
            List<List<String>> data, List<String> logs) throws SQLException {
        int cols = safeCols.size();
        List<String> types = SqliteTableLoader.inferColumnTypes(safeCols, data);
        StringBuilder ddl = new StringBuilder("CREATE TABLE ").append(tableName).append(" (");
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
        logs.add("created " + tableName + " table " + cols + " cols, " + data.size() + " rows");

        StringBuilder ins = new StringBuilder("INSERT INTO ").append(tableName).append(" (");
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

    /** FROM/JOIN 引用的表名（去掉字符串字面量后扫描，规避列名/字符串里的误报）。 */
    private static final Pattern TABLE_REF =
            Pattern.compile("(?i)\\b(?:from|join)\\s+[`\"]?([a-zA-Z_][\\w\\-]*)[`\"]?");

    /** 计算结果表（result_*）只能预览/导出，禁止在 SQL 的 FROM/JOIN 中引用消费。 */
    private static void assertNoResultConsumption(String sql) {
        String body = TRAILING_SEMIS.matcher(sql).replaceFirst("");
        String withoutLiterals = body.replaceAll("'([^']|'')*'", "''");
        Matcher m = TABLE_REF.matcher(withoutLiterals);
        while (m.find()) {
            String table = m.group(1);
            if (table.toLowerCase(Locale.ROOT).startsWith("result_")) {
                throw new IllegalArgumentException(DevErrors.DEV_RESULT_NOT_CONSUMABLE
                        + ": 计算结果表不能作为沙箱计算源（仅支持预览与导出）: " + table);
            }
        }
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
