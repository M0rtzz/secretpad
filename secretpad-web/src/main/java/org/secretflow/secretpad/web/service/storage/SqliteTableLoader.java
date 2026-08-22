/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.service.storage;

import org.secretflow.secretpad.web.service.governance.CsvUtil;

import java.nio.file.Path;
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

/**
 * SQLite 文件库行注入原语：三级存储（节点级 {@code node_data.db} / 沙箱级 {@code sandbox_data.db}
 * / 结果回填）共用的建表 + 批量写行 + 只读读取工具。
 *
 * <p>自 {@link org.secretflow.secretpad.web.service.dev.DevSqlEngine} 抽取，保持列名清洗
 * （{@code sanitizeColumns}）与列类型推断（{@code inferType}，TEXT/INTEGER/REAL）两处逻辑唯一。
 * 表名一律收敛到 {@code [a-zA-Z0-9_]}（非法字符替换为 {@code _}），杜绝 SQL 注入。</p>
 */
public final class SqliteTableLoader {

    private SqliteTableLoader() {
    }

    /** 一次物化的结果摘要。 */
    public record Materialized(String tableName, List<String> columns, long rowCount) {
    }

    /** 表名收敛：非法字符替换为 {@code _}，首字符为数字则前缀 {@code t_}。 */
    public static String sanitizeTableName(String tableName) {
        String name = tableName == null ? "" : tableName.trim();
        if (name.isEmpty()) {
            throw new IllegalArgumentException("表名不能为空");
        }
        String base = name.replaceAll("[^a-zA-Z0-9_]", "_");
        if (base.isEmpty()) {
            base = "tbl";
        }
        if (Character.isDigit(base.charAt(0))) {
            base = "t_" + base;
        }
        return base;
    }

    /** 打开（必要时创建）文件库连接；小文件库关闭 WAL、设 busy_timeout，避免锁冲突。 */
    public static Connection open(Path dbFile) throws SQLException {
        if (dbFile.getParent() != null) {
            java.io.File parent = dbFile.getParent().toFile();
            if (!parent.exists() && !parent.mkdirs()) {
                throw new SQLException("无法创建数据库目录: " + parent);
            }
        }
        Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbFile.toAbsolutePath());
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("PRAGMA journal_mode = DELETE");
            stmt.execute("PRAGMA busy_timeout = 10000");
            stmt.execute("PRAGMA synchronous = OFF");
        }
        return conn;
    }

    /** 列名清洗：与 {@code DevSqlEngine} 一致的规则（非字母数字下划线→下划线，数字开头加前缀，去重）。 */
    public static List<String> sanitizeColumns(List<String> header) {
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

    /** 每列类型推断：扫描至多 100 行（空值跳过），全部 long→INTEGER、否则全部 double→REAL、其余 TEXT。 */
    public static String inferType(List<String> values) {
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

    /** 全列类型推断。 */
    public static List<String> inferColumnTypes(List<String> header, List<List<String>> data) {
        List<String> types = new ArrayList<>(header.size());
        for (int c = 0; c < header.size(); c++) {
            List<String> values = new ArrayList<>(data.size());
            for (List<String> row : data) {
                values.add(row.size() > c ? row.get(c) : "");
            }
            types.add(inferType(values));
        }
        return types;
    }

    /**
     * 在文件库中建表并批量写入行。
     *
     * @param dbFile      目标 SQLite 文件（不存在则创建）
     * @param tableName   目标表名（自动清洗）
     * @param header      表头（原始列名，建表时清洗）
     * @param rows        数据行
     * @param ifNotExists 表已存在时跳过（幂等重建语义）
     */
    public static Materialized materializeToFile(Path dbFile, String tableName,
            List<String> header, List<List<String>> rows, boolean ifNotExists) {
        if (header == null || header.isEmpty()) {
            throw new IllegalArgumentException("表头不能为空");
        }
        String safeTable = sanitizeTableName(tableName);
        List<String> safeCols = sanitizeColumns(header);
        List<String> types = inferColumnTypes(header, rows);
        List<List<String>> data = rows == null ? new ArrayList<>() : rows;
        try (Connection conn = open(dbFile)) {
            if (ifNotExists && tableExists(conn, safeTable)) {
                return new Materialized(safeTable, safeCols, countRows(conn, safeTable));
            }
            StringBuilder ddl = new StringBuilder("CREATE TABLE ")
                    .append(safeTable).append(" (");
            for (int c = 0; c < safeCols.size(); c++) {
                if (c > 0) {
                    ddl.append(", ");
                }
                ddl.append(safeCols.get(c)).append(' ').append(types.get(c));
            }
            ddl.append(')');
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(ddl.toString());
            }
            StringBuilder ins = new StringBuilder("INSERT INTO ")
                    .append(safeTable).append(" (");
            for (int c = 0; c < safeCols.size(); c++) {
                if (c > 0) {
                    ins.append(", ");
                }
                ins.append(safeCols.get(c));
            }
            ins.append(") VALUES (");
            for (int c = 0; c < safeCols.size(); c++) {
                if (c > 0) {
                    ins.append(", ");
                }
                ins.append('?');
            }
            ins.append(')');
            try (PreparedStatement ps = conn.prepareStatement(ins.toString())) {
                for (List<String> row : data) {
                    for (int c = 0; c < safeCols.size(); c++) {
                        ps.setString(c + 1, row.size() > c ? row.get(c) : "");
                    }
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            return new Materialized(safeTable, safeCols, data.size());
        } catch (SQLException e) {
            throw new IllegalStateException("SQLite 物化失败 " + dbFile + "/" + safeTable + ": " + e.getMessage(), e);
        }
    }

    /** 表是否存在（表名已清洗）。 */
    public static boolean tableExists(Path dbFile, String tableName) {
        try (Connection conn = open(dbFile)) {
            return tableExists(conn, tableName);
        } catch (SQLException e) {
            throw new IllegalStateException("SQLite 读取失败: " + e.getMessage(), e);
        }
    }

    private static boolean tableExists(Connection conn, String tableName) throws SQLException {
        String safeTable = sanitizeTableName(tableName);
        try (PreparedStatement ps = conn.prepareStatement(
                "select 1 from sqlite_master where type='table' and name=?")) {
            ps.setString(1, safeTable);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    /** 删除表（表名已清洗）；不存在时静默。 */
    public static void dropTableIfExists(Path dbFile, String tableName) {
        String safeTable = sanitizeTableName(tableName);
        try (Connection conn = open(dbFile)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + safeTable);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("SQLite 删除表失败: " + e.getMessage(), e);
        }
    }

    /** 统计表行数（表名已清洗）。 */
    public static long countRows(Path dbFile, String tableName) {
        try (Connection conn = open(dbFile)) {
            return countRows(conn, tableName);
        } catch (SQLException e) {
            throw new IllegalStateException("SQLite 读取失败: " + e.getMessage(), e);
        }
    }

    private static long countRows(Connection conn, String tableName) throws SQLException {
        String safeTable = sanitizeTableName(tableName);
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT count(*) FROM " + safeTable)) {
            return rs.next() ? rs.getLong(1) : 0;
        }
    }

    /** 读取表 schema（{@code PRAGMA table_info} → name/type/notnull）。 */
    public static List<Map<String, Object>> tableSchema(Path dbFile, String tableName) {
        String safeTable = sanitizeTableName(tableName);
        try (Connection conn = open(dbFile)) {
            try (PreparedStatement ps = conn.prepareStatement("PRAGMA table_info(" + safeTable + ")");
                 ResultSet rs = ps.executeQuery()) {
                List<Map<String, Object>> cols = new ArrayList<>();
                while (rs.next()) {
                    Map<String, Object> col = new LinkedHashMap<>();
                    col.put("name", rs.getString("name"));
                    col.put("type", rs.getString("type"));
                    col.put("notnull", rs.getInt("notnull"));
                    cols.add(col);
                }
                return cols;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("SQLite 读取 schema 失败: " + e.getMessage(), e);
        }
    }

    /** 读取表前 limit 行，返回 header + rows。 */
    public static List<List<String>> readRows(Path dbFile, String tableName, int limit) {
        String safeTable = sanitizeTableName(tableName);
        try (Connection conn = open(dbFile)) {
            List<List<String>> result = new ArrayList<>();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM " + safeTable + " LIMIT " + Math.max(0, limit))) {
                ResultSetMetaData md = rs.getMetaData();
                int cols = md.getColumnCount();
                List<String> header = new ArrayList<>(cols);
                for (int i = 1; i <= cols; i++) {
                    header.add(md.getColumnLabel(i));
                }
                result.add(header);
                while (rs.next()) {
                    List<String> row = new ArrayList<>(cols);
                    for (int i = 1; i <= cols; i++) {
                        Object v = rs.getObject(i);
                        row.add(v == null ? "" : String.valueOf(v));
                    }
                    result.add(row);
                }
                return result;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("SQLite 读取失败: " + e.getMessage(), e);
        }
    }

    /** 从 CSV 文本物化（供跨模块复用：表头首行、其余为数据行）。 */
    public static Materialized materializeCsvToFile(Path dbFile, String tableName, String csvText, boolean ifNotExists) {
        List<List<String>> parsed = CsvUtil.parse(csvText);
        if (parsed.isEmpty()) {
            throw new IllegalArgumentException("CSV 表头为空");
        }
        List<String> header = new ArrayList<>(parsed.get(0));
        List<List<String>> data = parsed.size() > 1
                ? new ArrayList<>(parsed.subList(1, parsed.size()))
                : new ArrayList<>();
        return materializeToFile(dbFile, tableName, header, data, ifNotExists);
    }
}
