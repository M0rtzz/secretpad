package org.secretflow.secretpad.web.service;

import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.DigestInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

/** Read-only database preview and CSV materialization for catalog assets. */
@Service
public class DatabaseAssetImportService {
    private static final Set<String> TYPES = Set.of("MYSQL", "POSTGRESQL", "GREATSQL", "OPENGAUSS");
    private static final Pattern TABLE = Pattern.compile("[A-Za-z_][A-Za-z0-9_$]*(\\.[A-Za-z_][A-Za-z0-9_$]*)?");
    private static final Pattern WRITE_SQL = Pattern.compile(
            "(?is)\\b(insert|update|delete|merge|replace|create|alter|drop|truncate|grant|revoke|call|copy|vacuum|analyze)\\b");

    private final DataAssetService assets;

    public DatabaseAssetImportService(DataAssetService assets) {
        this.assets = assets;
    }

    public Map<String, Object> preview(Map<String, Object> request) {
        QuerySpec spec = querySpec(request);
        try (Connection connection = connect(spec); Statement statement = connection.createStatement()) {
            connection.setReadOnly(true);
            statement.setMaxRows(10);
            statement.setQueryTimeout(30);
            try (ResultSet result = statement.executeQuery(spec.sql())) {
                ResultSetMetaData metadata = result.getMetaData();
                List<String> columns = columns(metadata);
                List<Map<String, String>> rows = new ArrayList<>();
                while (result.next() && rows.size() < 10) {
                    Map<String, String> row = new LinkedHashMap<>();
                    for (int i = 1; i <= columns.size(); i++) {
                        row.put(columns.get(i - 1), Objects.toString(result.getObject(i), ""));
                    }
                    rows.add(row);
                }
                return Map.of("columns", columns, "rows", rows, "masked", false,
                        "asset", Map.of("name", displayName(request, spec)));
            }
        } catch (SQLException e) {
            throw new IllegalArgumentException("数据库只读查询失败: " + e.getMessage(), e);
        }
    }

    public Map<String, Object> testConnection(Map<String, Object> request) {
        QuerySpec spec = querySpec(request, "");
        try (Connection connection = connect(spec)) {
            var metadata = connection.getMetaData();
            List<String> tables = new ArrayList<>();
            try (ResultSet result = metadata.getTables(null, null, "%", new String[]{"TABLE", "VIEW"})) {
                while (result.next()) {
                    String schema = result.getString("TABLE_SCHEM");
                    String name = result.getString("TABLE_NAME");
                    if (name == null || name.isBlank()) continue;
                    if (!isVisibleSchema(spec, schema)) continue;
                    tables.add(schema == null || schema.isBlank() ? name : schema + "." + name);
                }
            }
            return Map.of("connected", true, "tables", tables);
        } catch (SQLException e) {
            throw new IllegalArgumentException("数据库连接失败: " + e.getMessage(), e);
        }
    }

    public Map<String, Object> importAsset(Map<String, Object> request) {
        QuerySpec spec = querySpec(request);
        Path temp = null;
        try {
            temp = Files.createTempFile("secretpad-database-", ".csv");
            try (Connection connection = connect(spec);
                 Statement statement = connection.createStatement();
                 BufferedWriter writer = Files.newBufferedWriter(temp, StandardCharsets.UTF_8)) {
                connection.setReadOnly(true);
                statement.setFetchSize(500);
                statement.setQueryTimeout(300);
                try (ResultSet result = statement.executeQuery(spec.sql())) {
                    ResultSetMetaData metadata = result.getMetaData();
                    int count = metadata.getColumnCount();
                    for (int i = 1; i <= count; i++) {
                        if (i > 1) writer.write(',');
                        csv(writer, metadata.getColumnLabel(i));
                    }
                    writer.newLine();
                    while (result.next()) {
                        for (int i = 1; i <= count; i++) {
                            if (i > 1) writer.write(',');
                            csv(writer, Objects.toString(result.getObject(i), ""));
                        }
                        writer.newLine();
                    }
                }
            }
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream input = new DigestInputStream(Files.newInputStream(temp), digest)) {
                input.transferTo(OutputStream.nullOutputStream());
            }
            long size = Files.size(temp);
            String checksum = HexFormat.of().formatHex(digest.digest());
            String name = displayName(request, spec);
            String uri = assets.storage().put("database/" + UUID.randomUUID() + "/" + safeFileName(name) + ".csv",
                    temp.toFile(), "text/csv", checksum);
            return assets.registerStored(name, "text/csv", "RAW", uri, checksum, size, "DATABASE");
        } catch (Exception e) {
            throw new IllegalStateException("数据库数据导入失败: " + e.getMessage(), e);
        } finally {
            if (temp != null) try { Files.deleteIfExists(temp); } catch (IOException ignored) { }
        }
    }

    private QuerySpec querySpec(Map<String, Object> request) {
        return querySpec(request, value(request, "tableName").trim());
    }

    private QuerySpec querySpec(Map<String, Object> request, String requestedTable) {
        String type = required(request, "databaseType").toUpperCase(Locale.ROOT);
        if (!TYPES.contains(type)) throw new IllegalArgumentException("不支持的数据库类型: " + type);
        String url = required(request, "jdbcUrl");
        String expectedPrefix = switch (type) {
            case "MYSQL" -> "jdbc:mysql:";
            case "POSTGRESQL" -> "jdbc:postgresql:";
            case "GREATSQL" -> "jdbc:mysql:";
            case "OPENGAUSS" -> "jdbc:opengauss:";
            default -> "";
        };
        if (!url.toLowerCase(Locale.ROOT).startsWith(expectedPrefix)) {
            throw new IllegalArgumentException("JDBC 地址与数据库类型不匹配");
        }
        String table = requestedTable;
        String sql = value(request, "sql").trim();
        if (!sql.isBlank()) throw new IllegalArgumentException("SQL 过滤已停用，请选择数据表");
        if (sql.isBlank()) {
            if (!table.isBlank()) {
                if (!TABLE.matcher(table).matches()) throw new IllegalArgumentException("表名格式无效");
                sql = "select * from " + table;
            }
        }
        return new QuerySpec(url, value(request, "username"),
                String.valueOf(request.getOrDefault("password", "")), sql, table);
    }

    static void validateReadOnlySql(String sql) {
        String normalized = sql.replaceAll("(?s)/\\*.*?\\*/", " ")
                .replaceAll("(?m)--.*$", " ")
                .replaceAll("'(?:''|[^'])*'", "''");
        if (normalized.indexOf(';') >= 0
                || !normalized.matches("(?is)^\\s*(select|with)\\b.*")
                || WRITE_SQL.matcher(normalized).find()) {
            throw new IllegalArgumentException("仅支持单条 SELECT/WITH 只读查询");
        }
    }

    private Connection connect(QuerySpec spec) throws SQLException {
        return DriverManager.getConnection(spec.url(), spec.username(), spec.password());
    }

    private boolean isSystemSchema(String url, String schema) {
        if (schema == null || schema.isBlank()) return false;
        String normalized = schema.toLowerCase(Locale.ROOT);
        if (normalized.equals("information_schema") || normalized.equals("pg_catalog")
                || normalized.equals("pg_toast") || normalized.startsWith("pg_temp_")) return true;
        // openGauss exposes optional DB4AI and PL/Developer schemas through
        // JDBC metadata; they are tools, not user data assets.
        return url.toLowerCase(Locale.ROOT).startsWith("jdbc:opengauss:")
                && (normalized.equals("db4ai") || normalized.equals("dbe_pldeveloper"));
    }

    private boolean isVisibleSchema(QuerySpec spec, String schema) {
        if (spec.url().toLowerCase(Locale.ROOT).startsWith("jdbc:opengauss:")) {
            // openGauss exposes many dbe_* monitoring schemas through JDBC.
            // A catalog connection should list only the current user's schema.
            return schema != null && schema.equalsIgnoreCase(spec.username());
        }
        return !isSystemSchema(spec.url(), schema);
    }

    private List<String> columns(ResultSetMetaData metadata) throws SQLException {
        List<String> columns = new ArrayList<>();
        for (int i = 1; i <= metadata.getColumnCount(); i++) columns.add(metadata.getColumnLabel(i));
        return columns;
    }

    private void csv(BufferedWriter writer, String value) throws IOException {
        writer.write('"');
        writer.write((value == null ? "" : value).replace("\"", "\"\""));
        writer.write('"');
    }

    private String displayName(Map<String, Object> request, QuerySpec spec) {
        String name = value(request, "name").trim();
        return name.isBlank() ? (spec.tableName().isBlank() ? "database-import" : spec.tableName()) : name;
    }

    private String safeFileName(String name) {
        String safe = name.replaceAll("[^A-Za-z0-9._-]", "_");
        return safe.isBlank() ? "database-import" : safe;
    }

    private String required(Map<String, Object> request, String key) {
        String value = value(request, key).trim();
        if (value.isBlank()) throw new IllegalArgumentException(key + " 不能为空");
        return value;
    }

    private String value(Map<String, Object> request, String key) {
        return String.valueOf(request.getOrDefault(key, ""));
    }

    private record QuerySpec(String url, String username, String password, String sql, String tableName) { }
}
