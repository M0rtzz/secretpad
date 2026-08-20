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
import java.util.List;

/**
 * Hand-rolled RFC 4180 CSV reader/writer for the Z-04 governance engine.
 *
 * <p>Deliberately dependency-free: the MVP handles controlled CSV files only, so a small
 * parser is preferred over pulling in a CSV library. Quoted fields, escaped quotes
 * ({@code ""}), embedded commas and newlines, and both CRLF / LF line endings are handled;
 * the leading UTF-8 BOM is stripped on read. Values are preserved verbatim (no trimming).</p>
 */
public final class CsvUtil {

    private CsvUtil() {
    }

    /** Parse CSV text into rows of fields, stripping a leading BOM. */
    public static List<List<String>> parse(String content) {
        List<List<String>> rows = new ArrayList<>();
        if (content == null || content.isEmpty()) {
            return rows;
        }
        String csv = content.startsWith("﻿") ? content.substring(1) : content;
        List<String> row = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean inQuotes = false;
        int i = 0;
        int n = csv.length();
        while (i < n) {
            char c = csv.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < n && csv.charAt(i + 1) == '"') {
                        field.append('"');
                        i += 2;
                    } else {
                        inQuotes = false;
                        i++;
                    }
                } else {
                    field.append(c);
                    i++;
                }
            } else if (c == '"') {
                inQuotes = true;
                i++;
            } else if (c == ',') {
                row.add(field.toString());
                field.setLength(0);
                i++;
            } else if (c == '\n') {
                row.add(field.toString());
                field.setLength(0);
                rows.add(row);
                row = new ArrayList<>();
                i++;
            } else if (c == '\r') {
                if (i + 1 < n && csv.charAt(i + 1) == '\n') {
                    i++;
                }
                row.add(field.toString());
                field.setLength(0);
                rows.add(row);
                row = new ArrayList<>();
                i++;
            } else {
                field.append(c);
                i++;
            }
        }
        if (field.length() > 0 || !row.isEmpty()) {
            row.add(field.toString());
            rows.add(row);
        }
        return rows;
    }

    /** Serialize header + rows to CSV text (RFC 4180 quoting, LF line endings). */
    public static String toCsv(List<String> header, List<List<String>> rows) {
        StringBuilder sb = new StringBuilder();
        appendRow(sb, header);
        if (rows != null) {
            for (List<String> row : rows) {
                appendRow(sb, row);
            }
        }
        return sb.toString();
    }

    /** Quote a single field when it contains a separator, quote or line break. */
    public static String escape(String field) {
        if (field == null) {
            field = "";
        }
        if (field.indexOf(',') >= 0 || field.indexOf('"') >= 0 || field.indexOf('\n') >= 0
                || field.indexOf('\r') >= 0) {
            return "\"" + field.replace("\"", "\"\"") + "\"";
        }
        return field;
    }

    private static void appendRow(StringBuilder sb, List<String> row) {
        if (row == null || row.isEmpty()) {
            return;
        }
        for (int i = 0; i < row.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(escape(row.get(i)));
        }
        sb.append('\n');
    }
}
