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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the hand-rolled RFC 4180 CSV reader/writer (no Spring dependency).
 */
public class CsvUtilTest {

    @Test
    void parsesSimpleRows() {
        List<List<String>> rows = CsvUtil.parse("id,name,amount\n1,a,10.5\n2,b,20");
        assertEquals(3, rows.size());
        assertEquals(List.of("id", "name", "amount"), rows.get(0));
        assertEquals(List.of("2", "b", "20"), rows.get(2));
    }

    @Test
    void handlesQuotedFieldsWithSeparators() {
        List<List<String>> rows = CsvUtil.parse("\"a,1\",\"b\"\"2\",\"line\nbreak\"");
        assertEquals(1, rows.size());
        assertEquals(List.of("a,1", "b\"2", "line\nbreak"), rows.get(0));
    }

    @Test
    void handlesCrlfLineEndings() {
        List<List<String>> rows = CsvUtil.parse("a,b\r\n1,2\r\n3,4\r\n");
        assertEquals(3, rows.size());
        assertEquals(List.of("1", "2"), rows.get(1));
    }

    @Test
    void stripsLeadingBom() {
        List<List<String>> rows = CsvUtil.parse("﻿id,name\n1,alice");
        assertEquals(List.of("id", "name"), rows.get(0));
    }

    @Test
    void emptyAndNullInputProduceEmpty() {
        assertTrue(CsvUtil.parse(null).isEmpty());
        assertTrue(CsvUtil.parse("").isEmpty());
    }

    @Test
    void toCsvRoundTrips() {
        List<String> header = List.of("id", "name", "phone");
        List<List<String>> rows = List.of(
                List.of("1", "alice", "13800001234"),
                List.of("2", "bob, jr", "no phone"));
        String csv = CsvUtil.toCsv(header, rows);
        assertTrue(csv.startsWith("id,name,phone\n"));
        // 含逗号字段被引号包裹
        assertTrue(csv.contains("\"bob, jr\""));
        assertEquals(List.of(header, rows.get(0), rows.get(1)), CsvUtil.parse(csv));
    }

    @Test
    void escapeQuotesWhenNeeded() {
        assertEquals("plain", CsvUtil.escape("plain"));
        assertEquals("\"a\"\"b\"", CsvUtil.escape("a\"b"));
        assertEquals("\"x,y\"", CsvUtil.escape("x,y"));
        assertEquals("", CsvUtil.escape(null));
    }
}
