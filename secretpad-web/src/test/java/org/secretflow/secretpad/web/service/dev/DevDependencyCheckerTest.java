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
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the pure Z-05 Python dependency whitelist checker (no Spring dependency).
 */
public class DevDependencyCheckerTest {

    private static final Set<String> WHITELIST = Set.of("numpy", "pandas");

    @Test
    void extractsSimpleImport() {
        assertEquals(List.of("os"), DevDependencyChecker.extractImports("import os\nprint('x')"));
    }

    @Test
    void extractsFromImport() {
        assertEquals(List.of("datetime"), DevDependencyChecker.extractImports("from datetime import datetime"));
    }

    @Test
    void extractsTopLevelOfDottedImport() {
        assertEquals(List.of("collections"), DevDependencyChecker.extractImports("import collections.abc as abc"));
    }

    @Test
    void extractsTopLevelOfFromDottedImport() {
        assertEquals(List.of("urllib"), DevDependencyChecker.extractImports("from urllib.parse import urlparse"));
    }

    @Test
    void validatesWhitelisted() {
        DevDependencyChecker.validate("import numpy\nimport pandas as pd", WHITELIST);
    }

    @Test
    void allowsStdlib() {
        DevDependencyChecker.validate("import os\nimport json\nfrom pathlib import Path", WHITELIST);
    }

    @Test
    void allowsSqlite3Stdlib() {
        // E2E 修复：sqlite3 为标准库，FUNCTION 包装器/PYTHON 脚本直连沙箱 DB 快照需放行
        DevDependencyChecker.validate("import sqlite3", WHITELIST);
        DevDependencyChecker.validate("import os, sqlite3\nfrom sqlite3 import connect", WHITELIST);
    }

    @Test
    void noImportOk() {
        DevDependencyChecker.validate("print('hello')", WHITELIST);
    }

    @Test
    void rejectsNonWhitelisted() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> DevDependencyChecker.validate("import requests", WHITELIST));
        assertTrue(e.getMessage().contains("DEV_DEPENDENCY_REJECTED"));
        assertTrue(e.getMessage().contains("requests"));
    }

    @Test
    void rejectsRelativeImport() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> DevDependencyChecker.validate("from . import helper", WHITELIST));
        assertTrue(e.getMessage().contains("DEV_PARAM_INVALID"));
    }

    @Test
    void rejectsNullWhitelist() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> DevDependencyChecker.validate("import scipy", null));
        assertTrue(e.getMessage().contains("DEV_DEPENDENCY_REJECTED"));
    }
}
