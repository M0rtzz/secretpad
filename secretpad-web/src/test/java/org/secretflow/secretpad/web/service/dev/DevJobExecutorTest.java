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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link DevJobExecutor#extractFailureReason}（Z-05 失败任务调试日志原因提取）。
 * 纯静态方法，无 Spring 依赖。
 */
public class DevJobExecutorTest {

    @Test
    void extractsExecutionFailedLineAndStripsPythonPrefix() {
        String log = """
                [py] running: /usr/local/bin/python /tmp/py/script_guarded.py --input /tmp/py/input.csv ...
                Traceback (most recent call last):
                  File "/tmp/py/script_guarded.py", line 24, in main
                    __import__('requests')
                  File "/tmp/py/script_guarded.py", line 18, in _ds_guarded_import
                    raise ImportError("dependency not allowed: " + _ds_top)
                ImportError: dependency not allowed: requests
                [py] EXECUTION FAILED: py failed rc=1: ImportError: dependency not allowed: requests
                """;
        String reason = DevJobExecutor.extractFailureReason(log);
        assertTrue(reason.contains("dependency not allowed: requests"), reason);
        assertTrue(!reason.contains("[py]"), "should strip the [py] runner prefix: " + reason);
    }

    @Test
    void extractsExecutionFailedLineAndStripsJarPrefix() {
        String log = """
                [jar] running: java -jar /app/app.jar --input /tmp/jar/input.csv ...
                Exception in thread "main" java.lang.IllegalArgumentException: groupColumn not found: category
                [jar] EXECUTION FAILED: jar failed rc=1: java.lang.IllegalArgumentException: groupColumn not found: category
                """;
        String reason = DevJobExecutor.extractFailureReason(log);
        assertTrue(reason.contains("groupColumn not found"), reason);
        assertTrue(!reason.contains("[jar]"), "should strip the [jar] runner prefix: " + reason);
    }

    @Test
    void fallsBackToImportErrorWhenNoExecutionFailedLine() {
        String log = "[py] running: python3 ...\n"
                + "Traceback (most recent call last):\n"
                + "ImportError: dependency not allowed: some_pkg\n";
        String reason = DevJobExecutor.extractFailureReason(log);
        assertTrue(reason.contains("dependency not allowed: some_pkg"), reason);
    }

    @Test
    void fallsBackToTimedOutLine() {
        String log = "[py] running: python3 ...\n" + "[py] timed out after 240s\n";
        assertEquals("[py] timed out after 240s", DevJobExecutor.extractFailureReason(log));
    }

    @Test
    void emptyOrBlankLogYieldsEmptyReason() {
        assertEquals("", DevJobExecutor.extractFailureReason(""));
        assertEquals("", DevJobExecutor.extractFailureReason("   \n  \n"));
        assertEquals("", DevJobExecutor.extractFailureReason(null));
    }
}
