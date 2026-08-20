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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the pure Z-05 JAR upload validator (no Spring dependency).
 */
public class DevJarValidatorTest {

    private static final String MANIFEST = "Manifest-Version: 1.0\r\nMain-Class: com.example.Main\r\n\r\n";

    /** Build an in-memory ZIP/JAR with the given entries (name → content). */
    private static byte[] zip(java.util.Map<String, String> entries) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(bos)) {
            for (java.util.Map.Entry<String, String> e : entries.entrySet()) {
                zos.putNextEntry(new ZipEntry(e.getKey()));
                zos.write(e.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8));
                zos.closeEntry();
            }
        }
        return bos.toByteArray();
    }

    @Test
    void acceptsValidJar() throws IOException {
        byte[] jar = zip(java.util.Map.of("META-INF/MANIFEST.MF", MANIFEST, "com/example/Main.class", "Ê"));
        DevJarValidator.validate(jar, 1_000_000);
        assertTrue(DevJarValidator.isZip(jar));
        assertTrue(DevJarValidator.hasManifest(jar));
    }

    @Test
    void rejectsNonZip() {
        byte[] notZip = "this is definitely not a zip file".getBytes();
        assertFalse(DevJarValidator.isZip(notZip));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> DevJarValidator.validate(notZip, 1_000_000));
        assertTrue(e.getMessage().contains("DEV_PARAM_INVALID"));
    }

    @Test
    void rejectsEmpty() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> DevJarValidator.validate(new byte[0], 1_000_000));
        assertTrue(e.getMessage().contains("DEV_PARAM_INVALID"));
    }

    @Test
    void rejectsMissingManifest() throws IOException {
        byte[] zipWithoutManifest = zip(java.util.Map.of("foo.txt", "hello"));
        assertTrue(DevJarValidator.isZip(zipWithoutManifest));
        assertFalse(DevJarValidator.hasManifest(zipWithoutManifest));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> DevJarValidator.validate(zipWithoutManifest, 1_000_000));
        assertTrue(e.getMessage().contains("MANIFEST"));
    }

    @Test
    void rejectsOversize() throws IOException {
        byte[] jar = zip(java.util.Map.of("META-INF/MANIFEST.MF", MANIFEST));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> DevJarValidator.validate(jar, 10));
        assertTrue(e.getMessage().contains("DEV_INPUT_TOO_LARGE"));
    }
}
