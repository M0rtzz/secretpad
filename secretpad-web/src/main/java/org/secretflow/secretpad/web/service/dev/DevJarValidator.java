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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Z-05 JAR 制品上传校验（纯类）。
 *
 * <p>校验点：ZIP 魔数（{@code PK\x03\x04 / PK\x05\x06 / PK\x07\x08}）、含
 * {@code META-INF/MANIFEST.MF}（{@code Main-Class} 是否必须取决于运行契约——runner 用
 * {@code java -jar} 需要 Main-Class，缺失会在运行期明确报错，文档注明）、大小上限。
 * 不做全量解压扫描（运行时在一次性隔离容器内，恶意载荷无法外联）。</p>
 */
public final class DevJarValidator {

    private DevJarValidator() {
    }

    private static final String MANIFEST_PATH = "META-INF/MANIFEST.MF";

    /** ZIP 魔数（本地文件头 0x03 0x04 / 目录结束记录 0x05 0x06 / 分卷记录 0x07 0x08）。 */
    public static boolean isZip(byte[] bytes) {
        if (bytes == null || bytes.length < 4) {
            return false;
        }
        if (bytes[0] != 'P' || bytes[1] != 'K') {
            return false;
        }
        int b2 = bytes[2] & 0xff;
        int b3 = bytes[3] & 0xff;
        return (b2 == 0x03 && b3 == 0x04)
                || (b2 == 0x05 && b3 == 0x06)
                || (b2 == 0x07 && b3 == 0x08);
    }

    /** 可完整解压且含 {@code META-INF/MANIFEST.MF} 条目（大小写不敏感）。 */
    public static boolean hasManifest(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return false;
        }
        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(bytes))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (MANIFEST_PATH.equalsIgnoreCase(entry.getName())) {
                    return true;
                }
            }
        } catch (IOException e) {
            return false;
        }
        return false;
    }

    /** 完整校验：ZIP 魔数 + MANIFEST + 大小上限。 */
    public static void validate(byte[] bytes, long maxBytes) {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 文件为空");
        }
        if (bytes.length > maxBytes) {
            throw new IllegalArgumentException(DevErrors.DEV_INPUT_TOO_LARGE
                    + ": JAR " + bytes.length + " 字节超过上限 " + maxBytes);
        }
        if (!isZip(bytes)) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 非 ZIP/JAR 文件（魔数不符）");
        }
        if (!hasManifest(bytes)) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID
                    + ": 缺少 " + MANIFEST_PATH + "（不是可执行的 JAR）");
        }
    }
}
