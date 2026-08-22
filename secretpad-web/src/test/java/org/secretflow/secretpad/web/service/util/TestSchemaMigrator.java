/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.service.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * 测试迁移助手：为全新测试库提供「去重后」的 Flyway 迁移集。
 *
 * <p>现状：仓库 schema 中 {@code V13__model_test.sql} 与 {@code V17__data_sandbox_model_test.sql}
 * 内容完全一致（历史分支合并残留），全量迁移会在全新库上因 duplicate column 失败。所有已部署库
 * （{@code .dev-runtime/*} 三个节点）的 flyway_schema_history 均只应用了 V17、从未应用 V13，
 * 因此线上真实迁移集 = 现有 schema 去掉 V13。测试从 {@code secretpad/} 模块目录启动时，
 * 将 {@code ./config/schema/{profile}} 复制到临时目录并剔除 V13，得到与线上一致的迁移集
 * （含 V27），仅用于测试隔离 —— 不改动仓库 schema 文件。</p>
 */
public final class TestSchemaMigrator {

    private TestSchemaMigrator() {
    }

    /** 返回去重后的迁移位置（filesystem:…）；须以 secretpad 模块目录为工作目录运行。 */
    public static String dedupedLocation(String profile) {
        Path src = Path.of("config", "schema", profile);
        if (!Files.isDirectory(src)) {
            throw new IllegalStateException("测试须在 secretpad 模块目录下运行（未找到 " + src.toAbsolutePath() + "）");
        }
        Path target = Path.of(System.getProperty("java.io.tmpdir"), "zgz-it-schema-" + profile);
        try {
            if (Files.exists(target)) {
                try (var walk = Files.walk(target)) {
                    for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                        Files.deleteIfExists(p);
                    }
                }
            }
            Files.createDirectories(target);
            try (var walk = Files.walk(src)) {
                for (Path p : walk.filter(Files::isRegularFile).toList()) {
                    if (p.getFileName().toString().startsWith("V13")) {
                        continue; // 剔除与 V17 完全重复的迁移
                    }
                    Path dest = target.resolve(src.relativize(p).toString());
                    Files.createDirectories(dest.getParent());
                    Files.copy(p, dest);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return "filesystem:" + target.toAbsolutePath();
    }
}
