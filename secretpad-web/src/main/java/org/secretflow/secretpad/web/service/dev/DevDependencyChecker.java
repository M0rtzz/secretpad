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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Z-05 Python 依赖库白名单校验（平台侧预检）。
 *
 * <p>用户已确认决策：白名单表（{@code ds_dev_dependency}）+ 运行时 import 校验。本类为提交前的
 * 平台侧预检（防御性）；权威校验在 runner 容器内（{@code builtins.__import__} 守卫 + 无网络 +
 * 仅预装白名单包，即使绕过平台侧也无法导入非白名单三方包）。</p>
 */
public final class DevDependencyChecker {

    private DevDependencyChecker() {
    }

    /** Python 标准库顶层模块（防御性预检集；权威 stdlib 集合在 runner 内 {@code sys.stdlib_module_names}）。 */
    public static final Set<String> STDLIB = Set.of(
            "os", "sys", "json", "csv", "math", "datetime", "time", "re", "collections",
            "itertools", "functools", "random", "statistics", "string", "typing", "logging",
            "io", "pathlib", "urllib", "hashlib", "base64", "tempfile", "subprocess", "textwrap",
            "unicodedata", "decimal", "fractions", "calendar", "argparse", "dataclasses", "enum",
            "types", "abc", "contextlib", "copy", "warnings", "traceback", "signal", "socket",
            "http", "ssl", "binascii", "struct", "array", "bisect", "heapq", "operator",
            "platform", "shutil", "glob", "gzip", "zipfile", "tarfile", "xml", "html", "numbers",
            "sysconfig", "concurrent", "asyncio", "threading", "queue", "weakref", "inspect",
            "importlib", "pkgutil", "codecs", "getpass", "secrets", "uuid", "zoneinfo", "contextvars",
            "sqlite3");

    private static final Pattern IMPORT_LINE = Pattern.compile("(?im)^\\s*import\\s+([a-zA-Z_][\\w.]*)");
    private static final Pattern FROM_LINE = Pattern.compile("(?im)^\\s*from\\s+([a-zA-Z_][\\w.]*)\\s+import");
    private static final Pattern RELATIVE_FROM = Pattern.compile("(?im)^\\s*from\\s+(\\.+)");

    /**
     * 提取脚本中引用的顶层模块名（{@code import x.y.z} / {@code from x.y import z} → {@code x}）。
     */
    public static List<String> extractImports(String script) {
        List<String> result = new ArrayList<>();
        if (script == null || script.isBlank()) {
            return result;
        }
        addMatches(result, IMPORT_LINE.matcher(script));
        addMatches(result, FROM_LINE.matcher(script));
        return result;
    }

    /**
     * 校验脚本所有顶层 import 均在（白名单 ∪ 标准库）内；相对导入直接拒绝。
     *
     * @param enabledWhitelist 启用的依赖白名单模块名（小写）
     */
    public static void validate(String script, Set<String> enabledWhitelist) {
        if (script == null || script.isBlank()) {
            return;
        }
        if (RELATIVE_FROM.matcher(script).find()) {
            throw new IllegalArgumentException(DevErrors.DEV_PARAM_INVALID + ": 脚本含相对导入（from .），仅支持顶层模块导入");
        }
        Set<String> whitelist = enabledWhitelist == null ? Set.of() : enabledWhitelist;
        for (String mod : extractImports(script)) {
            String top = mod.contains(".") ? mod.substring(0, mod.indexOf('.')) : mod;
            String key = top.toLowerCase(Locale.ROOT);
            if (!whitelist.contains(key) && !STDLIB.contains(key)) {
                throw new IllegalArgumentException(DevErrors.DEV_DEPENDENCY_REJECTED
                        + ": 依赖不在白名单: " + top + "（白名单: " + whitelist + "）");
            }
        }
    }

    private static void addMatches(List<String> result, Matcher matcher) {
        while (matcher.find()) {
            String mod = matcher.group(1);
            String top = mod.contains(".") ? mod.substring(0, mod.indexOf('.')) : mod;
            if (!result.contains(top)) {
                result.add(top);
            }
        }
    }
}
