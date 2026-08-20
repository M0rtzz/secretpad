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

/**
 * Desensitization methods supported by the Z-04 governance engine.
 *
 * <p>All are applied per column by {@link GovernanceMaskingExecutor}; {@code from} is lenient
 * (uppercase, blank-safe) like {@link GovernanceSamplingMethod#from}.</p>
 */
public enum GovernanceMaskingMethod {
    /** 掩码：保留前 keepLeft / 后 keepRight 位，中间以 maskChar 填充（如 138****1234）。 */
    MASK,
    /** 替换：整列替换为常量 value，或按 mapping 精确映射。 */
    REPLACE,
    /** 哈希：SHA-256 + 每列盐，不可逆。 */
    HASH,
    /** 取整：数值按 digits 位小数取整（HALF_UP）。 */
    ROUND,
    /** 空值/清除：mode=drop 时整列删除；否则置空。 */
    CLEAR;

    public static GovernanceMaskingMethod from(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("masking method is required");
        }
        return valueOf(value.trim().toUpperCase(java.util.Locale.ROOT));
    }
}
