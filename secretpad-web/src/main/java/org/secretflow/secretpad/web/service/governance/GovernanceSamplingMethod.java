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
 * Configurable sampling methods supported by the Z-04 governance engine.
 *
 * <p>All four methods are implemented in-process in
 * {@link GovernanceSamplingExecutor}; {@code from} is lenient (uppercase, blank-safe)
 * so persisted strategy JSON stays resilient to casing drift.</p>
 */
public enum GovernanceSamplingMethod {
    /** 随机抽样：按 count 或 ratio 从全量随机取子集，可固定 seed 复现。 */
    RANDOM,
    /** 等距/系统抽样：按 count 或 ratio 计算步长，每隔 step 取一行。 */
    SYSTEMATIC,
    /** 分层抽样：按 strataColumns 分组，每组按 count/ratio 取子集（至少 1 行）。 */
    STRATIFIED,
    /** 整群/分块抽样：按 clusterColumn 取值或连续块整群抽取，命中群的所有行。 */
    CLUSTER;

    public static GovernanceSamplingMethod from(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("sampling method is required");
        }
        return valueOf(value.trim().toUpperCase(java.util.Locale.ROOT));
    }
}
