/* Copyright 2026 Ant Group Co., Ltd. Licensed under the Apache License, Version 2.0. */
package org.secretflow.secretpad.web.service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;

/**
 * 数据目录为资产设置的访问 / 使用时间窗判定，由数据目录预览与沙箱挂载控制共用，
 * 保证同一份有效期在两处得到一致的解释。
 */
public final class AssetTimeWindow {
    private static final ZoneId DISPLAY_ZONE = ZoneId.of("Asia/Shanghai");

    private AssetTimeWindow() {
    }

    /** 当前时间落在窗口内返回 true；边界为空表示不限制，取值无法解析时按不限制处理。 */
    public static boolean within(Object start, Object end) {
        Instant now = Instant.now();
        Instant from = parse(start);
        Instant until = parse(end);
        return (from == null || !now.isBefore(from)) && (until == null || !now.isAfter(until));
    }

    private static Instant parse(Object value) {
        String text = value == null ? "" : String.valueOf(value).trim();
        if (text.isEmpty() || "null".equalsIgnoreCase(text)) return null;
        try { return OffsetDateTime.parse(text).toInstant(); } catch (DateTimeParseException ignored) { }
        try { return Instant.parse(text); } catch (DateTimeParseException ignored) { }
        try { return LocalDateTime.parse(text).atZone(DISPLAY_ZONE).toInstant(); } catch (DateTimeParseException ignored) { }
        return null;
    }
}
