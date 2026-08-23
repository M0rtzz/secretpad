/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.service.model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Z-06 受控模型 API 调用守卫（纯类）。
 *
 * <p>调用统一守卫顺序（在 {@code ModelApiService.invoke} 内对凭证调用与 User-Token 调用一致执行）：
 * 记录存在 → 启用 → 有效时间窗口 → 调用方 IP 白名单 → 授权用户名单。空 IP 白名单表示
 * 「不限制」，空授权列表表示仅允许凭据调用；时间窗口解析失败按失败关闭处理。</p>
 */
public final class ModelApiGuard {

    private ModelApiGuard() {
    }

    private static final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final String DISABLED = "DISABLED";

    /** API 状态是否为启用。 */
    public static boolean enabled(String status) {
        return !DISABLED.equalsIgnoreCase(status == null ? "" : status.trim());
    }

    /**
     * 当前时间是否在有效窗口内。validFrom/validTo 均空 → 不限制。
     *
     * @param validFrom 开始时间（yyyy-MM-dd HH:mm:ss，空=无下限）
     * @param validTo   结束时间（yyyy-MM-dd HH:mm:ss，空=无上限）
     * @param now       当前时间（yyyy-MM-dd HH:mm:ss）
     */
    public static boolean inValidityWindow(String validFrom, String validTo, String now) {
        if (isBlank(validFrom) && isBlank(validTo)) {
            return true;
        }
        Date nowDate = parse(now);
        if (nowDate == null) {
            return false;
        }
        if (!isBlank(validFrom)) {
            Date from = parse(validFrom);
            if (from == null || nowDate.before(from)) {
                return false;
            }
        }
        if (!isBlank(validTo)) {
            Date to = parse(validTo);
            if (to == null || nowDate.after(to)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 调用方 IP 是否在白名单内（空白名单 → 放行）。条目可为精确 IP 或 CIDR（{@code a.b.c.d/n}）。
     */
    public static boolean ipAllowed(String ip, List<String> whitelist) {
        if (whitelist == null || whitelist.isEmpty() || isBlank(ip)) {
            return true;
        }
        String candidate = ip.trim();
        for (String entry : whitelist) {
            String e = entry == null ? "" : entry.trim();
            if (e.isEmpty()) {
                continue;
            }
            if (e.contains("/")) {
                if (ipInCidr(candidate, e)) {
                    return true;
                }
            } else if (e.equals(candidate)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 调用方用户名是否在授权名单内（空授权列表 → 拒绝 User-Token 调用）。
     */
    public static boolean userAllowed(String name, List<String> authorizedUsers) {
        if (authorizedUsers == null || authorizedUsers.isEmpty() || isBlank(name)) {
            return false;
        }
        for (String u : authorizedUsers) {
            if (u != null && u.trim().equalsIgnoreCase(name.trim())) {
                return true;
            }
        }
        return false;
    }

    private static boolean ipInCidr(String ip, String cidr) {
        try {
            String[] parts = cidr.split("/");
            int prefix = parts.length == 1 ? 32 : Integer.parseInt(parts[1].trim());
            if (prefix < 0 || prefix > 32) {
                return false;
            }
            int ipInt = ipToInt(ip);
            int netInt = ipToInt(parts[0].trim());
            int mask = prefix == 0 ? 0 : (0xFFFFFFFF << (32 - prefix)) & 0xFFFFFFFF;
            return (ipInt & mask) == (netInt & mask);
        } catch (Exception e) {
            return false;
        }
    }

    private static int ipToInt(String ip) {
        String[] octets = ip.split("\\.");
        if (octets.length != 4) {
            throw new IllegalArgumentException("invalid ip");
        }
        int value = 0;
        for (String octet : octets) {
            int o = Integer.parseInt(octet.trim());
            if (o < 0 || o > 255) {
                throw new IllegalArgumentException("invalid ip octet");
            }
            value = (value << 8) | o;
        }
        return value;
    }

    private static Date parse(String text) {
        try {
            // 兼容 ISO 'T' 分隔（ModelApiService.now() 用 LocalDateTime.toString()）与空格分隔（外部配置）
            String normalized = text.trim();
            if (normalized.contains("T")) {
                normalized = normalized.replace('T', ' ');
            }
            return new SimpleDateFormat(DATETIME_PATTERN).parse(normalized);
        } catch (ParseException e) {
            return null;
        }
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
