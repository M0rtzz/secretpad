/*
 * Copyright 2023 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.secretflow.secretpad.web.service;

import org.secretflow.secretpad.common.errorcode.AuthErrorCode;
import org.secretflow.secretpad.common.errorcode.SystemErrorCode;
import org.secretflow.secretpad.common.exception.SecretpadException;
import org.secretflow.secretpad.common.util.UserContext;

import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Shared helpers for tenant, role and assignment management.
 */
final class SystemManagementSupport {

    /** 角色可配置的权限键，与前端菜单映射一致。 */
    static final List<String> PERMISSION_KEYS = List.of(
            "workbench:view", "project:manage", "node:manage", "data:catalog", "data:governance",
            "sandbox:apply", "sandbox:review", "compute:use", "model:review", "log:view");

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private SystemManagementSupport() {
    }

    static String now() {
        return LocalDateTime.now().format(TIME);
    }

    static String newId(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    static boolean isAdmin(String adminName) {
        return StringUtils.equalsIgnoreCase(adminName, UserContext.getUserName());
    }

    static void requireAdmin(String adminName) {
        if (!isAdmin(adminName)) {
            throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "仅管理员可执行该操作");
        }
    }

    static String text(Map<String, Object> request, String key) {
        Object value = request == null ? null : request.get(key);
        return value == null ? "" : StringUtils.trimToEmpty(String.valueOf(value));
    }

    static double number(Map<String, Object> request, String key, String label) {
        Object value = request == null ? null : request.get(key);
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        try {
            return Double.parseDouble(String.valueOf(value).trim());
        } catch (RuntimeException e) {
            throw validation(label + "必须为数字");
        }
    }

    static boolean bool(Map<String, Object> request, String key, boolean fallback) {
        Object value = request == null ? null : request.get(key);
        if (value == null) {
            return fallback;
        }
        return value instanceof Boolean flag ? flag : Boolean.parseBoolean(String.valueOf(value));
    }

    static List<String> stringList(Map<String, Object> request, String key) {
        Object value = request == null ? null : request.get(key);
        if (!(value instanceof Collection<?> items)) {
            return List.of();
        }
        return items.stream()
                .map(item -> StringUtils.trimToEmpty(String.valueOf(item)))
                .filter(StringUtils::isNotEmpty)
                .distinct()
                .toList();
    }

    static SecretpadException validation(String message) {
        return SecretpadException.of(SystemErrorCode.VALIDATION_ERROR, message);
    }
}
