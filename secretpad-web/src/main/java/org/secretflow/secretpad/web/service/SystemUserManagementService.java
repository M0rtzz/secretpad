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
import org.secretflow.secretpad.common.util.Sha256Utils;
import org.secretflow.secretpad.common.util.UserContext;
import org.secretflow.secretpad.web.service.model.ModelApiService;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Real account management backed by the login database.
 */
@Service
public class SystemUserManagementService {

    public static final String INITIAL_PASSWORD = "HUSTnlp2026!";
    private static final String ENABLED = "ENABLED";
    private static final String DISABLED = "DISABLED";
    private static final Pattern ACCOUNT_PATTERN =
            Pattern.compile("^[a-z][a-z0-9._-]{2,15}$");

    private final JdbcTemplate jdbc;
    private final ModelApiService modelApiService;

    @Value("${secretpad.auth.pad_name:admin}")
    private String adminName;

    public SystemUserManagementService(
            @Qualifier("jdbcTemplate") JdbcTemplate jdbc,
            ModelApiService modelApiService) {
        this.jdbc = jdbc;
        this.modelApiService = modelApiService;
    }

    public List<Map<String, Object>> list() {
        requireAdmin();
        String ownerId = UserContext.getUser().getOwnerId();
        return jdbc.query(
                "select name, display_name, account_status, last_login_at, gmt_create "
                        + "from user_accounts "
                        + "where owner_id = ? and is_deleted = 0 "
                        + "order by gmt_create desc",
                (rs, rowNum) -> toUser(rs),
                ownerId);
    }

    /**
     * Enabled accounts available for username-based authorization.
     *
     * <p>This endpoint deliberately includes the configured administrator and
     * is separate from the administrator-only management list.</p>
     */
    public List<Map<String, Object>> authorizationOptions() {
        String ownerId = UserContext.getUser().getOwnerId();
        return jdbc.query(
                "select name, display_name from user_accounts "
                        + "where (owner_id = ? or lower(name) = lower(?)) and is_deleted = 0 "
                        + "and account_status = 'ENABLED' order by name",
                (rs, rowNum) -> {
                    Map<String, Object> option = new LinkedHashMap<>();
                    option.put("account", rs.getString("name"));
                    option.put("displayName", StringUtils.defaultIfBlank(
                            rs.getString("display_name"), rs.getString("name")));
                    return option;
                },
                ownerId,
                adminName);
    }

    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> create(Map<String, Object> request) {
        requireAdmin();
        String account = normalizeAccount(value(request, "account"));
        String displayName = normalizeDisplayName(value(request, "displayName"), account);
        String ownerId = UserContext.getUser().getOwnerId();
        if (account.equalsIgnoreCase(adminName)) {
            validationError("管理员账户名不可用于普通用户");
        }
        if (activeAccountExists(account)) {
            validationError("账户名已存在");
        }

        try {
            revokeSessions(account);
            jdbc.update("delete from sys_user_permission_rel where lower(user_key) = ?", account);
            jdbc.update("delete from sys_user_node_rel where lower(user_id) = ?", account);

            jdbc.update(
                    "insert into user_accounts "
                            + "(name, password_hash, owner_type, owner_id, display_name, "
                            + "account_status, last_login_at, inst_id, is_deleted) "
                            + "values (?, ?, 'EDGE', ?, ?, 'ENABLED', null, '', 0)",
                    account,
                    Sha256Utils.hash(INITIAL_PASSWORD),
                    ownerId,
                    displayName);
            jdbc.update(
                    "insert into sys_user_permission_rel "
                            + "(user_type, user_key, target_type, target_code) "
                            + "values ('EDGE_USER', ?, 'ROLE', 'EDGE_USER')",
                    account);
        } catch (DataIntegrityViolationException e) {
            throw SecretpadException.of(
                    SystemErrorCode.VALIDATION_ERROR, e, "账户名已存在");
        }
        return requireManagedUser(account, ownerId);
    }

    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> update(Map<String, Object> request) {
        requireAdmin();
        String account = normalizeAccount(value(request, "account"));
        String ownerId = UserContext.getUser().getOwnerId();
        requireManagedUser(account, ownerId);
        String displayName = normalizeDisplayName(value(request, "displayName"), account);
        jdbc.update(
                "update user_accounts set display_name = ?, gmt_modified = CURRENT_TIMESTAMP "
                        + "where lower(name) = ? and owner_id = ? and is_deleted = 0",
                displayName,
                account,
                ownerId);
        return requireManagedUser(account, ownerId);
    }

    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> changeStatus(Map<String, Object> request) {
        requireAdmin();
        String account = normalizeAccount(value(request, "account"));
        String status = value(request, "status").trim().toUpperCase(Locale.ROOT);
        if (!ENABLED.equals(status) && !DISABLED.equals(status)) {
            validationError("用户状态只能为 ENABLED 或 DISABLED");
        }
        String ownerId = UserContext.getUser().getOwnerId();
        Map<String, Object> user = requireManagedUser(account, ownerId);
        String storedName = String.valueOf(user.get("account"));
        jdbc.update(
                "update user_accounts set account_status = ?, gmt_modified = CURRENT_TIMESTAMP "
                        + "where name = ? and owner_id = ? and is_deleted = 0",
                status,
                storedName,
                ownerId);
        if (DISABLED.equals(status)) {
            revokeSessions(storedName);
        }
        return requireManagedUser(account, ownerId);
    }

    @Transactional(rollbackFor = Exception.class)
    public void resetPassword(Map<String, Object> request) {
        requireAdmin();
        String account = normalizeAccount(value(request, "account"));
        String ownerId = UserContext.getUser().getOwnerId();
        Map<String, Object> user = requireManagedUser(account, ownerId);
        String storedName = String.valueOf(user.get("account"));
        jdbc.update(
                "update user_accounts set password_hash = ?, failed_attempts = null, "
                        + "locked_invalid_time = null, passwd_reset_failed_attempts = null, "
                        + "gmt_passwd_reset_release = null, gmt_modified = CURRENT_TIMESTAMP "
                        + "where name = ? and owner_id = ? and is_deleted = 0",
                Sha256Utils.hash(INITIAL_PASSWORD),
                storedName,
                ownerId);
        revokeSessions(storedName);
    }

    @Transactional(rollbackFor = Exception.class)
    public void delete(Map<String, Object> request) {
        requireAdmin();
        String account = normalizeAccount(value(request, "account"));
        String ownerId = UserContext.getUser().getOwnerId();
        Map<String, Object> user = requireManagedUser(account, ownerId);
        String storedName = String.valueOf(user.get("account"));
        revokeSessions(storedName);
        jdbc.update("delete from sys_user_permission_rel where lower(user_key) = ?", account);
        jdbc.update("delete from sys_user_node_rel where lower(user_id) = ?", account);
        modelApiService.removeAuthorizedUser(storedName);
        jdbc.update(
                "update user_accounts set is_deleted = 1, gmt_modified = CURRENT_TIMESTAMP "
                        + "where name = ? and owner_id = ? and is_deleted = 0",
                storedName,
                ownerId);
    }

    private Map<String, Object> requireManagedUser(String account, String ownerId) {
        List<Map<String, Object>> users = jdbc.query(
                "select name, display_name, account_status, last_login_at, gmt_create "
                        + "from user_accounts "
                        + "where lower(name) = ? and owner_id = ? and is_deleted = 0 "
                        + "and lower(name) <> lower(?)",
                (rs, rowNum) -> toUser(rs),
                account,
                ownerId,
                adminName);
        if (users.isEmpty()) {
            validationError("用户不存在或已删除");
        }
        return users.get(0);
    }

    private boolean activeAccountExists(String account) {
        Integer count = jdbc.queryForObject(
                "select count(1) from user_accounts "
                        + "where lower(name) = ? and is_deleted = 0",
                Integer.class,
                account);
        return count != null && count > 0;
    }

    private Map<String, Object> toUser(ResultSet rs) throws SQLException {
        Map<String, Object> user = new LinkedHashMap<>();
        String account = rs.getString("name");
        String displayName = rs.getString("display_name");
        user.put("account", account);
        user.put("displayName", StringUtils.defaultIfBlank(displayName, account));
        user.put("status", rs.getString("account_status"));
        user.put("lastLoginAt", rs.getString("last_login_at"));
        user.put("createdAt", rs.getString("gmt_create"));
        user.put("systemAccount", StringUtils.equalsIgnoreCase(adminName, account));
        return user;
    }

    private void revokeSessions(String account) {
        jdbc.update("delete from user_tokens where lower(name) = lower(?)", account);
    }

    private void requireAdmin() {
        if (!StringUtils.equalsIgnoreCase(adminName, UserContext.getUserName())) {
            throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "仅管理员可管理用户");
        }
    }

    private String normalizeAccount(String account) {
        String normalized = StringUtils.trimToEmpty(account).toLowerCase(Locale.ROOT);
        if (!ACCOUNT_PATTERN.matcher(normalized).matches()) {
            validationError("账户名须以字母开头，由 3-16 位小写字母、数字、点、下划线或短横线组成");
        }
        return normalized;
    }

    private String normalizeDisplayName(String displayName, String account) {
        String normalized = StringUtils.defaultIfBlank(StringUtils.trim(displayName), account);
        if (normalized.length() > 64) {
            validationError("用户名称不能超过 64 个字符");
        }
        return normalized;
    }

    private String value(Map<String, Object> request, String key) {
        Object value = request == null ? null : request.get(key);
        return value == null ? "" : String.valueOf(value);
    }

    private void validationError(String message) {
        throw SecretpadException.of(SystemErrorCode.VALIDATION_ERROR, message);
    }
}
