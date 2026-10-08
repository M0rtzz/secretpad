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

import static org.secretflow.secretpad.web.service.SystemManagementSupport.PERMISSION_KEYS;
import static org.secretflow.secretpad.web.service.SystemManagementSupport.newId;
import static org.secretflow.secretpad.web.service.SystemManagementSupport.now;
import static org.secretflow.secretpad.web.service.SystemManagementSupport.requireAdmin;
import static org.secretflow.secretpad.web.service.SystemManagementSupport.stringList;
import static org.secretflow.secretpad.web.service.SystemManagementSupport.text;
import static org.secretflow.secretpad.web.service.SystemManagementSupport.validation;
import static org.secretflow.secretpad.web.service.SystemTenantService.activeAccountJoin;

import org.secretflow.secretpad.common.util.UserContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Node-local role management and user-tenant-role assignments.
 */
@Service
public class SystemRoleService {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final JdbcTemplate jdbc;
    private final SystemTenantService tenantService;

    @Value("${secretpad.auth.pad_name:admin}")
    private String adminName;

    public SystemRoleService(@Qualifier("jdbcTemplate") JdbcTemplate jdbc,
                             SystemTenantService tenantService) {
        this.jdbc = jdbc;
        this.tenantService = tenantService;
    }

    public List<Map<String, Object>> list() {
        requireAdmin(adminName);
        Map<String, Integer> memberCounts = new LinkedHashMap<>();
        jdbc.query("select ur.role_id, count(1) c from ds_user_role ur "
                        + activeAccountJoin("ur.account") + " group by ur.role_id",
                rs -> {
                    memberCounts.put(rs.getString("role_id"), rs.getInt("c"));
                },
                currentOwnerId());
        return jdbc.queryForList("select * from ds_role where deleted = 0 "
                        + "order by system_role desc, created_at desc")
                .stream()
                .map(row -> {
                    Map<String, Object> role = toRole(row);
                    role.put("memberCount", memberCounts.getOrDefault(String.valueOf(row.get("id")), 0));
                    return role;
                })
                .toList();
    }

    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> save(Map<String, Object> request) {
        requireAdmin(adminName);
        String id = text(request, "id");
        if (StringUtils.isNotEmpty(id)) {
            Map<String, Object> existing = requireRole(id);
            if (isSystemRole(existing)) {
                throw validation("系统预置角色不可编辑");
            }
        }
        String name = text(request, "name");
        if (name.length() < 2 || name.length() > 20) {
            throw validation("角色名称长度为 2 到 20 个字符");
        }
        String description = text(request, "description");
        if (description.length() > 100) {
            throw validation("角色说明不能超过 100 个字符");
        }
        List<String> permissions = stringList(request, "permissions").stream()
                .filter(PERMISSION_KEYS::contains)
                .toList();
        if (permissions.isEmpty()) {
            throw validation("请至少选择一项角色权限");
        }
        String permissionJson = writeJson(permissions);
        String now = now();
        try {
            if (StringUtils.isEmpty(id)) {
                id = newId("role");
                jdbc.update("insert into ds_role (id, name, description, permissions, system_role, "
                                + "created_at, updated_at, deleted) values (?, ?, ?, ?, 0, ?, ?, 0)",
                        id, name, description, permissionJson, now, now);
            } else {
                jdbc.update("update ds_role set name = ?, description = ?, permissions = ?, updated_at = ? "
                                + "where id = ? and deleted = 0",
                        name, description, permissionJson, now, id);
            }
        } catch (DataIntegrityViolationException e) {
            throw validation("角色名称已存在");
        }
        return toRole(requireRole(id));
    }

    @Transactional(rollbackFor = Exception.class)
    public void delete(Map<String, Object> request) {
        requireAdmin(adminName);
        String id = text(request, "id");
        Map<String, Object> role = requireRole(id);
        if (isSystemRole(role)) {
            throw validation("系统预置角色不可删除");
        }
        Integer members = jdbc.queryForObject("select count(1) from ds_user_role ur "
                        + activeAccountJoin("ur.account") + " where ur.role_id = ?",
                Integer.class, currentOwnerId(), id);
        if (members != null && members > 0) {
            throw validation("该角色仍有用户使用，无法删除");
        }
        jdbc.update("update ds_role set deleted = 1, updated_at = ? where id = ? and deleted = 0", now(), id);
        jdbc.update("delete from ds_user_role where role_id = ?", id);
    }

    /** 本节点全部有效账号的租户与角色分配。 */
    public List<Map<String, Object>> listAssignments() {
        requireAdmin(adminName);
        Map<String, Map<String, Object>> result = new LinkedHashMap<>();
        jdbc.query("select ua.account, ua.tenant_id from ds_user_assignment ua "
                        + activeAccountJoin("ua.account"),
                rs -> {
                    assignment(result, rs.getString("account")).put("tenantId", rs.getString("tenant_id"));
                },
                currentOwnerId());
        jdbc.query("select ur.account, ur.role_id from ds_user_role ur "
                        + "join ds_role r on r.id = ur.role_id and r.deleted = 0 "
                        + activeAccountJoin("ur.account") + " order by ur.account",
                rs -> {
                    @SuppressWarnings("unchecked")
                    List<String> roleIds = (List<String>) assignment(result, rs.getString("account")).get("roleIds");
                    roleIds.add(rs.getString("role_id"));
                },
                currentOwnerId());
        return new ArrayList<>(result.values());
    }

    /**
     * 保存单个账号的分配。租户与角色均为空时删除分配，账号恢复为“未分配”。
     */
    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> saveAssignment(Map<String, Object> request) {
        requireAdmin(adminName);
        String account = text(request, "account").toLowerCase(Locale.ROOT);
        requireManagedAccount(account);
        String tenantId = text(request, "tenantId");
        List<String> roleIds = stringList(request, "roleIds");
        // 已冻结租户不能新分配用户；保持原租户不变时允许保存，便于只调整角色
        String currentTenantId = jdbc.queryForList(
                "select tenant_id from ds_user_assignment where account = ?", String.class, account)
                .stream().findFirst().orElse("");
        if (StringUtils.isNotEmpty(tenantId) && !tenantId.equals(currentTenantId)) {
            tenantService.requireActiveTenant(tenantId);
        }
        for (String roleId : roleIds) {
            requireRole(roleId);
        }

        jdbc.update("delete from ds_user_role where account = ?", account);
        if (StringUtils.isEmpty(tenantId) && roleIds.isEmpty()) {
            jdbc.update("delete from ds_user_assignment where account = ?", account);
        } else {
            jdbc.update("insert into ds_user_assignment (account, tenant_id, updated_at) values (?, ?, ?) "
                    + "on conflict(account) do update set tenant_id = excluded.tenant_id, "
                    + "updated_at = excluded.updated_at", account, tenantId, now());
            for (String roleId : roleIds) {
                jdbc.update("insert into ds_user_role (account, role_id) values (?, ?)", account, roleId);
            }
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("account", account);
        result.put("tenantId", tenantId);
        result.put("roleIds", roleIds);
        return result;
    }

    private void requireManagedAccount(String account) {
        if (StringUtils.isEmpty(account) || account.equalsIgnoreCase(adminName)) {
            throw validation("管理员账号无需分配租户与角色");
        }
        Integer count = jdbc.queryForObject("select count(1) from user_accounts "
                        + "where lower(name) = ? and owner_id = ? and is_deleted = 0",
                Integer.class, account, currentOwnerId());
        if (count == null || count == 0) {
            throw validation("用户不存在或已删除");
        }
    }

    private Map<String, Object> requireRole(String id) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select * from ds_role where id = ? and deleted = 0", StringUtils.defaultString(id));
        if (rows.isEmpty()) {
            throw validation("角色不存在或已删除");
        }
        return rows.get(0);
    }

    private String currentOwnerId() {
        return UserContext.getUser().getOwnerId();
    }

    private static Map<String, Object> assignment(Map<String, Map<String, Object>> result, String account) {
        return result.computeIfAbsent(account, key -> {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("account", key);
            item.put("tenantId", "");
            item.put("roleIds", new ArrayList<String>());
            return item;
        });
    }

    private static boolean isSystemRole(Map<String, Object> row) {
        return SystemTenantService.toDouble(row.get("system_role")) != 0;
    }

    private static Map<String, Object> toRole(Map<String, Object> row) {
        Map<String, Object> role = new LinkedHashMap<>();
        role.put("id", row.get("id"));
        role.put("name", row.get("name"));
        role.put("description", row.get("description"));
        role.put("permissions", readPermissions(row.get("permissions")));
        role.put("system", isSystemRole(row));
        role.put("createdAt", row.get("created_at"));
        return role;
    }

    static List<String> readPermissions(Object value) {
        try {
            List<String> keys = JSON.readValue(String.valueOf(value), new TypeReference<List<String>>() {
            });
            return keys.stream().filter(PERMISSION_KEYS::contains).toList();
        } catch (JsonProcessingException | RuntimeException e) {
            return List.of();
        }
    }

    private static String writeJson(List<String> values) {
        try {
            return JSON.writeValueAsString(values);
        } catch (JsonProcessingException e) {
            throw validation("角色权限格式错误");
        }
    }
}
