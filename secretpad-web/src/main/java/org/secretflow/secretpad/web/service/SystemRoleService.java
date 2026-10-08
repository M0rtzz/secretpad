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

import static org.secretflow.secretpad.web.service.SystemManagementSupport.ADMIN_ROLE_ID;
import static org.secretflow.secretpad.web.service.SystemManagementSupport.PERMISSION_KEYS;
import static org.secretflow.secretpad.web.service.SystemManagementSupport.ROLE_MANAGE;
import static org.secretflow.secretpad.web.service.SystemManagementSupport.USER_MANAGE;
import static org.secretflow.secretpad.web.service.SystemManagementSupport.newId;
import static org.secretflow.secretpad.web.service.SystemManagementSupport.now;
import static org.secretflow.secretpad.web.service.SystemManagementSupport.stringList;
import static org.secretflow.secretpad.web.service.SystemManagementSupport.text;
import static org.secretflow.secretpad.web.service.SystemManagementSupport.validation;
import static org.secretflow.secretpad.web.service.SystemTenantService.activeAccountJoin;

import org.secretflow.secretpad.common.util.UserContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Node-local role management and user-tenant-role assignments.
 */
@Service
public class SystemRoleService {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final JdbcTemplate jdbc;
    private final SystemTenantService tenantService;
    private final SystemAccessService access;

    public SystemRoleService(@Qualifier("jdbcTemplate") JdbcTemplate jdbc,
                             SystemTenantService tenantService,
                             SystemAccessService access) {
        this.jdbc = jdbc;
        this.tenantService = tenantService;
        this.access = access;
    }

    public List<Map<String, Object>> list() {
        access.requireAny(ROLE_MANAGE, USER_MANAGE);
        Map<String, Integer> memberCounts = new LinkedHashMap<>();
        jdbc.query("select ur.role_id, count(1) c from ds_user_role ur "
                        + activeAccountJoin("ur.account") + " group by ur.role_id",
                rs -> {
                    memberCounts.put(rs.getString("role_id"), rs.getInt("c"));
                },
                currentOwnerId());
        // 节点管理员账号固定持有沙箱管理员角色，不落库但计入成员数
        memberCounts.merge(ADMIN_ROLE_ID, 1, Integer::sum);
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
        access.requireAny(ROLE_MANAGE);
        String id = text(request, "id");
        if (StringUtils.isNotEmpty(id)) {
            Map<String, Object> existing = requireRole(id);
            if (isSystemRole(existing)) {
                throw validation("系统预置角色不可编辑");
            }
            access.assertCanGrant(SystemAccessService.readPermissions(existing.get("permissions")));
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
        access.assertCanGrant(permissions);
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
        access.requireAny(ROLE_MANAGE);
        String id = text(request, "id");
        Map<String, Object> role = requireRole(id);
        if (isSystemRole(role)) {
            throw validation("系统预置角色不可删除");
        }
        access.assertCanGrant(SystemAccessService.readPermissions(role.get("permissions")));
        Integer members = jdbc.queryForObject("select count(1) from ds_user_role ur "
                        + activeAccountJoin("ur.account") + " where ur.role_id = ?",
                Integer.class, currentOwnerId(), id);
        if (members != null && members > 0) {
            throw validation("该角色仍有用户使用，无法删除");
        }
        jdbc.update("update ds_role set deleted = 1, updated_at = ? where id = ? and deleted = 0", now(), id);
        jdbc.update("delete from ds_user_role where role_id = ?", id);
    }

    /**
     * 有效账号的租户与角色分配。全节点管理者可见全部账号，节点管理员固定为沙箱管理员且锁定；
     * 租户范围内的管理者只能看到本租户的账号。
     */
    public List<Map<String, Object>> listAssignments() {
        access.requireAny(USER_MANAGE);
        String scope = access.managedTenantScope();
        Map<String, Map<String, Object>> result = new LinkedHashMap<>();
        if (scope == null) {
            Map<String, Object> admin = assignment(result, access.adminAccount());
            admin.put("roleIds", new ArrayList<>(List.of(ADMIN_ROLE_ID)));
            admin.put("locked", true);
        }
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
        return result.values().stream()
                .filter(item -> scope == null || (!scope.isEmpty() && scope.equals(item.get("tenantId"))))
                .toList();
    }

    /** 保存已有账号的分配，受 G1 至 G4 与租户范围约束。 */
    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> saveAssignment(Map<String, Object> request) {
        access.requireAny(USER_MANAGE);
        String account = text(request, "account").toLowerCase(Locale.ROOT);
        requireLocalAccount(account);
        access.assertCanManage(account);
        String tenantId = text(request, "tenantId");
        List<String> roleIds = stringList(request, "roleIds");
        validateAssignment(account, tenantId, roleIds, access.tenantOf(account));
        writeAssignment(account, tenantId, roleIds);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("account", account);
        result.put("tenantId", tenantId);
        result.put("roleIds", roleIds);
        return result;
    }

    /**
     * 校验分配内容：普通用户必须归属租户，沙箱管理员不归属租户；所授角色不得超出操作者权限；
     * 租户范围内的管理者只能分配到本租户；新选择的租户须为正常状态，且转入的沙箱占用不超过其配额。
     *
     * @param currentTenantId 账号当前租户，新建账号传空串
     */
    void validateAssignment(String account, String tenantId, List<String> roleIds, String currentTenantId) {
        Set<String> granted = new LinkedHashSet<>();
        for (String roleId : roleIds) {
            granted.addAll(SystemAccessService.readPermissions(requireRole(roleId).get("permissions")));
        }
        access.assertCanGrant(granted);
        if (roleIds.contains(ADMIN_ROLE_ID)) {
            if (StringUtils.isNotEmpty(tenantId)) {
                throw validation("沙箱管理员不归属租户，请清空所属租户后再保存");
            }
            return;
        }
        if (StringUtils.isEmpty(tenantId)) {
            throw validation("请选择所属租户");
        }
        String scope = access.managedTenantScope();
        if (scope != null && !scope.equals(tenantId)) {
            throw validation("只能将用户分配到本租户");
        }
        // 已冻结租户不能新分配用户；保持原租户不变时允许保存，便于只调整角色
        if (!tenantId.equals(currentTenantId)) {
            tenantService.requireActiveTenant(tenantId);
            tenantService.assertTransferWithinQuota(account, tenantId);
        }
    }

    void writeAssignment(String account, String tenantId, List<String> roleIds) {
        jdbc.update("delete from ds_user_role where account = ?", account);
        jdbc.update("insert into ds_user_assignment (account, tenant_id, updated_at) values (?, ?, ?) "
                + "on conflict(account) do update set tenant_id = excluded.tenant_id, "
                + "updated_at = excluded.updated_at", account, tenantId, now());
        for (String roleId : roleIds) {
            jdbc.update("insert into ds_user_role (account, role_id) values (?, ?)", account, roleId);
        }
    }

    private void requireLocalAccount(String account) {
        Integer count = jdbc.queryForObject("select count(1) from user_accounts "
                        + "where lower(name) = ? and owner_id = ? and is_deleted = 0",
                Integer.class, account, currentOwnerId());
        if (StringUtils.isEmpty(account) || count == null || count == 0) {
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
            item.put("locked", false);
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
        role.put("permissions", SystemAccessService.readPermissions(row.get("permissions")));
        role.put("system", isSystemRole(row));
        role.put("createdAt", row.get("created_at"));
        return role;
    }

    private static String writeJson(List<String> values) {
        try {
            return JSON.writeValueAsString(values);
        } catch (JsonProcessingException e) {
            throw validation("角色权限格式错误");
        }
    }
}
