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
import static org.secretflow.secretpad.web.service.SystemManagementSupport.BUSINESS_PERMISSION_KEYS;
import static org.secretflow.secretpad.web.service.SystemManagementSupport.PERMISSION_KEYS;

import org.secretflow.secretpad.common.errorcode.AuthErrorCode;
import org.secretflow.secretpad.common.exception.SecretpadException;
import org.secretflow.secretpad.common.util.UserContext;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Effective permissions, management scope and anti-escalation rules of system management.
 *
 * <p>The node administrator ({@code pad_name}) always holds every permission; its identity
 * does not depend on any editable data, so it can never be locked out.</p>
 */
@Service
public class SystemAccessService {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final JdbcTemplate jdbc;

    @Value("${secretpad.auth.pad_name:admin}")
    private String adminName;

    public SystemAccessService(@Qualifier("jdbcTemplate") JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public String adminAccount() {
        return adminName.toLowerCase(Locale.ROOT);
    }

    public boolean isNodeAdmin(String account) {
        return StringUtils.equalsIgnoreCase(adminName, account);
    }

    public String currentAccount() {
        return StringUtils.defaultString(UserContext.getUserName()).toLowerCase(Locale.ROOT);
    }

    /** 节点管理员或持有沙箱管理员角色：管理范围为全节点，且不归属租户。 */
    public boolean isSandboxAdmin(String account) {
        return isNodeAdmin(account) || roleIdsOf(account).contains(ADMIN_ROLE_ID);
    }

    public List<String> roleIdsOf(String account) {
        return jdbc.queryForList("select ur.role_id from ds_user_role ur "
                        + "join ds_role r on r.id = ur.role_id and r.deleted = 0 where ur.account = ?",
                String.class, normalize(account));
    }

    /** 已分配角色时取角色权限并集；未分配角色时为全部业务权限（与升级前一致），不含系统管理。 */
    public Set<String> permissionsOf(String account) {
        if (isNodeAdmin(account)) {
            return new LinkedHashSet<>(PERMISSION_KEYS);
        }
        List<String> rows = jdbc.queryForList("select r.permissions from ds_user_role ur "
                        + "join ds_role r on r.id = ur.role_id and r.deleted = 0 where ur.account = ?",
                String.class, normalize(account));
        if (rows.isEmpty()) {
            return new LinkedHashSet<>(BUSINESS_PERMISSION_KEYS);
        }
        Set<String> result = new LinkedHashSet<>();
        rows.forEach(json -> result.addAll(readPermissions(json)));
        return result;
    }

    /** 账号所属租户，未归属时为空串。 */
    public String tenantOf(String account) {
        return jdbc.queryForList("select tenant_id from ds_user_assignment where account = ?",
                        String.class, normalize(account))
                .stream().findFirst().orElse("");
    }

    /**
     * 当前操作者的管理范围：返回 {@code null} 表示全节点；否则只能管理该租户（空串表示无可管理范围）。
     */
    public String managedTenantScope() {
        String current = currentAccount();
        return isSandboxAdmin(current) ? null : tenantOf(current);
    }

    public void requireAny(String... permissionKeys) {
        Set<String> permissions = permissionsOf(currentAccount());
        for (String key : permissionKeys) {
            if (permissions.contains(key)) {
                return;
            }
        }
        throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "当前账号无权执行该操作");
    }

    /** G3：授予或定义的权限不得超出操作者自身权限。 */
    public void assertCanGrant(Collection<String> permissionKeys) {
        if (!permissionsOf(currentAccount()).containsAll(permissionKeys)) {
            throw SystemManagementSupport.validation("不能授予或配置超出自身权限的角色");
        }
    }

    /** G1、G2、G4 与租户范围：校验当前操作者能否管理目标账号。 */
    public void assertCanManage(String targetAccount) {
        String target = normalize(targetAccount);
        String current = currentAccount();
        if (target.equals(current)) {
            throw SystemManagementSupport.validation("不能修改自己的账号、角色与租户");
        }
        if (isNodeAdmin(target)) {
            throw SystemManagementSupport.validation("节点管理员账号不可修改");
        }
        String scope = managedTenantScope();
        if (scope != null && (scope.isEmpty() || !scope.equals(tenantOf(target)))) {
            throw SystemManagementSupport.validation("只能管理本租户的用户");
        }
        if (!permissionsOf(current).containsAll(permissionsOf(target))) {
            throw SystemManagementSupport.validation("不能管理权限高于自己的账号");
        }
    }

    /** 当前登录用户的权限上下文，供前端过滤菜单与操作。 */
    public Map<String, Object> context() {
        String account = currentAccount();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("account", account);
        result.put("admin", isNodeAdmin(account));
        result.put("sandboxAdmin", isSandboxAdmin(account));
        result.put("hasRoles", isNodeAdmin(account) || !roleIdsOf(account).isEmpty());
        String tenantId = isSandboxAdmin(account) ? "" : tenantOf(account);
        result.put("tenantId", tenantId);
        result.put("tenantName", tenantId.isEmpty() ? "" : jdbc.queryForList(
                        "select name from ds_tenant where id = ? and deleted = 0", String.class, tenantId)
                .stream().findFirst().orElse(""));
        result.put("permissions", permissionsOf(account));
        return result;
    }

    static List<String> readPermissions(Object value) {
        try {
            List<String> keys = JSON.readValue(String.valueOf(value), new TypeReference<List<String>>() {
            });
            return keys.stream().filter(PERMISSION_KEYS::contains).toList();
        } catch (Exception e) {
            return List.of();
        }
    }

    private static String normalize(String account) {
        return StringUtils.defaultString(account).trim().toLowerCase(Locale.ROOT);
    }
}
