/*
 * Copyright 2026 Ant Group Co., Ltd.
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

import org.secretflow.secretpad.common.dto.UserContextDTO;
import org.secretflow.secretpad.common.errorcode.SystemErrorCode;
import org.secretflow.secretpad.common.exception.SecretpadException;
import org.secretflow.secretpad.common.util.UserContext;
import org.secretflow.secretpad.web.service.model.ModelApiService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Permission, scope and anti-escalation rules of system management, run against the real
 * V43 migration on a temporary SQLite database.
 */
class SystemManagementPermissionTest {

    private static final Path DB_FILE = Path.of(
            System.getProperty("java.io.tmpdir"), "system-management-permission-test.sqlite");
    private static final Path V43 = Path.of("..", "config", "schema", "p2p", "V43__system_tenant_role.sql");

    private JdbcTemplate jdbc;
    private SystemAccessService access;
    private SystemRoleService roleService;
    private SystemUserManagementService userService;
    private SystemTenantService tenantService;

    @BeforeEach
    void setUp() throws Exception {
        Files.deleteIfExists(DB_FILE);
        DriverManagerDataSource dataSource = new DriverManagerDataSource("jdbc:sqlite:" + DB_FILE, "", "");
        jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("""
                create table user_accounts (
                  name varchar(128) primary key,
                  password_hash varchar(128) not null default '',
                  owner_type varchar(16) not null default 'EDGE',
                  owner_id varchar(64) not null,
                  inst_id varchar(64) not null default '',
                  is_deleted integer not null default 0,
                  display_name varchar(64) not null default '',
                  account_status varchar(16) not null default 'ENABLED',
                  last_login_at datetime default null,
                  failed_attempts integer default null,
                  locked_invalid_time datetime default null,
                  passwd_reset_failed_attempts integer default null,
                  gmt_passwd_reset_release datetime default null,
                  gmt_create datetime not null default current_timestamp,
                  gmt_modified datetime not null default current_timestamp
                )""");
        jdbc.execute("create table user_tokens (name varchar(128), token varchar(128))");
        jdbc.execute("create table sys_user_permission_rel (user_type varchar(16), user_key varchar(128), "
                + "target_type varchar(16), target_code varchar(64))");
        jdbc.execute("create table sys_user_node_rel (user_id varchar(128), node_id varchar(64))");
        jdbc.execute("create table ds_sandbox (id varchar(64) primary key, created_by varchar(128), "
                + "deleted integer not null default 0)");
        jdbc.execute("create table ds_resource_allocation (id varchar(64) primary key, sandbox_id varchar(64), "
                + "resource_type varchar(32), amount real, state varchar(16))");
        try (Connection connection = dataSource.getConnection()) {
            // 迁移脚本含中文，须按 UTF-8 读取，与 Flyway 一致
            ScriptUtils.executeSqlScript(connection,
                    new EncodedResource(new FileSystemResource(V43.toFile()), StandardCharsets.UTF_8));
        }
        jdbc.update("insert into user_accounts(name, owner_id, display_name) values ('devadmin', 'node-a', 'admin')");

        access = new SystemAccessService(jdbc);
        ReflectionTestUtils.setField(access, "adminName", "devadmin");
        tenantService = new SystemTenantService(jdbc, access);
        roleService = new SystemRoleService(jdbc, tenantService, access);
        userService = new SystemUserManagementService(jdbc, mock(ModelApiService.class), access, roleService);
        ReflectionTestUtils.setField(userService, "adminName", "devadmin");
        loginAs("devadmin");
    }

    @AfterEach
    void tearDown() throws Exception {
        UserContext.remove();
        Files.deleteIfExists(DB_FILE);
    }

    @Test
    void nodeAdminCreatesUserTogetherWithTenantAndRoles() {
        userService.create(user("pm1", "tenant-research", "role-project-manager"));

        assertEquals("tenant-research", access.tenantOf("pm1"));
        assertEquals(List.of("role-project-manager"), access.roleIdsOf("pm1"));
    }

    @Test
    void ordinaryUserMustBelongToTenant() {
        SecretpadException rejected = assertThrows(SecretpadException.class,
                () -> userService.create(user("dev1", "", "role-developer")));
        // 系统管理的校验提示原样展示，不带“入参校验失败”前缀
        assertEquals(SystemErrorCode.BUSINESS_RULE_ERROR, rejected.getErrorCode());
        assertEquals(0, count("select count(1) from user_accounts where name = 'dev1'"));
    }

    @Test
    void sandboxAdministratorCannotBelongToTenant() {
        assertThrows(SecretpadException.class,
                () -> userService.create(user("sa1", "tenant-research", "role-admin")));

        userService.create(user("sa1", "", "role-admin"));
        assertTrue(access.isSandboxAdmin("sa1"));
        assertEquals("", access.tenantOf("sa1"));
    }

    @Test
    void projectManagerOnlyManagesOwnTenantWithinOwnPermissions() {
        userService.create(user("pm1", "tenant-research", "role-project-manager"));
        userService.create(user("other", "tenant-platform", "role-developer"));
        loginAs("pm1");

        userService.create(user("dev1", "tenant-research", "role-developer"));
        assertThrows(SecretpadException.class,
                () -> userService.create(user("dev2", "tenant-platform", "role-developer")));
        assertThrows(SecretpadException.class,
                () -> userService.create(user("aud1", "tenant-research", "role-auditor")));
        assertThrows(SecretpadException.class,
                () -> userService.create(user("sa2", "", "role-admin")));

        List<String> visible = userService.list().stream().map(item -> (String) item.get("account")).toList();
        assertTrue(visible.containsAll(List.of("pm1", "dev1")));
        assertFalse(visible.contains("other"));
        assertFalse(visible.contains("devadmin"));
        assertThrows(SecretpadException.class,
                () -> userService.changeStatus(Map.of("account", "other", "status", "DISABLED")));
    }

    @Test
    void nobodyCanModifyThemselvesOrTheNodeAdministrator() {
        userService.create(user("sa1", "", "role-admin"));
        loginAs("sa1");

        assertThrows(SecretpadException.class,
                () -> userService.changeStatus(Map.of("account", "sa1", "status", "DISABLED")));
        assertThrows(SecretpadException.class,
                () -> roleService.saveAssignment(Map.of("account", "sa1", "roleIds", List.of("role-developer"),
                        "tenantId", "tenant-research")));
        assertThrows(SecretpadException.class,
                () -> userService.delete(Map.of("account", "devadmin")));
    }

    @Test
    void rolesCannotExceedOperatorPermissions() {
        userService.create(user("pm1", "tenant-research", "role-project-manager"));
        loginAs("pm1");

        assertThrows(SecretpadException.class, () -> roleService.save(
                Map.of("name", "租户管理员", "permissions", List.of("system:user"))));

        loginAs("devadmin");
        Map<String, Object> role = roleService.save(
                Map.of("name", "租户管理员", "permissions", List.of("workbench:view", "system:user")));
        assertEquals(List.of("workbench:view", "system:user"), role.get("permissions"));
        assertThrows(SecretpadException.class, () -> roleService.save(
                Map.of("id", "role-admin", "name", "沙箱管理员", "permissions", List.of("log:view"))));
    }

    @Test
    void usersWithoutUserManagePermissionAreRejected() {
        userService.create(user("dev1", "tenant-research", "role-developer"));
        loginAs("dev1");

        assertThrows(SecretpadException.class, () -> userService.list());
        assertThrows(SecretpadException.class, () -> roleService.listAssignments());
    }

    @Test
    void contextReflectsRolesTenantAndNodeAdministrator() {
        userService.create(user("pm1", "tenant-research", "role-project-manager"));

        Map<String, Object> admin = access.context();
        assertEquals(true, admin.get("admin"));
        assertEquals(true, admin.get("sandboxAdmin"));
        assertTrue(((Collection<?>) admin.get("permissions")).contains("system:tenant"));

        loginAs("pm1");
        Map<String, Object> pm = access.context();
        assertEquals(false, pm.get("sandboxAdmin"));
        assertEquals("tenant-research", pm.get("tenantId"));
        assertTrue(((Collection<?>) pm.get("permissions")).contains("system:user"));
        assertFalse(((Collection<?>) pm.get("permissions")).contains("system:role"));
    }

    @Test
    void resourceApplicationRequiresTenantAndRespectsQuota() {
        // 联合建模租户预置配额：CPU 16 核
        userService.create(user("dev1", "tenant-research", "role-developer"));
        jdbc.update("insert into user_accounts(name, owner_id) values ('loner', 'node-a')");
        jdbc.update("insert into ds_sandbox(id, created_by) values ('sbx-1', 'dev1')");
        jdbc.update("insert into ds_resource_allocation values ('a1', 'sbx-1', 'CPU', 12, 'BOUND')");

        assertDoesNotThrow(() -> tenantService.assertResourceApplication(
                "dev1", Map.of("cpuCores", 4), ""));
        SecretpadException exceeded = assertThrows(SecretpadException.class,
                () -> tenantService.assertResourceApplication("dev1", Map.of("cpuCores", 8), ""));
        // 业务拒绝的提示原样展示，不带“系统未知错误”前缀
        assertEquals(SystemErrorCode.BUSINESS_RULE_ERROR, exceeded.getErrorCode());
        // 规格变更排除目标沙箱自身的占用
        assertDoesNotThrow(() -> tenantService.assertResourceApplication(
                "dev1", Map.of("cpuCores", 16), "sbx-1"));
        assertThrows(SecretpadException.class, () -> tenantService.assertResourceApplication(
                "loner", Map.of("cpuCores", 1), ""));
        // 节点管理员与沙箱管理员豁免
        assertDoesNotThrow(() -> tenantService.assertResourceApplication(
                "devadmin", Map.of("cpuCores", 999), ""));
        userService.create(user("sa1", "", "role-admin"));
        assertDoesNotThrow(() -> tenantService.assertResourceApplication(
                "sa1", Map.of("cpuCores", 999), ""));
    }

    @Test
    void tenantTransferCarriesSandboxUsageAndRespectsQuota() {
        // 联合建模租户 CPU 配额 16 核，平台运营租户 32 核
        userService.create(user("dev1", "tenant-platform", "role-developer"));
        userService.create(user("dev2", "tenant-research", "role-developer"));
        jdbc.update("insert into ds_sandbox(id, created_by) values ('sbx-1', 'dev1'), ('sbx-2', 'dev2')");
        jdbc.update("insert into ds_resource_allocation values ('a1', 'sbx-1', 'CPU', 12, 'BOUND'), "
                + "('a2', 'sbx-2', 'CPU', 8, 'BOUND')");

        SecretpadException rejected = assertThrows(SecretpadException.class, () -> roleService.saveAssignment(
                Map.of("account", "dev1", "tenantId", "tenant-research", "roleIds", List.of("role-developer"))));
        assertEquals(SystemErrorCode.BUSINESS_RULE_ERROR, rejected.getErrorCode());
        assertEquals("tenant-platform", access.tenantOf("dev1"));
        // 保持原租户、只调整角色时不校验转入
        assertDoesNotThrow(() -> roleService.saveAssignment(Map.of("account", "dev2",
                "tenantId", "tenant-research", "roleIds", List.of("role-project-manager"))));

        jdbc.update("update ds_resource_allocation set state = 'RELEASED' where id = 'a1'");
        jdbc.update("insert into ds_resource_allocation values ('a3', 'sbx-1', 'CPU', 8, 'RESERVED')");
        assertDoesNotThrow(() -> roleService.saveAssignment(
                Map.of("account", "dev1", "tenantId", "tenant-research", "roleIds", List.of("role-developer"))));
        assertEquals("tenant-research", access.tenantOf("dev1"));
    }

    @Test
    void recreatedAccountInheritsSandboxUsageForQuotaCheck() {
        // 已删除账号遗留的沙箱：按创建人名称关联到同名重建的账号
        jdbc.update("insert into ds_sandbox(id, created_by) values ('sbx-1', 'dev1')");
        jdbc.update("insert into ds_resource_allocation values ('a1', 'sbx-1', 'CPU', 20, 'BOUND')");

        assertThrows(SecretpadException.class,
                () -> userService.create(user("dev1", "tenant-research", "role-developer")));
        assertEquals(0, count("select count(1) from user_accounts where name = 'dev1' and is_deleted = 0"));
        assertDoesNotThrow(() -> userService.create(user("dev1", "tenant-platform", "role-developer")));
    }

    @Test
    void frozenTenantBlocksLoginAndRevokesSessions() {
        userService.create(user("dev1", "tenant-research", "role-developer"));
        userService.create(user("sa1", "", "role-admin"));
        jdbc.update("insert into user_tokens(name, token) values ('dev1', 't1'), ('sa1', 't2')");

        tenantService.changeStatus(Map.of("id", "tenant-research", "status", "FROZEN"));

        assertEquals(0, count("select count(1) from user_tokens where name = 'dev1'"));
        assertEquals(1, count("select count(1) from user_tokens where name = 'sa1'"));
        assertThrows(SecretpadException.class, () -> tenantService.check("dev1"));
        assertThrows(SecretpadException.class, () -> tenantService.assertResourceApplication(
                "dev1", Map.of("cpuCores", 1), ""));
        assertDoesNotThrow(() -> tenantService.check("sa1"));
        assertDoesNotThrow(() -> tenantService.check("devadmin"));

        tenantService.changeStatus(Map.of("id", "tenant-research", "status", "ACTIVE"));
        assertDoesNotThrow(() -> tenantService.check("dev1"));
    }

    @Test
    void internalErrorsDoNotBlockApplicationsOrLogin() {
        userService.create(user("dev1", "tenant-research", "role-developer"));
        jdbc.execute("drop table ds_resource_allocation");

        assertDoesNotThrow(() -> tenantService.assertResourceApplication(
                "dev1", Map.of("cpuCores", 1), ""));
        jdbc.execute("drop table ds_tenant");
        assertDoesNotThrow(() -> tenantService.check("dev1"));
    }

    private void loginAs(String account) {
        UserContext.remove();
        UserContext.setBaseUser(UserContextDTO.builder().name(account).ownerId("node-a").build());
    }

    private int count(String sql) {
        Integer value = jdbc.queryForObject(sql, Integer.class);
        return value == null ? 0 : value;
    }

    private static Map<String, Object> user(String account, String tenantId, String roleId) {
        return Map.of("account", account, "displayName", account, "tenantId", tenantId,
                "roleIds", List.of(roleId));
    }
}
