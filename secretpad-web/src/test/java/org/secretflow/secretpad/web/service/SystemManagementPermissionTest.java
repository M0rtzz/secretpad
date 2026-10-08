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
        try (Connection connection = dataSource.getConnection()) {
            // 迁移脚本含中文，须按 UTF-8 读取，与 Flyway 一致
            ScriptUtils.executeSqlScript(connection,
                    new EncodedResource(new FileSystemResource(V43.toFile()), StandardCharsets.UTF_8));
        }
        jdbc.update("insert into user_accounts(name, owner_id, display_name) values ('devadmin', 'node-a', 'admin')");

        access = new SystemAccessService(jdbc);
        ReflectionTestUtils.setField(access, "adminName", "devadmin");
        roleService = new SystemRoleService(jdbc, new SystemTenantService(jdbc, access), access);
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
        assertThrows(SecretpadException.class,
                () -> userService.create(user("dev1", "", "role-developer")));
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
