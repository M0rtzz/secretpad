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
import org.secretflow.secretpad.common.util.UserContext;
import org.secretflow.secretpad.web.service.model.ModelApiService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class SystemUserManagementServiceTest {

    private static final Path DB_FILE = Path.of(
            System.getProperty("java.io.tmpdir"), "system-user-management-test.sqlite");

    private SystemUserManagementService service;

    @BeforeEach
    void setUp() throws Exception {
        Files.deleteIfExists(DB_FILE);
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                "jdbc:sqlite:" + DB_FILE, "", "");
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("""
                create table user_accounts (
                  name varchar(128) primary key,
                  owner_id varchar(64) not null,
                  is_deleted integer not null default 0,
                  display_name varchar(64) not null default '',
                  account_status varchar(16) not null default 'ENABLED',
                  last_login_at datetime default null,
                  gmt_create datetime not null default current_timestamp
                )
                """);
        jdbc.update("insert into user_accounts(name,owner_id,display_name) values(?,?,?)",
                "devadmin", "node-a", "Developer administrator");
        jdbc.update("insert into user_accounts(name,owner_id,display_name) values(?,?,?)",
                "alice", "node-a", "Alice");
        jdbc.update("insert into user_accounts(name,owner_id,display_name) values(?,?,?)",
                "bob", "node-b", "Bob");

        service = new SystemUserManagementService(jdbc, mock(ModelApiService.class));
        ReflectionTestUtils.setField(service, "adminName", "devadmin");
        UserContext.setBaseUser(UserContextDTO.builder()
                .name("devadmin")
                .ownerId("node-a")
                .build());
    }

    @AfterEach
    void tearDown() throws Exception {
        UserContext.remove();
        Files.deleteIfExists(DB_FILE);
    }

    @Test
    void listIncludesCurrentOwnerAdministratorAsProtectedSystemAccount() {
        List<Map<String, Object>> users = service.list();

        assertEquals(2, users.size());
        Map<String, Object> administrator = user(users, "devadmin");
        Map<String, Object> member = user(users, "alice");
        assertTrue((Boolean) administrator.get("systemAccount"));
        assertFalse((Boolean) member.get("systemAccount"));
    }

    private Map<String, Object> user(List<Map<String, Object>> users, String account) {
        return users.stream()
                .filter(user -> account.equals(user.get("account")))
                .findFirst()
                .orElseThrow();
    }
}
