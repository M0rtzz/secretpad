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

package org.secretflow.secretpad.web.controller;

import org.secretflow.secretpad.service.model.common.SecretPadResponse;
import org.secretflow.secretpad.web.service.SystemAccessService;
import org.secretflow.secretpad.web.service.SystemRoleService;
import org.secretflow.secretpad.web.service.SystemTenantService;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Tenant, role and assignment management APIs (node-local, permission based).
 */
@RestController
@RequestMapping("/api/v1alpha1/system")
public class SystemManagementController {

    private final SystemTenantService tenantService;
    private final SystemRoleService roleService;
    private final SystemAccessService accessService;

    public SystemManagementController(SystemTenantService tenantService,
                                      SystemRoleService roleService,
                                      SystemAccessService accessService) {
        this.tenantService = tenantService;
        this.roleService = roleService;
        this.accessService = accessService;
    }

    @GetMapping("/me/context")
    public SecretPadResponse<Map<String, Object>> currentContext() {
        return SecretPadResponse.success(accessService.context());
    }

    @GetMapping("/tenants/list")
    public SecretPadResponse<List<Map<String, Object>>> listTenants() {
        return SecretPadResponse.success(tenantService.list());
    }

    @GetMapping("/tenants/overview")
    public SecretPadResponse<Map<String, Object>> tenantOverview() {
        return SecretPadResponse.success(tenantService.overview());
    }

    @PostMapping("/tenants/save")
    public SecretPadResponse<Map<String, Object>> saveTenant(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(tenantService.save(request));
    }

    @PostMapping("/tenants/changeStatus")
    public SecretPadResponse<Map<String, Object>> changeTenantStatus(
            @RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(tenantService.changeStatus(request));
    }

    @PostMapping("/tenants/delete")
    public SecretPadResponse<String> deleteTenant(@RequestBody Map<String, Object> request) {
        tenantService.delete(request);
        return SecretPadResponse.success("ok");
    }

    @GetMapping("/roles/list")
    public SecretPadResponse<List<Map<String, Object>>> listRoles() {
        return SecretPadResponse.success(roleService.list());
    }

    @PostMapping("/roles/save")
    public SecretPadResponse<Map<String, Object>> saveRole(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(roleService.save(request));
    }

    @PostMapping("/roles/delete")
    public SecretPadResponse<String> deleteRole(@RequestBody Map<String, Object> request) {
        roleService.delete(request);
        return SecretPadResponse.success("ok");
    }

    @GetMapping("/assignments/list")
    public SecretPadResponse<List<Map<String, Object>>> listAssignments() {
        return SecretPadResponse.success(roleService.listAssignments());
    }

    @PostMapping("/assignments/save")
    public SecretPadResponse<Map<String, Object>> saveAssignment(
            @RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(roleService.saveAssignment(request));
    }
}
