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
import org.secretflow.secretpad.web.service.SystemUserManagementService;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * System user management APIs.
 */
@RestController
@RequestMapping("/api/v1alpha1/system/users")
public class SystemUserManagementController {

    private final SystemUserManagementService userManagementService;

    public SystemUserManagementController(
            SystemUserManagementService userManagementService) {
        this.userManagementService = userManagementService;
    }

    @GetMapping("/list")
    public SecretPadResponse<List<Map<String, Object>>> list() {
        return SecretPadResponse.success(userManagementService.list());
    }

    @GetMapping("/authorization-options")
    public SecretPadResponse<List<Map<String, Object>>> authorizationOptions() {
        return SecretPadResponse.success(userManagementService.authorizationOptions());
    }

    @PostMapping("/create")
    public SecretPadResponse<Map<String, Object>> create(
            @RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(userManagementService.create(request));
    }

    @PostMapping("/update")
    public SecretPadResponse<Map<String, Object>> update(
            @RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(userManagementService.update(request));
    }

    @PostMapping("/changeStatus")
    public SecretPadResponse<Map<String, Object>> changeStatus(
            @RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(userManagementService.changeStatus(request));
    }

    @PostMapping("/resetPassword")
    public SecretPadResponse<String> resetPassword(
            @RequestBody Map<String, Object> request) {
        userManagementService.resetPassword(request);
        return SecretPadResponse.success("ok");
    }

    @PostMapping("/delete")
    public SecretPadResponse<String> delete(
            @RequestBody Map<String, Object> request) {
        userManagementService.delete(request);
        return SecretPadResponse.success("ok");
    }
}
