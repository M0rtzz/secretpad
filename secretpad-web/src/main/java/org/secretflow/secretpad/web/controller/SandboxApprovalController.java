/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.controller;

import org.secretflow.secretpad.service.model.common.SecretPadResponse;
import org.secretflow.secretpad.web.service.sandbox.SandboxApprovalService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Z-03 沙箱资源申请与审批 APIs：申请单 CRUD、两级审批动作、审批历史与门禁配置。
 *
 * <p>门禁只拦直接创建/续期/回收（DataSandboxController），申请单本身始终可提交、
 * 可审核——权限由 {@link SandboxApprovalService} 内按角色/状态机校验。</p>
 */
@Tag(name = "Data Sandbox Approval", description = "沙箱资源申请与审批（创建/续期/规格变更/回收）")
@RestController
@RequestMapping("/api/v1alpha1/data-sandbox/approvals")
public class SandboxApprovalController {

    private final SandboxApprovalService service;

    public SandboxApprovalController(SandboxApprovalService service) {
        this.service = service;
    }

    @Operation(summary = "申请单列表（可按状态/类型/关键字过滤）")
    @GetMapping("")
    public SecretPadResponse<List<Map<String, Object>>> approvals(
            @RequestParam(defaultValue = "") String status,
            @RequestParam(defaultValue = "") String type,
            @RequestParam(defaultValue = "") String keyword) {
        return SecretPadResponse.success(service.listApprovals(status, type, keyword));
    }

    @Operation(summary = "申请单详情（含审批历史）")
    @GetMapping("/detail")
    public SecretPadResponse<Map<String, Object>> approval(@RequestParam String id) {
        return SecretPadResponse.success(service.approval(id));
    }

    @Operation(summary = "提交沙箱申请单（数据删除申请须从数据目录发起）")
    @PostMapping("/submit")
    public SecretPadResponse<Map<String, Object>> submit(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.submit(request));
    }

    @Operation(summary = "审批动作（APPROVE/REJECT/RESUBMIT/RETRY/CANCEL）")
    @PostMapping("/action")
    public SecretPadResponse<Map<String, Object>> action(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.approvalAction(request));
    }

    @Operation(summary = "申请单审批历史")
    @GetMapping("/history")
    public SecretPadResponse<List<Map<String, Object>>> history(@RequestParam String id) {
        return SecretPadResponse.success(service.approvalHistory(id));
    }

    @Operation(summary = "门禁与重试配置（前端感知 required 状态）")
    @GetMapping("/config")
    public SecretPadResponse<Map<String, Object>> config() {
        return SecretPadResponse.success(service.approvalConfig());
    }
}
