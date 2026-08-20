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
import org.secretflow.secretpad.web.service.model.ModelApprovalService;

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
 * Z-06 模型注册与审批 API：模型绑定 Z-05 制品+版本+项目，两级审批（MODEL_REVIEW→RESOURCE_REVIEW→APPROVED→PUBLISHED）
 * 带强制测试门禁（MODEL_TEST_REQUIRED）。错误码 MODEL_*，权限：创建人管理模型、提交人/创建人执行审批动作。
 */
@Tag(name = "Model", description = "模型中心：模型注册/审批（两级、强制测试门禁）")
@RestController
@RequestMapping("/api/v1alpha1/models")
public class ModelController {

    private final ModelApprovalService service;

    public ModelController(ModelApprovalService service) {
        this.service = service;
    }

    @Operation(summary = "注册模型（JAR/PYTHON 制品+版本+项目；SQL 拒绝；同项目同制品重注册拒绝；版本自增）")
    @PostMapping("/register")
    public SecretPadResponse<Map<String, Object>> register(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.registerModel(request));
    }

    @Operation(summary = "模型列表（status/keyword 过滤）")
    @GetMapping("")
    public SecretPadResponse<List<Map<String, Object>>> list(
            @RequestParam(defaultValue = "") String status,
            @RequestParam(defaultValue = "") String keyword,
            @RequestParam(defaultValue = "") String sandboxId) {
        return SecretPadResponse.success(service.listModels(status, keyword, sandboxId));
    }

    @Operation(summary = "模型详情（+当前审批 + 测试 + API）")
    @GetMapping("/detail")
    public SecretPadResponse<Map<String, Object>> detail(@RequestParam String id) {
        return SecretPadResponse.success(service.modelDetail(id));
    }

    @Operation(summary = "更新模型（仅创建人；DRAFT/REJECTED 可改）")
    @PostMapping("/update")
    public SecretPadResponse<Map<String, Object>> update(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.updateModel(
                String.valueOf(request.get("id")),
                String.valueOf(request.get("name")),
                String.valueOf(request.get("description"))));
    }

    @Operation(summary = "删除模型（仅创建人；DRAFT/REJECTED/OFFLINE 可删）")
    @PostMapping("/delete")
    public SecretPadResponse<Void> delete(@RequestBody Map<String, Object> request) {
        service.deleteModel(String.valueOf(request.get("id")));
        return SecretPadResponse.success();
    }

    /* ------------------------------- 审批 ------------------------------- */

    @Operation(summary = "提交审批（DRAFT/REJECTED → APPROVING，绑定制品+版本+项目）")
    @PostMapping("/approvals/submit")
    public SecretPadResponse<Map<String, Object>> submitApproval(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.submitApproval(
                String.valueOf(request.get("modelId")),
                String.valueOf(request.get("comment"))));
    }

    @Operation(summary = "审批列表（status/keyword 过滤）")
    @GetMapping("/approvals")
    public SecretPadResponse<List<Map<String, Object>>> listApprovals(
            @RequestParam(defaultValue = "") String status,
            @RequestParam(defaultValue = "") String keyword) {
        return SecretPadResponse.success(service.listApprovals(status, keyword));
    }

    @Operation(summary = "审批详情（+历史 + 模型 + 测试）")
    @GetMapping("/approvals/detail")
    public SecretPadResponse<Map<String, Object>> approvalDetail(@RequestParam String id) {
        return SecretPadResponse.success(service.approvalDetail(id));
    }

    @Operation(summary = "审批历史")
    @GetMapping("/approvals/history")
    public SecretPadResponse<List<Map<String, Object>>> approvalHistory(@RequestParam String id) {
        return SecretPadResponse.success(service.approvalHistory(id));
    }

    @Operation(summary = "审批动作 APPROVE/REJECT/RESUBMIT/PUBLISH（APPROVE→APPROVED 前强制测试门禁）")
    @PostMapping("/approvals/action")
    public SecretPadResponse<Map<String, Object>> approvalAction(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.approvalAction(
                String.valueOf(request.get("id")),
                String.valueOf(request.get("action")),
                String.valueOf(request.get("comment"))));
    }
}
