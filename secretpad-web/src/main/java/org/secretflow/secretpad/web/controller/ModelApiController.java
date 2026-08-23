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
import org.secretflow.secretpad.web.service.model.ModelApiApprovalService;
import org.secretflow.secretpad.web.service.model.ModelApiService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Z-06 受控模型 API：发布（一次性 app_id+secret）、授权用户/IP 白名单/有效时间控制、同步推理调用。
 *
 * <p>{@code /invoke} 支持两路鉴权：X-APP-ID/X-APP-SECRET 凭证头（LoginInterceptor 独立于 auth.enabled 强制校验）
 * 或 User-Token（授权用户名单约束，appId 放请求体）。其余管理端点走 User-Token。</p>
 */
@Tag(name = "Model API", description = "受控模型 API：发布/凭证/授权/白名单/有效时间/同步推理")
@RestController
@RequestMapping("/api/v1alpha1/model-api")
public class ModelApiController {

    private final ModelApiService service;
    private final ModelApiApprovalService approvalService;

    public ModelApiController(ModelApiService service, ModelApiApprovalService approvalService) {
        this.service = service;
        this.approvalService = approvalService;
    }

    @Operation(summary = "发布模型为 API（模型需 APPROVED/PUBLISHED；一次性 app_id+secret 明文仅本次返回）")
    @PostMapping("/create")
    public SecretPadResponse<Map<String, Object>> create(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.create(request));
    }

    @Operation(summary = "统一发布受控 API（MODEL 使用跨机构数据时先进入供数方审批；通过后才启用）")
    @PostMapping("/publish")
    public SecretPadResponse<Map<String, Object>> publish(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.publish(request));
    }

    @Operation(summary = "制品→API 一键发布（自动注册 APPROVED 模型 + 创建 API；一次性 app_id+secret 明文仅本次返回）")
    @PostMapping("/create-from-artifact")
    public SecretPadResponse<Map<String, Object>> createFromArtifact(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.createFromArtifact(request));
    }

    @Operation(summary = "API 列表（keyword 过滤，不回显 secret）")
    @GetMapping("/list")
    public SecretPadResponse<List<Map<String, Object>>> list(
            @RequestParam(defaultValue = "") String keyword,
            @RequestParam String sandboxId) {
        return SecretPadResponse.success(service.list(keyword, sandboxId));
    }

    @Operation(summary = "API 详情（+ 模型摘要）")
    @GetMapping("/detail")
    public SecretPadResponse<Map<String, Object>> detail(@RequestParam String id) {
        return SecretPadResponse.success(service.detail(id));
    }

    @Operation(summary = "更新 API 授权/白名单/有效时间/描述")
    @PostMapping("/update")
    public SecretPadResponse<Map<String, Object>> update(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.update(String.valueOf(request.get("id")), request));
    }

    @Operation(summary = "重发调用密钥（新 secret 一次性返回，旧密钥立即失效）")
    @PostMapping("/regenerate-secret")
    public SecretPadResponse<Map<String, Object>> regenerateSecret(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.regenerateSecret(String.valueOf(request.get("id"))));
    }

    @Operation(summary = "启用 API")
    @PostMapping("/enable")
    public SecretPadResponse<Map<String, Object>> enable(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.enable(String.valueOf(request.get("id"))));
    }

    @Operation(summary = "停用 API")
    @PostMapping("/disable")
    public SecretPadResponse<Map<String, Object>> disable(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.disable(String.valueOf(request.get("id"))));
    }

    @Operation(summary = "删除 API（软删）")
    @PostMapping("/delete")
    public SecretPadResponse<Void> delete(@RequestBody Map<String, Object> request) {
        service.delete(String.valueOf(request.get("id")));
        return SecretPadResponse.success();
    }

    @Operation(summary = "受控推理调用（X-APP-ID/X-APP-SECRET 或 User-Token+body.appId；守卫后同步执行）")
    @PostMapping("/invoke")
    public SecretPadResponse<Map<String, Object>> invoke(
            @RequestHeader(value = "X-APP-ID", required = false) String appIdHeader,
            @RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.invoke(appIdHeader, request));
    }

    /* ============================== 模型 API 供数方审批 ============================== */

    @Operation(summary = "我的模型 API 审批申请单（approval_type=MODEL_API）")
    @GetMapping("/approvals/mine")
    public SecretPadResponse<List<Map<String, Object>>> approvalsMine(
            @RequestParam(defaultValue = "") String status,
            @RequestParam(defaultValue = "") String keyword) {
        return SecretPadResponse.success(approvalService.listMine(status, keyword));
    }

    @Operation(summary = "待我审批的模型 API 申请单（当前节点为供数方投票人）")
    @GetMapping("/approvals/pending")
    public SecretPadResponse<List<Map<String, Object>>> approvalsPending(
            @RequestParam(defaultValue = "") String keyword) {
        return SecretPadResponse.success(approvalService.listPending(keyword));
    }

    @Operation(summary = "模型 API 审批申请单详情（模型/数据/拓扑/凭证，审批方可在线调试）")
    @GetMapping("/approvals/detail")
    public SecretPadResponse<Map<String, Object>> approvalDetail(@RequestParam String id) {
        return SecretPadResponse.success(approvalService.detail(id));
    }

    @Operation(summary = "模型 API 审批动作：APPROVE/REJECT")
    @PostMapping("/approvals/action")
    public SecretPadResponse<Map<String, Object>> approvalAction(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(approvalService.action(
                String.valueOf(request.get("id")),
                String.valueOf(request.get("action")),
                String.valueOf(request.get("comment"))));
    }

    @Operation(summary = "撤回模型 API 审批申请（仅申请方，PENDING 状态）")
    @PostMapping("/approvals/cancel")
    public SecretPadResponse<Map<String, Object>> approvalCancel(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(approvalService.cancel(String.valueOf(request.get("id"))));
    }

    @Operation(summary = "模型 API 审批在线调试：以临时 API 凭证调用模型，返回推理结果")
    @PostMapping("/approvals/test")
    public SecretPadResponse<Map<String, Object>> approvalTest(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(approvalService.test(
                String.valueOf(request.get("id")), request));
    }
}
