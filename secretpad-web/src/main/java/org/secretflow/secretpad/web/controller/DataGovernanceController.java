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
import org.secretflow.secretpad.web.service.governance.DataGovernanceService;

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
 * Z-04 数据治理 API：抽样/脱敏策略与任务、结果数据集、血缘、源数据预览。
 *
 * <p>权限：策略/任务按创建人隔离；submit/preview/mount 走 {@link DataGovernanceService#checkSourcePermission}。
 * 错误码沿用全局异常体系（GOV_NO_PERMISSION / GOV_INPUT_TOO_LARGE / GOV_NOT_FOUND /
 * GOV_STATE_CONFLICT / GOV_PARAM_INVALID）。所有写操作在服务内审计 + webhook。</p>
 */
@Tag(name = "Data Governance", description = "数据治理：抽样/脱敏策略与任务、结果数据集、血缘、预览")
@RestController
@RequestMapping("/api/v1alpha1/data-governance")
public class DataGovernanceController {

    private final DataGovernanceService service;

    public DataGovernanceController(DataGovernanceService service) {
        this.service = service;
    }

    /* ------------------------------- 策略 ------------------------------- */

    @Operation(summary = "创建策略（同名幂等拒绝）")
    @PostMapping("/policies")
    public SecretPadResponse<Map<String, Object>> createPolicy(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.createPolicy(request));
    }

    @Operation(summary = "更新策略（仅创建人）")
    @PostMapping("/policies/update")
    public SecretPadResponse<Map<String, Object>> updatePolicy(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.updatePolicy(request));
    }

    @Operation(summary = "软删策略")
    @PostMapping("/policies/delete")
    public SecretPadResponse<Void> deletePolicy(@RequestBody Map<String, Object> request) {
        service.deletePolicy(String.valueOf(request.get("id")));
        return SecretPadResponse.success();
    }

    @Operation(summary = "策略列表")
    @GetMapping("/policies")
    public SecretPadResponse<List<Map<String, Object>>> listPolicies(
            @RequestParam(defaultValue = "") String type,
            @RequestParam(defaultValue = "") String keyword) {
        return SecretPadResponse.success(service.listPolicies(type, keyword));
    }

    @Operation(summary = "策略详情（含引用任务）")
    @GetMapping("/policies/detail")
    public SecretPadResponse<Map<String, Object>> policyDetail(@RequestParam String id) {
        return SecretPadResponse.success(service.policyDetail(id));
    }

    /* ------------------------------- 任务 ------------------------------- */

    @Operation(summary = "提交治理任务（BUILTIN 内置引擎 / CUSTOM 自定义代码执行）")
    @PostMapping("/tasks/submit")
    public SecretPadResponse<Map<String, Object>> submitTask(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.submitTask(request));
    }

    @Operation(summary = "任务列表")
    @GetMapping("/tasks")
    public SecretPadResponse<List<Map<String, Object>>> listTasks(
            @RequestParam(defaultValue = "") String status,
            @RequestParam(defaultValue = "") String execMode,
            @RequestParam(defaultValue = "") String keyword) {
        return SecretPadResponse.success(service.listTasks(status, execMode, keyword));
    }

    @Operation(summary = "任务详情（含血缘链）")
    @GetMapping("/tasks/detail")
    public SecretPadResponse<Map<String, Object>> taskDetail(@RequestParam String id) {
        return SecretPadResponse.success(service.taskDetail(id));
    }

    @Operation(summary = "取消任务（PENDING/RUNNING → CANCELLED，RUNNING 停 Kuscia Job）")
    @PostMapping("/tasks/cancel")
    public SecretPadResponse<Void> cancelTask(@RequestBody Map<String, Object> request) {
        service.cancelTask(String.valueOf(request.get("id")));
        return SecretPadResponse.success();
    }

    @Operation(summary = "失败任务重试")
    @PostMapping("/tasks/retry")
    public SecretPadResponse<Map<String, Object>> retryTask(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.retryTask(String.valueOf(request.get("id"))));
    }

    @Operation(summary = "结果数据集（status=SUCCEEDED 且 result_datatable_id 非空）")
    @GetMapping("/tasks/results")
    public SecretPadResponse<List<Map<String, Object>>> results(@RequestParam(defaultValue = "") String nodeId) {
        return SecretPadResponse.success(service.listResults(nodeId));
    }

    @Operation(summary = "结果数据集挂载项目（source=IMPORTED）")
    @PostMapping("/tasks/mount")
    public SecretPadResponse<Map<String, Object>> mountResult(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.mountResult(request));
    }

    @Operation(summary = "查看任务结果数据（仅脱敏后结果可返回行；表头携带数据源信息）")
    @GetMapping("/tasks/results/view")
    public SecretPadResponse<Map<String, Object>> viewResult(@RequestParam String taskId) {
        return SecretPadResponse.success(service.viewResult(taskId));
    }

    /* ------------------------------- 血缘 / 预览 ------------------------------- */

    @Operation(summary = "血缘查询（source 或 target 命中）")
    @GetMapping("/lineage")
    public SecretPadResponse<List<Map<String, Object>>> lineage(
            @RequestParam(defaultValue = "") String nodeId,
            @RequestParam(defaultValue = "") String datatableId) {
        return SecretPadResponse.success(service.lineage(nodeId, datatableId));
    }

    @Operation(summary = "源数据预览（强制权限校验，仅前 limit 行 + schema + 行数）")
    @GetMapping("/preview")
    public SecretPadResponse<Map<String, Object>> preview(
            @RequestParam String nodeId,
            @RequestParam String datatableId,
            @RequestParam(defaultValue = "20") int limit) {
        return SecretPadResponse.success(service.previewSource(
                Map.of("nodeId", nodeId, "datatableId", datatableId, "limit", limit)));
    }
}
