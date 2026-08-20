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
import org.secretflow.secretpad.web.service.model.ModelTestService;

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
 * Z-06 模型测试 API：审批人配置测试参数、选择测试数据并执行模型；保存测试日志/输入/输出摘要/评估指标。
 * 测试任务复用 Z-05 一次性 Kuscia Job（channel='model'，调度器轮询收官），读取时惰性收官。
 */
@Tag(name = "Model Test", description = "模型测试：执行/列表/详情/日志/取消/重试")
@RestController
@RequestMapping("/api/v1alpha1/models/tests")
public class ModelTestController {

    private final ModelTestService service;

    public ModelTestController(ModelTestService service) {
        this.service = service;
    }

    @Operation(summary = "执行模型测试（nodeId+datatableId+labelColumn+predictionColumn+metricType+params；门禁测试）")
    @PostMapping("/execute")
    public SecretPadResponse<Map<String, Object>> execute(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.executeTest(request));
    }

    @Operation(summary = "测试列表（modelId/status 过滤）")
    @GetMapping("")
    public SecretPadResponse<List<Map<String, Object>>> list(
            @RequestParam(defaultValue = "") String modelId,
            @RequestParam(defaultValue = "") String status) {
        return SecretPadResponse.success(service.listTests(modelId, status));
    }

    @Operation(summary = "测试详情（读取时惰性收官，+ 指标/摘要/任务）")
    @GetMapping("/detail")
    public SecretPadResponse<Map<String, Object>> detail(@RequestParam String id) {
        return SecretPadResponse.success(service.testDetail(id));
    }

    @Operation(summary = "测试调试日志（attempt 默认最新）")
    @GetMapping("/log")
    public SecretPadResponse<Map<String, Object>> log(@RequestParam String id, @RequestParam(required = false) Integer attempt) {
        return SecretPadResponse.success(service.testLog(id, attempt));
    }

    @Operation(summary = "取消测试（RUNNING → CANCELLED，停 Kuscia Job）")
    @PostMapping("/cancel")
    public SecretPadResponse<Map<String, Object>> cancel(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.cancelTest(String.valueOf(request.get("id"))));
    }

    @Operation(summary = "重试失败测试（FAILED 且 retry_count<max，attempt=新 retry_count）")
    @PostMapping("/retry")
    public SecretPadResponse<Map<String, Object>> retry(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.retryTest(String.valueOf(request.get("id"))));
    }
}
