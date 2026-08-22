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
import org.secretflow.secretpad.web.service.dev.DataDevService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;

/**
 * Z-05 计算任务开发能力 API：制品（JAR/SQL/PYTHON/FUNCTION）与版本、依赖白名单、任务提交/操作/调试日志。
 *
 * <p>权限：制品/任务/依赖按创建人隔离；submit/preview/mount 走
 * {@link DataDevService#checkSourcePermission}；viewResult/runLog/mount 限创建人。
 * 错误码沿用全局异常体系（DEV_NO_PERMISSION / DEV_INPUT_TOO_LARGE / DEV_NOT_FOUND /
 * DEV_STATE_CONFLICT / DEV_PARAM_INVALID / DEV_DEPENDENCY_REJECTED）。所有写操作在服务内审计 + webhook。</p>
 */
@Tag(name = "Data Dev", description = "数据开发：制品与版本管理、JAR/SQL/Python/函数(UDF) 计算任务、依赖白名单、调试日志")
@RestController
@RequestMapping("/api/v1alpha1/data-dev")
public class DataDevController {

    private final DataDevService service;

    public DataDevController(DataDevService service) {
        this.service = service;
    }

    /* ------------------------------- 制品 ------------------------------- */

    @Operation(summary = "创建制品（type=JAR/SQL/PYTHON/FUNCTION，同名幂等拒绝）")
    @PostMapping("/artifacts")
    public SecretPadResponse<Map<String, Object>> createArtifact(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.createArtifact(request));
    }

    @Operation(summary = "更新制品（仅创建人）")
    @PostMapping("/artifacts/update")
    public SecretPadResponse<Map<String, Object>> updateArtifact(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.updateArtifact(request));
    }

    @Operation(summary = "软删制品（制品+版本）")
    @PostMapping("/artifacts/delete")
    public SecretPadResponse<Void> deleteArtifact(@RequestBody Map<String, Object> request) {
        service.deleteArtifact(String.valueOf(request.get("id")));
        return SecretPadResponse.success();
    }

    @Operation(summary = "制品列表（type/keyword 过滤）")
    @GetMapping("/artifacts")
    public SecretPadResponse<List<Map<String, Object>>> listArtifacts(
            @RequestParam(defaultValue = "") String type,
            @RequestParam(defaultValue = "") String keyword,
            @RequestParam(defaultValue = "") String sandboxId) {
        return SecretPadResponse.success(service.listArtifacts(type, keyword, sandboxId));
    }

    @Operation(summary = "制品详情（含版本列表）")
    @GetMapping("/artifacts/detail")
    public SecretPadResponse<Map<String, Object>> artifactDetail(@RequestParam String id) {
        return SecretPadResponse.success(service.artifactDetail(id));
    }

    /* ------------------------------- 版本 ------------------------------- */

    @Operation(summary = "新增 SQL/PYTHON/函数 版本（版本自增，不可变）")
    @PostMapping("/artifacts/versions")
    public SecretPadResponse<Map<String, Object>> createVersion(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.createVersion(request));
    }

    @Operation(summary = "JAR 多部分上传新版本（DevJarValidator 校验 + sha256 + 落盘；version 可选，手填需查重）")
    @PostMapping(value = "/artifacts/versions/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public SecretPadResponse<Map<String, Object>> uploadJarVersion(
            @RequestParam("artifactId") String artifactId,
            @RequestParam("file") MultipartFile file,
            @RequestParam(defaultValue = "[]") String paramsSchema,
            @RequestParam(defaultValue = "{}") String defaultParams,
            @RequestParam(defaultValue = "") String description,
            @RequestParam(required = false) Integer version) throws java.io.IOException {
        return SecretPadResponse.success(service.uploadJarVersion(artifactId, file.getBytes(),
                paramsSchema, defaultParams, description, version));
    }

    @Operation(summary = "软删版本（latest_version 回退）")
    @PostMapping("/artifacts/versions/delete")
    public SecretPadResponse<Void> deleteVersion(@RequestBody Map<String, Object> request) {
        service.deleteVersion(String.valueOf(request.get("id")));
        return SecretPadResponse.success();
    }

    @Operation(summary = "版本列表")
    @GetMapping("/artifacts/versions")
    public SecretPadResponse<List<Map<String, Object>>> listVersions(@RequestParam String artifactId) {
        return SecretPadResponse.success(service.listVersions(artifactId));
    }

    @Operation(summary = "版本详情")
    @GetMapping("/artifacts/versions/detail")
    public SecretPadResponse<Map<String, Object>> versionDetail(@RequestParam String versionId) {
        return SecretPadResponse.success(service.versionDetail(versionId));
    }

    @Operation(summary = "下载 JAR 文件（原始字节）")
    @GetMapping("/artifacts/versions/download")
    public ResponseEntity<byte[]> downloadJar(@RequestParam String versionId) {
        byte[] bytes = service.downloadJar(versionId);
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_OCTET_STREAM)
                .header("Content-Disposition", "attachment; filename=\"" + versionId + ".jar\"")
                .body(bytes);
    }

    /* ------------------------------- 依赖白名单 ------------------------------- */

    @Operation(summary = "依赖白名单列表（enabled=0/1 过滤）")
    @GetMapping("/dependencies")
    public SecretPadResponse<List<Map<String, Object>>> listDependencies(
            @RequestParam(defaultValue = "") String enabled,
            @RequestParam(defaultValue = "") String keyword) {
        return SecretPadResponse.success(service.listDependencies(enabled, keyword));
    }

    @Operation(summary = "新增依赖白名单条目")
    @PostMapping("/dependencies")
    public SecretPadResponse<Map<String, Object>> createDependency(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.createDependency(request));
    }

    @Operation(summary = "更新依赖白名单条目（仅创建人）")
    @PostMapping("/dependencies/update")
    public SecretPadResponse<Map<String, Object>> updateDependency(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.updateDependency(request));
    }

    @Operation(summary = "删除依赖白名单条目")
    @PostMapping("/dependencies/delete")
    public SecretPadResponse<Void> deleteDependency(@RequestBody Map<String, Object> request) {
        service.deleteDependency(String.valueOf(request.get("id")));
        return SecretPadResponse.success();
    }

    /* ------------------------------- 任务 ------------------------------- */

    @Operation(summary = "提交计算任务（runMode=DEV/PROD，execType=JAR/SQL/PYTHON）")
    @PostMapping("/tasks/submit")
    public SecretPadResponse<Map<String, Object>> submitTask(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.submitTask(request));
    }

    @Operation(summary = "沙箱表源任务提交（sandboxId+sourceTable；SQL 文件库只读 / JAR/PYTHON CSV+JDBC 契约）")
    @PostMapping("/tasks/submit-sandbox")
    public SecretPadResponse<Map<String, Object>> submitSandboxTask(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.submitSandboxTask(request));
    }

    @Operation(summary = "沙箱表预览（任务 Modal 即时预览，仅沙箱创建人）")
    @GetMapping("/tasks/sandbox-preview")
    public SecretPadResponse<Map<String, Object>> sandboxPreview(
            @RequestParam String sandboxId,
            @RequestParam String tableName,
            @RequestParam(defaultValue = "20") int limit) {
        return SecretPadResponse.success(service.previewSandboxTable(sandboxId, tableName, limit));
    }

    @Operation(summary = "任务列表（status/runMode/execType/keyword 过滤）")
    @GetMapping("/tasks")
    public SecretPadResponse<List<Map<String, Object>>> listTasks(
            @RequestParam(defaultValue = "") String status,
            @RequestParam(defaultValue = "") String runMode,
            @RequestParam(defaultValue = "") String execType,
            @RequestParam(defaultValue = "") String keyword,
            @RequestParam(defaultValue = "") String sandboxId) {
        return SecretPadResponse.success(service.listTasks(status, runMode, execType, keyword, sandboxId));
    }

    @Operation(summary = "任务详情（含血缘链 + runLogs 摘要）")
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

    @Operation(summary = "失败任务重试（retry_count < maxRetries，run_log attempt=新 retry_count）")
    @PostMapping("/tasks/retry")
    public SecretPadResponse<Map<String, Object>> retryTask(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.retryTask(String.valueOf(request.get("id"))));
    }

    @Operation(summary = "源数据预览（强制权限校验，仅前 limit 行）")
    @GetMapping("/tasks/preview-source")
    public SecretPadResponse<Map<String, Object>> previewSource(
            @RequestParam String nodeId,
            @RequestParam String datatableId,
            @RequestParam(defaultValue = "20") int limit) {
        return SecretPadResponse.success(service.previewSource(
                Map.of("nodeId", nodeId, "datatableId", datatableId, "limit", limit)));
    }

    @Operation(summary = "结果数据集（status=SUCCEEDED 且 result_datatable_id 非空）")
    @GetMapping("/tasks/results")
    public SecretPadResponse<List<Map<String, Object>>> results(@RequestParam(defaultValue = "") String nodeId) {
        return SecretPadResponse.success(service.listResults(nodeId));
    }

    @Operation(summary = "查看任务结果（仅创建人 + SUCCEEDED，DEV 调试预览 / PROD 前 N 行）")
    @GetMapping("/tasks/results/view")
    public SecretPadResponse<Map<String, Object>> viewResult(@RequestParam String taskId) {
        return SecretPadResponse.success(service.viewResult(taskId));
    }

    @Operation(summary = "调试日志全文（attempt 默认最新）")
    @GetMapping("/tasks/log")
    public SecretPadResponse<Map<String, Object>> runLog(
            @RequestParam String taskId,
            @RequestParam(required = false) Integer attempt) {
        return SecretPadResponse.success(service.runLog(taskId, attempt));
    }

    @Operation(summary = "PROD 结果挂载项目（source=IMPORTED）")
    @PostMapping("/tasks/mount")
    public SecretPadResponse<Map<String, Object>> mountResult(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.mountResult(request));
    }
}
