/* Copyright 2026 Ant Group Co., Ltd. Licensed under the Apache License, Version 2.0. */
package org.secretflow.secretpad.web.controller;

import org.secretflow.secretpad.service.model.common.SecretPadResponse;
import org.secretflow.secretpad.web.service.canvas.SandboxCanvasService;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 可视化建模画布执行端点：整图/单节点/断点运行、停止、状态轮询、节点输出/日志、模板导入、版本回滚/对比。
 */
@RestController
@RequestMapping("/api/v1alpha1/data-compute/canvas")
public class SandboxCanvasController {

    private final SandboxCanvasService service;

    public SandboxCanvasController(SandboxCanvasService service) {
        this.service = service;
    }

    @PostMapping("/run")
    @SuppressWarnings("unchecked")
    public SecretPadResponse<Map<String, Object>> run(@RequestBody Map<String, Object> request) {
        Object raw = request.get("nodeIds");
        List<String> nodeIds = raw instanceof List<?> list
                ? list.stream().map(String::valueOf).toList()
                : null;
        return SecretPadResponse.success(service.run(String.valueOf(request.get("canvasId")),
                String.valueOf(request.getOrDefault("mode", "ALL")), String.valueOf(request.getOrDefault("nodeId", "")),
                nodeIds));
    }

    @PostMapping("/run/stop")
    public SecretPadResponse<Map<String, Object>> stopRun(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.stopRun(String.valueOf(request.get("runId"))));
    }

    @GetMapping("/run/status")
    public SecretPadResponse<Map<String, Object>> runStatus(@RequestParam String canvasId) {
        return SecretPadResponse.success(service.runStatus(canvasId));
    }

    @GetMapping("/runs")
    public SecretPadResponse<List<Map<String, Object>>> runs(@RequestParam String canvasId) {
        return SecretPadResponse.success(service.runs(canvasId));
    }

    @GetMapping("/node/output")
    public SecretPadResponse<Map<String, Object>> nodeOutput(
            @RequestParam String canvasId, @RequestParam String nodeId,
            @RequestParam(defaultValue = "") String runId,
            @RequestParam(defaultValue = "50") int limit) {
        return SecretPadResponse.success(service.nodeOutput(canvasId, nodeId, runId, limit));
    }

    @GetMapping("/node/logs")
    public SecretPadResponse<Map<String, Object>> nodeLogs(
            @RequestParam String canvasId, @RequestParam String nodeId,
            @RequestParam(defaultValue = "") String runId) {
        return SecretPadResponse.success(service.nodeLogs(canvasId, nodeId, runId));
    }

    /** 节点当前输入数据表（schema + 预览行）：处理列/预测列下拉候选与「查看输入数据表」预览。 */
    @GetMapping("/node/input")
    public SecretPadResponse<Map<String, Object>> nodeInput(
            @RequestParam String canvasId, @RequestParam String nodeId,
            @RequestParam(defaultValue = "20") int limit) {
        return SecretPadResponse.success(service.nodeInput(canvasId, nodeId, limit));
    }

    @GetMapping("/data-resources")
    public SecretPadResponse<Map<String, Object>> dataResources(@RequestParam String sandboxId) {
        return SecretPadResponse.success(service.dataResources(sandboxId));
    }

    @GetMapping("/templates")
    public SecretPadResponse<List<Map<String, Object>>> templates() {
        return SecretPadResponse.success(service.templates());
    }

    @PostMapping("/templates/import")
    public SecretPadResponse<Map<String, Object>> importTemplate(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.importTemplate(String.valueOf(request.get("sandboxId")),
                String.valueOf(request.get("code")), String.valueOf(request.getOrDefault("name", ""))));
    }

    @GetMapping("/versions")
    public SecretPadResponse<List<Map<String, Object>>> versions(@RequestParam String canvasId) {
        return SecretPadResponse.success(service.versions(canvasId));
    }

    @GetMapping("/models")
    public SecretPadResponse<List<Map<String, Object>>> models(@RequestParam String canvasId) {
        return SecretPadResponse.success(service.models(canvasId));
    }

    @GetMapping("/models/report")
    public SecretPadResponse<Map<String, Object>> modelReport(
            @RequestParam String canvasModelId,
            @RequestParam(defaultValue = "") String testId) {
        return SecretPadResponse.success(service.modelReport(canvasModelId, testId));
    }

    /** 按需计算特征重要性：树模型读不纯度重要性、线性模型读系数绝对值，结果落库后由报告接口复用。 */
    @PostMapping("/models/feature-importance")
    public SecretPadResponse<Map<String, Object>> featureImportance(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(
                service.computeFeatureImportance(String.valueOf(request.get("canvasModelId"))));
    }

    /** 按需导出树结构：树模型加载 joblib 后导出指定序号的单棵树，结果落库后由报告接口复用。 */
    @PostMapping("/models/tree-structure")
    public SecretPadResponse<Map<String, Object>> treeStructure(@RequestBody Map<String, Object> request) {
        Object treeIndex = request.get("treeIndex");
        return SecretPadResponse.success(service.computeTreeStructure(
                String.valueOf(request.get("canvasModelId")),
                treeIndex instanceof Number number ? number.intValue() : 0));
    }

    @GetMapping("/models/candidates")
    public SecretPadResponse<List<Map<String, Object>>> modelCandidates(@RequestParam String canvasId) {
        return SecretPadResponse.success(service.modelCandidates(canvasId));
    }

    @PostMapping("/models/save")
    public SecretPadResponse<Map<String, Object>> saveModel(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.saveModel(request));
    }

    @PostMapping("/versions/rollback")
    public SecretPadResponse<Map<String, Object>> rollbackVersion(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.rollbackVersion(String.valueOf(request.get("versionId"))));
    }

    @GetMapping("/versions/compare")
    public SecretPadResponse<Map<String, Object>> compareVersions(
            @RequestParam String versionIdA, @RequestParam String versionIdB) {
        return SecretPadResponse.success(service.compareVersions(versionIdA, versionIdB));
    }
}
