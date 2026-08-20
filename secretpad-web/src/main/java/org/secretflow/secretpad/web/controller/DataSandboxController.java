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
import org.secretflow.secretpad.web.service.DataSandboxMvpService;
import org.secretflow.secretpad.web.service.sandbox.SandboxApprovalGate;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/** Data Sandbox MVP management APIs. */
@Tag(name = "Data Sandbox MVP", description = "沙箱、资源、模型审批、日志、系统对接与运维管理")
@RestController
@RequestMapping("/api/v1alpha1/data-sandbox")
public class DataSandboxController {

    private final DataSandboxMvpService service;
    private final SandboxApprovalGate gate;

    public DataSandboxController(DataSandboxMvpService service, SandboxApprovalGate gate) {
        this.service = service;
        this.gate = gate;
    }

    @Operation(summary = "查询沙箱")
    @GetMapping("/sandboxes")
    public SecretPadResponse<List<Map<String, Object>>> sandboxes(
            @RequestParam(defaultValue = "") String ownerId,
            @RequestParam(defaultValue = "") String keyword,
            @RequestParam(defaultValue = "") String status) {
        return SecretPadResponse.success(service.listSandboxes(ownerId, keyword, status));
    }

    @Operation(summary = "创建沙箱")
    @PostMapping("/sandboxes/create")
    public SecretPadResponse<Map<String, Object>> createSandbox(@RequestBody Map<String, Object> request) {
        // Z-03 门禁：approval.required 开启且非 admin/运营方时，直接创建被拒，需提交申请单
        gate.assertDirectCreateAllowed();
        return SecretPadResponse.success(service.createSandbox(request));
    }

    @Operation(summary = "沙箱启停、销毁、续期或快照")
    @PostMapping("/sandboxes/action")
    public SecretPadResponse<Map<String, Object>> sandboxAction(@RequestBody Map<String, Object> request) {
        // Z-03 门禁：RENEW/DESTROY 在 approval.required 开启且非 admin/运营方时需走申请单；START/STOP/SNAPSHOT 不设门禁
        String action = String.valueOf(request.get("action"));
        if ("RENEW".equals(action) || "DESTROY".equals(action)) {
            gate.assertDirectActionAllowed(action);
        }
        return SecretPadResponse.success(service.sandboxAction(request));
    }

    @Operation(summary = "签发开发环境访问 token（一次性，30 分钟有效）")
    @PostMapping("/sandboxes/dev-token")
    public SecretPadResponse<Map<String, Object>> devToken(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.generateDevToken(String.valueOf(request.get("id"))));
    }

    @GetMapping("/snapshots")
    public SecretPadResponse<List<Map<String, Object>>> snapshots(@RequestParam String sandboxId) {
        return SecretPadResponse.success(service.listSnapshots(sandboxId));
    }

    @GetMapping("/images")
    public SecretPadResponse<List<Map<String, Object>>> images() {
        return SecretPadResponse.success(service.listImages());
    }

    @PostMapping("/images/save")
    public SecretPadResponse<Map<String, Object>> saveImage(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.saveImage(request));
    }

    @Operation(summary = "资源池、配额、分配和使用率")
    @GetMapping("/resources/overview")
    public SecretPadResponse<Map<String, Object>> resourceOverview(@RequestParam(defaultValue = "") String ownerId) {
        return SecretPadResponse.success(service.resourceOverview(ownerId));
    }

    @PostMapping("/resources/quota")
    public SecretPadResponse<Map<String, Object>> saveQuota(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.saveQuota(request));
    }

    @GetMapping("/resources/alerts")
    public SecretPadResponse<List<Map<String, Object>>> alerts(@RequestParam(defaultValue = "") String status) {
        return SecretPadResponse.success(service.listAlerts(status));
    }

    @PostMapping("/resources/alerts/resolve")
    public SecretPadResponse<Void> resolveAlert(@RequestBody Map<String, Object> request) {
        service.resolveAlert(String.valueOf(request.get("id")));
        return SecretPadResponse.success();
    }

    @Operation(summary = "网络白名单列表（ALLOW_LIST 策略放行登记）")
    @GetMapping("/resources/network/allowlist")
    public SecretPadResponse<List<Map<String, Object>>> networkAllowlist(
            @RequestParam(defaultValue = "") String sandboxId) {
        return SecretPadResponse.success(service.listNetworkAllowlist(sandboxId));
    }

    @Operation(summary = "新增网络白名单条目")
    @PostMapping("/resources/network/allowlist")
    public SecretPadResponse<Map<String, Object>> addNetworkAllowlist(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.addNetworkAllowlist(request));
    }

    @Operation(summary = "删除网络白名单条目")
    @PostMapping("/resources/network/allowlist/delete")
    public SecretPadResponse<Void> deleteNetworkAllowlist(@RequestBody Map<String, Object> request) {
        service.deleteNetworkAllowlist(String.valueOf(request.get("id")));
        return SecretPadResponse.success();
    }

    @Operation(summary = "模型审批列表")
    @GetMapping("/models")
    public SecretPadResponse<List<Map<String, Object>>> approvals(
            @RequestParam(defaultValue = "") String status,
            @RequestParam(defaultValue = "") String keyword) {
        return SecretPadResponse.success(service.listApprovals(status, keyword));
    }

    @PostMapping("/models/submit")
    public SecretPadResponse<Map<String, Object>> submitModel(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.submitModel(request));
    }

    @PostMapping("/models/action")
    public SecretPadResponse<Map<String, Object>> approvalAction(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.approvalAction(request));
    }

    @GetMapping("/models/history")
    public SecretPadResponse<List<Map<String, Object>>> approvalHistory(@RequestParam String id) {
        return SecretPadResponse.success(service.approvalHistory(id));
    }

    @Operation(summary = "统一日志检索")
    @GetMapping("/logs")
    public SecretPadResponse<List<Map<String, Object>>> logs(
            @RequestParam(defaultValue = "") String type,
            @RequestParam(defaultValue = "") String level,
            @RequestParam(defaultValue = "") String actor,
            @RequestParam(defaultValue = "") String keyword,
            @RequestParam(defaultValue = "") String start,
            @RequestParam(defaultValue = "") String end,
            @RequestParam(defaultValue = "500") int limit) {
        return SecretPadResponse.success(service.listLogs(type, level, actor, keyword, start, end, limit));
    }

    @Operation(summary = "导出日志 CSV")
    @GetMapping("/logs/export")
    public ResponseEntity<byte[]> exportLogs(
            @RequestParam(defaultValue = "") String type,
            @RequestParam(defaultValue = "") String level,
            @RequestParam(defaultValue = "") String actor,
            @RequestParam(defaultValue = "") String keyword,
            @RequestParam(defaultValue = "") String start,
            @RequestParam(defaultValue = "") String end) {
        byte[] content = service.exportLogs(type, level, actor, keyword, start, end);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(new MediaType("text", "csv", StandardCharsets.UTF_8));
        headers.setContentDisposition(ContentDisposition.attachment().filename("data-sandbox-logs-" + LocalDate.now() + ".csv").build());
        return ResponseEntity.ok().headers(headers).body(content);
    }

    @GetMapping("/logs/retention")
    public SecretPadResponse<List<Map<String, Object>>> retention() {
        return SecretPadResponse.success(service.retentionPolicies());
    }

    @PostMapping("/logs/retention")
    public SecretPadResponse<Void> saveRetention(@RequestBody Map<String, Object> request) {
        service.saveRetention(request);
        return SecretPadResponse.success();
    }

    @Operation(summary = "对接配置总览")
    @GetMapping("/integrations")
    public SecretPadResponse<Map<String, Object>> integrations() {
        return SecretPadResponse.success(service.integrationOverview());
    }

    @PostMapping("/integrations/clients/create")
    public SecretPadResponse<Map<String, Object>> createClient(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.createApiClient(request));
    }

    @PostMapping("/integrations/clients/revoke")
    public SecretPadResponse<Void> revokeClient(@RequestBody Map<String, Object> request) {
        service.revokeApiClient(String.valueOf(request.get("id")));
        return SecretPadResponse.success();
    }

    @PostMapping("/integrations/clients/rotate")
    public SecretPadResponse<Map<String, Object>> rotateClient(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.rotateApiClient(String.valueOf(request.get("id"))));
    }

    @PostMapping("/integrations/webhooks/save")
    public SecretPadResponse<Map<String, Object>> saveWebhook(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.saveWebhook(request));
    }

    @PostMapping("/integrations/webhooks/test")
    public SecretPadResponse<Map<String, Object>> testWebhook(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.testWebhook(String.valueOf(request.get("id"))));
    }

    @PostMapping("/integrations/deliveries/retry")
    public SecretPadResponse<Map<String, Object>> retryDelivery(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.retryDelivery(String.valueOf(request.get("id"))));
    }

    @PostMapping("/integrations/oidc/save")
    public SecretPadResponse<Map<String, Object>> saveOidc(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.saveOidc(request));
    }

    @PostMapping("/integrations/oidc/test")
    public SecretPadResponse<Map<String, Object>> testOidc() {
        return SecretPadResponse.success(service.testOidc());
    }

    @PostMapping("/integrations/oidc/mappings/save")
    public SecretPadResponse<Map<String, Object>> saveOidcMapping(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.saveOidcMapping(request));
    }

    @PostMapping("/integrations/oidc/mappings/delete")
    public SecretPadResponse<Void> deleteOidcMapping(@RequestBody Map<String, Object> request) {
        service.deleteOidcMapping(String.valueOf(request.get("id")));
        return SecretPadResponse.success();
    }

    @GetMapping("/tenants")
    public SecretPadResponse<List<Map<String, Object>>> tenants() {
        return SecretPadResponse.success(service.tenants());
    }

    @PostMapping("/tenants/open")
    public SecretPadResponse<Map<String, Object>> openTenant(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.openTenant(request));
    }

    @PostMapping("/tenants/resize")
    public SecretPadResponse<Map<String, Object>> resizeTenant(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.resizeTenant(request));
    }

    @PostMapping("/tenants/deploy")
    public SecretPadResponse<Map<String, Object>> deployTenant(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.deployTenant(String.valueOf(request.get("tenantId"))));
    }

    @GetMapping("/billing/usage")
    public SecretPadResponse<List<Map<String, Object>>> billingUsage(@RequestParam String tenantId) {
        return SecretPadResponse.success(service.billingUsage(tenantId));
    }

    @PostMapping("/billing/calculate")
    public SecretPadResponse<Map<String, Object>> calculateBilling(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.calculateBilling(request));
    }

    @GetMapping("/trusted/exchanges")
    public SecretPadResponse<List<Map<String, Object>>> trustedExchanges(@RequestParam(defaultValue = "") String tenantId) {
        return SecretPadResponse.success(service.trustedExchanges(tenantId));
    }

    @PostMapping("/trusted/push")
    public SecretPadResponse<Map<String, Object>> trustedPush(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.trustedPush(request));
    }

    @Operation(summary = "运维总览")
    @GetMapping("/operations")
    public SecretPadResponse<Map<String, Object>> operations() {
        return SecretPadResponse.success(service.operationOverview());
    }

    @PostMapping("/operations/backups/create")
    public SecretPadResponse<Map<String, Object>> createBackup() {
        return SecretPadResponse.success(service.createBackup());
    }

    @PostMapping("/operations/backups/restore")
    public SecretPadResponse<Map<String, Object>> restoreBackup(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.stageRestore(String.valueOf(request.get("id"))));
    }

    @PostMapping("/operations/diagnostics")
    public SecretPadResponse<Map<String, Object>> diagnostics() {
        return SecretPadResponse.success(service.diagnostics());
    }

    @Operation(summary = "沙箱资源限制生效校验（期望值 + 运维核对指引）")
    @PostMapping("/operations/limit-verify")
    public SecretPadResponse<Map<String, Object>> limitVerify(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.limitVerify(String.valueOf(request.get("sandboxId"))));
    }

    @GetMapping("/operations/help")
    public SecretPadResponse<List<Map<String, Object>>> help() {
        return SecretPadResponse.success(service.helpArticles());
    }

    @PostMapping("/operations/tickets/create")
    public SecretPadResponse<Map<String, Object>> createTicket(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.createTicket(request));
    }

    @PostMapping("/operations/tickets/update")
    public SecretPadResponse<Map<String, Object>> updateTicket(@RequestBody Map<String, Object> request) {
        return SecretPadResponse.success(service.updateTicket(request));
    }
}
