/* Copyright 2026 Ant Group Co., Ltd. Licensed under the Apache License, Version 2.0. */
package org.secretflow.secretpad.web.controller;

import org.secretflow.secretpad.service.model.common.SecretPadResponse;
import org.secretflow.secretpad.web.service.DataComputeService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1alpha1/data-compute")
public class DataComputeController {
    private final DataComputeService service;
    public DataComputeController(DataComputeService service){this.service=service;}
    @GetMapping("/overview") public SecretPadResponse<List<Map<String,Object>>> overview(){return SecretPadResponse.success(service.overview());}
    @GetMapping("/context") public SecretPadResponse<Map<String,Object>> context(@RequestParam String sandboxId){return SecretPadResponse.success(service.context(sandboxId));}
    @GetMapping("/workspace/data") public SecretPadResponse<Map<String,Object>> workspaceData(@RequestParam String sandboxId){return SecretPadResponse.success(service.workspaceData(sandboxId));}
    @PostMapping("/mount-requests") public SecretPadResponse<Map<String,Object>> requestMount(@RequestBody Map<String,Object> request){return SecretPadResponse.success(service.requestMount(request));}
    @GetMapping("/mount-requests") public SecretPadResponse<List<Map<String,Object>>> mountRequests(@RequestParam(defaultValue="") String status){return SecretPadResponse.success(service.mountRequests(status));}
    @GetMapping("/components") public SecretPadResponse<List<Map<String,Object>>> components(@RequestParam String sandboxId){return SecretPadResponse.success(service.components(sandboxId));}
    @PostMapping("/components/publish") public SecretPadResponse<Map<String,Object>> publish(@RequestBody Map<String,Object> request){return SecretPadResponse.success(service.publishComponent(request));}
    @GetMapping("/canvases") public SecretPadResponse<List<Map<String,Object>>> canvases(@RequestParam String sandboxId){return SecretPadResponse.success(service.canvases(sandboxId));}
    @PostMapping("/canvases/save") public SecretPadResponse<Map<String,Object>> saveCanvas(@RequestBody Map<String,Object> request){return SecretPadResponse.success(service.saveCanvas(request));}
    @PostMapping("/canvases/delete") public SecretPadResponse<Map<String,Object>> deleteCanvas(@RequestBody Map<String,Object> request){return SecretPadResponse.success(service.deleteCanvas(request));}
    @GetMapping("/reports") public SecretPadResponse<List<Map<String,Object>>> reports(@RequestParam String sandboxId,@RequestParam(defaultValue="") String type){return SecretPadResponse.success(service.reports(sandboxId,type));}
    @GetMapping("/sandbox-db/directory") public SecretPadResponse<Map<String,Object>> sandboxDbDirectory(@RequestParam String sandboxId){return SecretPadResponse.success(service.sandboxDbDirectory(sandboxId));}
    @GetMapping("/sandbox-db/table-preview") public SecretPadResponse<Map<String,Object>> sandboxDbTablePreview(@RequestParam String sandboxId,@RequestParam String tableName,@RequestParam(defaultValue="20") int limit){return SecretPadResponse.success(service.sandboxDbTablePreview(sandboxId,tableName,limit));}
    @GetMapping("/sandbox-db/table-export") public ResponseEntity<byte[]> sandboxDbTableExport(@RequestParam String sandboxId,@RequestParam String tableName){byte[] bytes=service.sandboxDbTableExport(sandboxId,tableName);String filename=tableName.replaceAll("[^a-zA-Z0-9_-]","_")+".csv";return ResponseEntity.ok().header(org.springframework.http.HttpHeaders.CONTENT_DISPOSITION,"attachment; filename=\""+filename+"\"").contentType(org.springframework.http.MediaType.parseMediaType("text/csv; charset=utf-8")).body(bytes);}
    @GetMapping("/result-controls") public SecretPadResponse<List<Map<String,Object>>> resultControls(@RequestParam String sandboxId){return SecretPadResponse.success(service.resultControls(sandboxId));}
    @PostMapping("/result-controls/save") public SecretPadResponse<Map<String,Object>> saveResultControl(@RequestBody Map<String,Object> request){return SecretPadResponse.success(service.saveResultControl(request));}
}
