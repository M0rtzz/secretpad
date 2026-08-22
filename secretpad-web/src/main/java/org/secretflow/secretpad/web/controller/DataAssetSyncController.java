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

import org.secretflow.secretpad.web.service.sync.AssetSyncService;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 跨节点资产同步端点（provider 侧）：请求方节点经 Kuscia gateway（Host 路由）调用本端点
 * 拉取已授权的 PROCESSED 数据，响应携带 {@code X-Asset-Sha256} 供请求方端到端校验。
 */
@RestController
@RequestMapping("/api/v1alpha1/data-assets/sync")
public class DataAssetSyncController {

    private final AssetSyncService assetSyncService;

    public DataAssetSyncController(AssetSyncService assetSyncService) {
        this.assetSyncService = assetSyncService;
    }

    @GetMapping("/download")
    public ResponseEntity<byte[]> download(@RequestParam String assetId,
            @RequestHeader(value = "kuscia-origin-source", required = false) String headerRequester,
            @RequestParam(required = false) String requesterNodeId) {
        String requester = headerRequester != null && !headerRequester.isBlank()
                ? headerRequester : requesterNodeId;
        AssetSyncService.AssetDownload dl = assetSyncService.download(assetId, requester);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);
        headers.add("X-Asset-Sha256", dl.sha256());
        return new ResponseEntity<>(dl.bytes(), headers, HttpStatus.OK);
    }
}
