/*
 * Copyright 2026 Ant Group Co., Ltd.
 * Licensed under the Apache License, Version 2.0.
 */
package org.secretflow.secretpad.persistence.entity;

import com.fasterxml.jackson.databind.JavaType;
import org.junit.jupiter.api.Test;
import org.secretflow.secretpad.common.dto.SyncDataDTO;
import org.secretflow.secretpad.common.util.JsonUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SandboxApprovalSyncDOTest {

    @Test
    void p2pPayloadKeepsRequiredProjectId() {
        SandboxApprovalSyncDO source = SandboxApprovalSyncDO.builder()
                .upk(new SandboxApprovalSyncDO.UPK("apr-1"))
                .projectId("project-1")
                .applicantNodeId("node-a")
                .snapshotJson("{}")
                .build();
        SyncDataDTO<SandboxApprovalSyncDO> outbound = SyncDataDTO.<SandboxApprovalSyncDO>builder()
                .tableName(SandboxApprovalSyncDO.class.getName())
                .action("create")
                .data(source)
                .build();

        JavaType payloadType = JsonUtils.makeJavaType(SyncDataDTO.class, SandboxApprovalSyncDO.class);
        SyncDataDTO<SandboxApprovalSyncDO> inbound = JsonUtils.toJavaObject(outbound.toJson(), payloadType);

        assertEquals("project-1", inbound.getData().getProjectId());
        assertEquals("node-a", inbound.getData().getApplicantNodeId());
        assertEquals("apr-1", inbound.getData().getUpk().getApprovalId());
    }
}
