/*
 * Copyright 2026 Ant Group Co., Ltd.
 * Licensed under the Apache License, Version 2.0.
 */
package org.secretflow.secretpad.persistence.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.hibernate.annotations.Where;

import java.io.Serial;
import java.io.Serializable;

/** Project-scoped sandbox approval snapshot synchronized between P2P participants. */
@Entity
@Builder
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "ds_sandbox_approval_sync")
@Where(clause = "is_deleted = 0")
public class SandboxApprovalSyncDO extends BaseAggregationRoot<SandboxApprovalSyncDO> {

    @EmbeddedId
    private UPK upk;

    @Column(name = "project_id", nullable = false)
    private String projectId;

    @Column(name = "applicant_node_id", nullable = false)
    private String applicantNodeId;

    @Column(name = "snapshot_json", nullable = false, columnDefinition = "text")
    private String snapshotJson;

    @Override
    public String getProjectId() {
        return projectId;
    }

    @Override
    @JsonIgnore
    public String getNodeId() {
        return applicantNodeId;
    }

    @Getter
    @Setter
    @ToString
    @EqualsAndHashCode
    @NoArgsConstructor
    @AllArgsConstructor
    public static class UPK implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;

        @Column(name = "approval_id", nullable = false, length = 64)
        private String approvalId;
    }
}
