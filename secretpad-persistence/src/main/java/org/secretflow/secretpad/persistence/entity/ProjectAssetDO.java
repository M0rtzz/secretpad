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

/** Project-scoped data asset metadata synchronized between P2P participants. */
@Entity
@Builder
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "ds_project_asset")
@Where(clause = "is_deleted = 0")
public class ProjectAssetDO extends BaseAggregationRoot<ProjectAssetDO> {

    @EmbeddedId
    private UPK upk;

    @Column(name = "provider_node_id", nullable = false)
    private String providerNodeId;

    @Column(name = "asset_json", nullable = false, columnDefinition = "text")
    private String assetJson;

    @Column(name = "attached_by")
    private String attachedBy;

    @Column(name = "attached_at")
    private String attachedAt;

    @Column(name = "expires_at")
    private String expiresAt;

    @Override
    @JsonIgnore
    public String getProjectId() {
        return upk == null ? null : upk.projectId;
    }

    @Override
    @JsonIgnore
    public String getNodeId() {
        return providerNodeId;
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

        @Column(name = "project_id", nullable = false, length = 64)
        private String projectId;

        @Column(name = "asset_id", nullable = false, length = 64)
        private String assetId;
    }
}
