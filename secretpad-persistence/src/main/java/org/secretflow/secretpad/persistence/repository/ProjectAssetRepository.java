/*
 * Copyright 2026 Ant Group Co., Ltd.
 * Licensed under the Apache License, Version 2.0.
 */
package org.secretflow.secretpad.persistence.repository;

import org.secretflow.secretpad.persistence.entity.ProjectAssetDO;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/** Repository for P2P-synchronized project asset metadata. */
@Repository
public interface ProjectAssetRepository extends BaseRepository<ProjectAssetDO, ProjectAssetDO.UPK> {
    List<ProjectAssetDO> findByUpkProjectId(String projectId);

    /** Update a soft-deleted relation without being affected by the entity-level {@code @Where}. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Transactional
    @Query(value = "update ds_project_asset set is_deleted = 1, gmt_modified = :gmtModified "
            + "where project_id = :projectId and asset_id = :assetId", nativeQuery = true)
    int softDeleteIncludingDeleted(
            @Param("projectId") String projectId,
            @Param("assetId") String assetId,
            @Param("gmtModified") String gmtModified);
}
