/*
 * Copyright 2026 Ant Group Co., Ltd.
 * Licensed under the Apache License, Version 2.0.
 */
package org.secretflow.secretpad.persistence.repository;

import org.secretflow.secretpad.persistence.entity.ProjectAssetDO;
import org.springframework.stereotype.Repository;

import java.util.List;

/** Repository for P2P-synchronized project asset metadata. */
@Repository
public interface ProjectAssetRepository extends BaseRepository<ProjectAssetDO, ProjectAssetDO.UPK> {
    List<ProjectAssetDO> findByUpkProjectId(String projectId);
}
