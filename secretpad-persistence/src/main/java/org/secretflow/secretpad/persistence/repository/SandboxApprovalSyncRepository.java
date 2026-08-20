/*
 * Copyright 2026 Ant Group Co., Ltd.
 * Licensed under the Apache License, Version 2.0.
 */
package org.secretflow.secretpad.persistence.repository;

import org.secretflow.secretpad.persistence.entity.SandboxApprovalSyncDO;
import org.springframework.stereotype.Repository;

/** Repository for P2P-synchronized sandbox approval snapshots. */
@Repository
public interface SandboxApprovalSyncRepository extends BaseRepository<SandboxApprovalSyncDO, SandboxApprovalSyncDO.UPK> {
}
