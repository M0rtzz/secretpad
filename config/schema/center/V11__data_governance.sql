/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

-- Z-04 数据抽样与脱敏服务：治理策略 + 治理任务 + 数据血缘
--
-- ds_governance_policy: 可保存复用的抽样/脱敏策略
--   policy_type     SAMPLING(仅抽样) | MASKING(仅脱敏) | SAMPLING_MASKING(先抽样后脱敏)
--   sampling_method RANDOM 随机 | SYSTEMATIC 等距 | STRATIFIED 分层 | CLUSTER 整群/分块
--   sampling_params JSON {count|ratio, strataColumns, clusterColumn, blockSize, seed, limit}
--   masking_columns JSON [{column, method, params}]
--     method MASK 掩码 | REPLACE 替换 | HASH 哈希 | ROUND 取整 | CLEAR 空值/清除
-- ds_governance_task: 每次执行一条记录，exec_params 全快照可追溯
--   exec_mode   BUILTIN(进程内 Java) | CUSTOM(一次性 Kuscia 容器)
--   status      PENDING 待执行 | RUNNING 执行中 | SUCCEEDED 成功 | FAILED 失败 | CANCELLED 取消
--   result_*    结果数据集 = 注册的 Kuscia DomainData(type=table, CSV)
-- ds_governance_lineage: source 数据表 → 策略/任务 → target 结果数据集 全链血缘
create table if not exists ds_governance_policy (
    id              varchar(64)  primary key,        -- 'gp-' + shortId()
    name            varchar(128) not null,
    description     varchar(512) default '',
    policy_type     varchar(16)  not null,            -- SAMPLING / MASKING / SAMPLING_MASKING
    sampling_method varchar(16)  default '',          -- RANDOM/SYSTEMATIC/STRATIFIED/CLUSTER
    sampling_params varchar(2048) default '{}',       -- JSON {count|ratio|strataColumns|clusterColumn|blockSize|seed|limit}
    masking_columns varchar(4096) default '[]',       -- JSON [{column,method,params}]
    created_by      varchar(64)  not null,
    created_at      varchar(32)  not null,
    updated_at      varchar(32)  not null,
    deleted         integer      not null default 0
);
create index if not exists idx_gp_type on ds_governance_policy(policy_type, deleted);

create table if not exists ds_governance_task (
    id                   varchar(64)  primary key,   -- 'gt-' + shortId()
    name                 varchar(128) not null,
    description          varchar(512) default '',
    policy_id            varchar(64)  default '',
    exec_mode            varchar(16)  not null,      -- BUILTIN / CUSTOM
    source_node_id       varchar(64)  not null,
    source_datatable_id  varchar(64)  not null,
    source_relative_uri  varchar(255) default '',
    exec_params          varchar(8192) default '{}', -- 全快照 {sampling:{method,params},masking:[...]}
    script_content       varchar(65535) default '',  -- CUSTOM 脚本文本
    status               varchar(16)  not null default 'PENDING', -- PENDING/RUNNING/SUCCEEDED/FAILED/CANCELLED
    result_node_id       varchar(64)  default '',
    result_datatable_id  varchar(64)  default '',
    source_rows          bigint default 0,
    result_rows          bigint default 0,
    error_message        varchar(2048) default '',
    kuscia_job_id        varchar(128) default '',
    retry_count          integer      not null default 0,
    created_by           varchar(64)  not null,
    created_at           varchar(32)  not null,
    updated_at           varchar(32)  not null default '',
    started_at           varchar(32)  default '',
    finished_at          varchar(32)  default '',
    deleted              integer      not null default 0
);
create index if not exists idx_gt_status on ds_governance_task(status, deleted);
create index if not exists idx_gt_source on ds_governance_task(source_node_id, source_datatable_id, deleted);

create table if not exists ds_governance_lineage (
    id                   integer primary key autoincrement,
    task_id              varchar(64)  not null,
    source_node_id       varchar(64)  not null,
    source_datatable_id  varchar(64)  not null,
    target_node_id       varchar(64)  not null,
    target_datatable_id  varchar(64)  not null,
    op_type              varchar(16)  not null,      -- SAMPLE/MASK/SAMPLE_MASK/CUSTOM
    created_by           varchar(64)  not null,
    created_at           varchar(32)  not null,
    deleted              integer      not null default 0
);
create index if not exists idx_gl_source on ds_governance_lineage(source_node_id, source_datatable_id);
create index if not exists idx_gl_target on ds_governance_lineage(target_node_id, target_datatable_id);
