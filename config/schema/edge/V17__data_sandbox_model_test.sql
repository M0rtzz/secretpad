/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

-- Z-06 模型测试执行与 API 发布：审批单绑定制品/版本 + 测试证据 + 受控模型 API
--
-- ds_model_approval(ALTER): 现有 V6 审批单表扩展现有列——绑定实际模型制品/版本 + 测试证据
--   artifact_id / artifact_version_id  绑定 Z-05 制品 ds_dev_artifact / ds_dev_artifact_version
--   test_evidence  JSON {testId,metrics,ranAt}，测试收官时写入（与旧流程行并存，向后兼容）
-- ds_dev_task(ALTER):      执行通道 + 结果文件路径
--   channel    dev(默认,Z-05) / model(模型测试,调度器轮询) / api(API 调用,同步不收官)
--   result_uri 结果 CSV 相对路径（model/api 或 PROD 时落盘，供指标计算/调用取数）
-- ds_model:               模型注册表——绑定制品+版本+项目（仅 JAR/PYTHON，SQL 非模型）
--   node_id 执行/调用运行节点（注册时取项目首个节点或创建人平台节点）
--   version 同项目同制品重注册自增（代码判重，不加 DB 唯一约束）
-- ds_model_test:          一次测试执行一行，参数/摘要/指标 JSON 全量可追溯
--   input_summary {header,rowCount,columnCount} 输入测试集摘要
--   output_summary {header,rowCount,previewRows} 输出结果摘要
--   metrics {metricType,...} 评估指标（分类 accuracy/P/R/F1；回归 MAE/RMSE/R²）
-- ds_model_api:           受控模型 API——调用凭证 + 授权用户 + IP 白名单 + 有效时间
--   app_id/secret_hash  调用凭证（sha256 存储，明文一次性展示）
--   authorized_users JSON 用户名数组（空=仅凭证调用）；ip_whitelist JSON IP/CIDR（空=任意 IP）
--   valid_from/valid_to 有效时间窗口
alter table ds_model_approval add column artifact_id varchar(64) not null default '';
alter table ds_model_approval add column artifact_version_id varchar(64) not null default '';
alter table ds_model_approval add column test_evidence varchar(4096) not null default '';

alter table ds_dev_task add column channel varchar(16) not null default 'dev';
alter table ds_dev_task add column result_uri varchar(255) default '';

create table if not exists ds_model (
    id varchar(64) primary key,                 -- 'dm-' + shortId()
    name varchar(128) not null,
    description varchar(512) default '',
    project_id varchar(128) not null,
    artifact_id varchar(64) not null,           -- 仅 JAR/PYTHON（SQL 非模型，注册即拒）
    artifact_version_id varchar(64) not null,
    node_id varchar(64) default '',             -- 执行/调用运行节点
    version integer not null default 1,         -- 同项目同制品重注册自增
    status varchar(16) not null default 'DRAFT',-- DRAFT/APPROVING/APPROVED/REJECTED/PUBLISHED/OFFLINE
    created_by varchar(128) not null,           -- username
    created_by_owner varchar(128) default '',   -- ownerId（权限回退判定）
    created_at varchar(32) not null,
    updated_at varchar(32) not null,
    approved_at varchar(32) default '',
    published_at varchar(32) default '',
    deleted integer not null default 0
);
create index if not exists idx_dm_project on ds_model(project_id, deleted);
create index if not exists idx_dm_artifact on ds_model(project_id, artifact_id, deleted);
create index if not exists idx_dm_status on ds_model(status, deleted);

create table if not exists ds_model_test (
    id varchar(64) primary key,                 -- 'mt-' + shortId()
    model_id varchar(64) not null,
    approval_id varchar(64) default '',         -- 提交审批后绑定当前审批单
    task_id varchar(64) not null,               -- 关联 ds_dev_task（channel='model'）
    run_mode varchar(8) not null default 'DEV', -- DEV/PROD
    exec_type varchar(8) not null,              -- JAR/PYTHON
    source_node_id varchar(64) not null,
    source_datatable_id varchar(64) not null,
    source_relative_uri varchar(255) default '',
    params varchar(8192) default '{}',
    label_column varchar(128) default '',       -- 真实列（输入 CSV）
    prediction_column varchar(128) default '',  -- 预测列（结果 CSV）
    metric_type varchar(16) default 'auto',     -- auto/classification/regression
    status varchar(16) not null default 'RUNNING', -- RUNNING/SUCCEEDED/FAILED/CANCELLED
    input_summary varchar(4096) default '{}',   -- {header,rowCount,columnCount}
    output_summary varchar(4096) default '{}',  -- {header,rowCount,previewRows}
    metrics varchar(4096) default '{}',         -- {metricType,...}
    result_preview varchar(8192) default '',
    error_message varchar(2048) default '',
    created_by varchar(128) not null,
    created_at varchar(32) not null,
    updated_at varchar(32) not null,
    started_at varchar(32) default '',
    finished_at varchar(32) default '',
    deleted integer not null default 0
);
create index if not exists idx_mt_model on ds_model_test(model_id, deleted);
create index if not exists idx_mt_approval on ds_model_test(approval_id, status);

create table if not exists ds_model_api (
    id varchar(64) primary key,                 -- 'mapi-' + shortId()
    model_id varchar(64) not null,
    name varchar(128) not null,
    description varchar(512) default '',
    status varchar(16) not null default 'ENABLED', -- ENABLED/DISABLED
    app_id varchar(128) not null unique,        -- 'ai-' + shortId()
    secret_hash varchar(128) not null,          -- sha256(secret)
    authorized_users varchar(4096) default '[]',-- JSON 用户名数组（空=仅凭证调用）
    ip_whitelist varchar(4096) default '[]',    -- JSON IP/CIDR 数组（空=任意 IP）
    valid_from varchar(32) default '',
    valid_to varchar(32) default '',
    call_count bigint not null default 0,
    last_called_at varchar(32) default '',
    created_by varchar(128) not null,
    created_at varchar(32) not null,
    updated_at varchar(32) not null,
    deleted integer not null default 0
);
create index if not exists idx_map_model on ds_model_api(model_id, deleted);
