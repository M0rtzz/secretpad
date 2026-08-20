/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

-- Z-05 计算任务开发能力：制品管理 + 版本管理 + 任务执行 + 依赖白名单 + 调试日志
--
-- ds_dev_artifact:        可保存复用的计算制品（JAR/SQL/PYTHON）
--   type   JAR(Java 程序包) | SQL(SQL 脚本) | PYTHON(Python 函数/脚本)
-- ds_dev_artifact_version: 制品每次保存/编辑生成一个新版本（version 自增，不可变）
--   content_text  SQL/PYTHON 脚本全文；file_path  JAR 相对路径（存库不含 nodeId，读取时拼接 storeDir/{nodeId}）
--   sha256/size   内容校验；params_schema 参数声明 JSON；default_params 缺省参数
--   dependency_names PYTHON 依赖名列表 JSON（须在白名单 ds_dev_dependency 内）
-- ds_dev_task:           每次执行一条记录，params/脚本快照全量可追溯
--   run_mode  DEV 调试运行（即时返回日志+结果预览，不注册结果表）
--             PROD 正式运行（全审计 + 注册结果 Kuscia DomainData + 血缘 + 可挂载项目）
--   exec_type JAR/SQL/PYTHON；status PENDING/RUNNING/SUCCEEDED/FAILED/CANCELLED
-- ds_dev_dependency:     Python 依赖库白名单（runner 无网络、禁 pip、仅预装白名单包，导入前校验）
-- ds_dev_run_log:        每次尝试（attempt=retry_count）的调试日志全文
create table if not exists ds_dev_artifact (
    id              varchar(64)  primary key,        -- 'da-' + shortId()
    name            varchar(128) not null,
    type            varchar(8)   not null,           -- JAR / SQL / PYTHON
    description     varchar(512) default '',
    latest_version  integer      not null default 0,
    created_by      varchar(64)  not null,
    created_at      varchar(32)  not null,
    updated_at      varchar(32)  not null,
    deleted         integer      not null default 0
);
create index if not exists idx_da_type on ds_dev_artifact(type, deleted);

create table if not exists ds_dev_artifact_version (
    id               varchar(64)  primary key,       -- 'dav-' + shortId()
    artifact_id      varchar(64)  not null,
    version          integer      not null,
    content_text     varchar(65535) default '',      -- SQL/PYTHON 脚本全文
    file_path        varchar(255) default '',        -- JAR 相对路径（不含 nodeId）
    sha256           varchar(64)  default '',
    size             bigint       default 0,
    params_schema    varchar(4096) default '[]',     -- JSON [{name,type,required,default,description}]
    default_params   varchar(2048) default '{}',     -- JSON {name: value}
    dependency_names varchar(2048) default '[]',     -- JSON ["numpy","pandas"]
    description      varchar(512) default '',
    created_by       varchar(64)  not null,
    created_at       varchar(32)  not null,
    deleted          integer      not null default 0,
    unique(artifact_id, version)
);
create index if not exists idx_dav_artifact on ds_dev_artifact_version(artifact_id, version);

create table if not exists ds_dev_task (
    id                   varchar(64)  primary key,   -- 'dt-' + shortId()
    name                 varchar(128) not null,
    description          varchar(512) default '',
    artifact_id          varchar(64)  default '',    -- 引用制品；adhoc 提交也自动落制品（任务保存）
    version              integer      default 0,
    run_mode             varchar(8)   not null,      -- DEV / PROD
    exec_type            varchar(8)   not null,      -- JAR / SQL / PYTHON
    source_node_id       varchar(64)  not null,
    source_datatable_id  varchar(64)  not null,
    source_relative_uri  varchar(255) default '',
    params               varchar(8192) default '{}', -- 执行参数全快照
    content_snapshot     varchar(65535) default '',  -- SQL/PYTHON 脚本快照（JAR 为空，重跑按版本重读文件）
    dependency_names     varchar(2048) default '[]',
    status               varchar(16)  not null default 'PENDING', -- PENDING/RUNNING/SUCCEEDED/FAILED/CANCELLED
    result_node_id       varchar(64)  default '',
    result_datatable_id  varchar(64)  default '',
    result_preview       varchar(8192) default '',   -- JSON {header:[...],rows:[[...]]} DEV 调试预览/PROD 前 N 行
    source_rows          bigint       default 0,
    result_rows          bigint       default 0,
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
create index if not exists idx_dt_status on ds_dev_task(status, deleted);
create index if not exists idx_dt_source on ds_dev_task(source_node_id, source_datatable_id, deleted);
create index if not exists idx_dt_artifact on ds_dev_task(artifact_id, version);

create table if not exists ds_dev_dependency (
    id              varchar(64)  primary key,        -- 'dep-' + shortId()
    name            varchar(128) not null,
    version_spec    varchar(64)  default '',
    description     varchar(512) default '',
    enabled         integer      not null default 1,
    created_by      varchar(64)  not null,
    created_at      varchar(32)  not null,
    updated_at      varchar(32)  not null,
    deleted         integer      not null default 0
);
create index if not exists idx_dd_enabled on ds_dev_dependency(enabled, deleted);
-- 预置与 data-sandbox-python-runner 镜像内预装一致的白名单（新条目须同步重镜像）
insert or ignore into ds_dev_dependency(id,name,version_spec,description,enabled,created_by,created_at,updated_at,deleted)
values('dep-numpy','numpy','>=1.24','NumPy 数值计算',1,'system','2026-08-19 00:00:00','2026-08-19 00:00:00',0);
insert or ignore into ds_dev_dependency(id,name,version_spec,description,enabled,created_by,created_at,updated_at,deleted)
values('dep-pandas','pandas','>=2.0','Pandas 数据分析',1,'system','2026-08-19 00:00:00','2026-08-19 00:00:00',0);

create table if not exists ds_dev_run_log (
    id          varchar(64)  primary key,            -- 'dl-' + shortId()
    task_id     varchar(64)  not null,
    attempt     integer      not null default 0,     -- 第几次执行（= retry_count）
    log_text    varchar(65535) default '',
    created_at  varchar(32)  not null
);
create index if not exists idx_dl_task on ds_dev_run_log(task_id, attempt);
