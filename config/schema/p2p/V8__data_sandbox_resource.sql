/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

-- Z-02 资源调度与隔离：分配生命周期、节点指标、GPU 台账与网络白名单

-- 沙箱资源分配状态：'' | RESERVED(预占) | BOUND(绑定) | RELEASED(释放)
alter table ds_sandbox add column alloc_state varchar(16) not null default '';

-- 资源池新增危险阈值（CRITICAL），区别于 warning_threshold（WARNING）
alter table ds_resource_pool add column critical_threshold real not null default 90;

-- 节点资源指标：ResourceCollector 每轮覆盖写入，保留最近一次有效行；
-- status: FRESH=正常采集 | STALE=采集失败（保留上次有效值）
create table if not exists ds_node_metric (
  id varchar(64) primary key,
  node_id varchar(128) not null,
  cpu_cores real not null default 0,
  cpu_usage_percent real not null default 0,
  memory_total_gb real not null default 0,
  memory_available_gb real not null default 0,
  memory_usage_percent real not null default 0,
  storage_total_gb real not null default 0,
  storage_available_gb real not null default 0,
  storage_usage_percent real not null default 0,
  gpu_utilization_percent real not null default -1,
  source varchar(32) not null default 'prometheus',
  status varchar(16) not null default 'FRESH',
  raw_json varchar(8192) default '',
  created_at varchar(32) not null
);

-- 资源分配生命周期：预占(RESERVED) → 绑定(BOUND) → 释放(RELEASED)
-- released_by: MANUAL | EXPIRE | RECLAIM | DESTROY
create table if not exists ds_resource_allocation (
  id varchar(64) primary key,
  sandbox_id varchar(64) not null,
  resource_type varchar(32) not null,
  amount real not null default 0,
  state varchar(16) not null,
  owner_id varchar(128) not null,
  sandbox_status varchar(32) not null default '',
  bound_at varchar(32) default '',
  released_at varchar(32) default '',
  released_by varchar(16) default '',
  created_at varchar(32) not null
);
create index if not exists idx_ds_resource_allocation_sandbox on ds_resource_allocation(sandbox_id);
create index if not exists idx_ds_resource_allocation_owner_state on ds_resource_allocation(owner_id, state);

-- GPU 台账（真实存在可被调度的 GPU；本环境为台账+配额级，不做容器直通）
create table if not exists ds_gpu_ledger (
  id varchar(64) primary key,
  model varchar(64) not null,
  status varchar(16) not null default 'AVAILABLE',
  owner_id varchar(128) default '',
  allocated_at varchar(32) default '',
  created_at varchar(32) not null
);
insert or ignore into ds_gpu_ledger(id, model, status, created_at) values
  ('gpu-a100-0', 'NVIDIA A100', 'AVAILABLE', datetime('now')),
  ('gpu-a100-1', 'NVIDIA A100', 'AVAILABLE', datetime('now')),
  ('gpu-a100-2', 'NVIDIA A100', 'AVAILABLE', datetime('now')),
  ('gpu-a100-3', 'NVIDIA A100', 'AVAILABLE', datetime('now'));

-- 网络白名单（ALLOW_LIST 策略的放行登记；egress 强制受平台限制，见隔离验证报告）
create table if not exists ds_network_allowlist (
  id varchar(64) primary key,
  sandbox_id varchar(64) not null,
  host varchar(256) not null,
  port integer not null default 0,
  proto varchar(16) not null default 'tcp',
  remark varchar(512) default '',
  created_by varchar(128) default '',
  created_at varchar(32) not null
);
create index if not exists idx_ds_network_allowlist_sandbox on ds_network_allowlist(sandbox_id);

-- 存量沙箱回填：未销毁沙箱按规格生成 RESERVED 分配行（幂等，可安全重复执行）
insert or ignore into ds_resource_allocation(id, sandbox_id, resource_type, amount, state, owner_id, sandbox_status, created_at)
select 'alloc-' || replace(s.id, '-', '') || '-CPU', s.id, 'CPU', s.cpu_cores, 'RESERVED', s.owner_id, s.status, datetime('now')
from ds_sandbox s where s.deleted=0 and s.status<>'DESTROYED';
insert or ignore into ds_resource_allocation(id, sandbox_id, resource_type, amount, state, owner_id, sandbox_status, created_at)
select 'alloc-' || replace(s.id, '-', '') || '-MEMORY', s.id, 'MEMORY', s.memory_gb, 'RESERVED', s.owner_id, s.status, datetime('now')
from ds_sandbox s where s.deleted=0 and s.status<>'DESTROYED';
insert or ignore into ds_resource_allocation(id, sandbox_id, resource_type, amount, state, owner_id, sandbox_status, created_at)
select 'alloc-' || replace(s.id, '-', '') || '-GPU', s.id, 'GPU', s.gpu_count, 'RESERVED', s.owner_id, s.status, datetime('now')
from ds_sandbox s where s.deleted=0 and s.status<>'DESTROYED' and s.gpu_count>0;
insert or ignore into ds_resource_allocation(id, sandbox_id, resource_type, amount, state, owner_id, sandbox_status, created_at)
select 'alloc-' || replace(s.id, '-', '') || '-STORAGE', s.id, 'STORAGE', s.storage_gb, 'RESERVED', s.owner_id, s.status, datetime('now')
from ds_sandbox s where s.deleted=0 and s.status<>'DESTROYED';
