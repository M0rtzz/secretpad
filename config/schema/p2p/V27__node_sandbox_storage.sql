-- Node-level physical storage + sandbox authoritative SQLite database.
--
-- ds_node_dataset:      node-level physical index in the platform DB. The actual
--                       rows live in node_data.db (TABULAR) or MinIO (IMAGE).
-- ds_sandbox_data_dir:  sandbox data directory (initial mounts + dev results),
--                       the source for the frontend "沙箱数据目录".
-- ds_sandbox_db:        per-sandbox authoritative SQLite tracking.
-- ds_asset_sync_record: cross-node asset sync log (PHYSICAL for PROCESSED,
--                       SCHEMA-only for RAW source).
-- ds_dev_task:          table-name contract columns for the sandbox compute flow.
create table if not exists ds_node_dataset (
  id                  varchar(64)   primary key,
  asset_id            varchar(64)   not null,
  node_id             varchar(64)   not null,
  modality            varchar(16)   not null,   -- TABULAR / IMAGE
  physical_kind       varchar(24)   not null,   -- SQLITE_TABLE / MINIO_OBJECT
  table_name          varchar(128)  default '',
  table_columns_json  text          not null default '[]',
  row_count           bigint        not null default 0,
  storage_ref         varchar(1024) default '',
  checksum            varchar(128)  default '',
  provenance_json     text          not null default '{}',  -- {sourceAssetId,providerNodeId,syncedAt}
  created_at          varchar(32)   not null,
  updated_at          varchar(32)   not null,
  deleted             integer       not null default 0
);
create index if not exists idx_ds_node_dataset_asset on ds_node_dataset(asset_id, deleted);
create index if not exists idx_ds_node_dataset_node on ds_node_dataset(node_id, deleted);

create table if not exists ds_sandbox_data_dir (
  id            varchar(64)   primary key,
  sandbox_id    varchar(64)   not null,
  kind          varchar(16)   not null,   -- MOUNT / RESULT
  asset_id      varchar(64)   default '',
  table_name    varchar(128)  default '',
  name          varchar(256)  not null,
  modality      varchar(16)   default 'TABULAR',
  row_count     bigint        default 0,
  columns_json  text          default '[]',
  source        varchar(16)   default 'LOCAL',   -- LOCAL / SYNCED
  created_at    varchar(32)   not null,
  updated_at    varchar(32)   not null,
  deleted       integer       not null default 0
);
create index if not exists idx_ds_sandbox_data_dir on ds_sandbox_data_dir(sandbox_id, deleted);

create table if not exists ds_sandbox_db (
  sandbox_id  varchar(64)   primary key,
  db_path     varchar(1024) not null,
  file_size   bigint        default 0,
  checksum    varchar(128)  default '',
  table_count integer       default 0,
  row_count   bigint        default 0,
  built_at    varchar(32)   default '',
  status      varchar(16)   default 'READY'
);

create table if not exists ds_asset_sync_record (
  id               varchar(64)   primary key,
  project_id       varchar(128)  not null,
  asset_id         varchar(64)   not null,   -- source (provider) asset
  local_asset_id   varchar(64)   default '', -- locally materialized asset (PHYSICAL sync)
  provider_node_id varchar(64)   not null,
  sync_mode        varchar(16)   not null,   -- PHYSICAL / SCHEMA
  status           varchar(16)   not null,   -- PENDING / SYNCED / FAILED
  synced_at        varchar(32)   default '',
  error_message    varchar(1024) default ''
);
create index if not exists idx_ds_asset_sync on ds_asset_sync_record(project_id, asset_id);

alter table ds_dev_task add column source_table_name varchar(128) not null default '';
alter table ds_dev_task add column output_table_name varchar(128) not null default '';
alter table ds_dev_task add column result_table_name varchar(128) not null default '';
