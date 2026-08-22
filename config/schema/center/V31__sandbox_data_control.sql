-- Sandbox mount usage and development result access controls.
create table if not exists ds_sandbox_mount_control (
  id varchar(64) primary key,
  sandbox_id varchar(64) not null,
  asset_id varchar(64) not null,
  allow_use integer not null default 1,
  version integer not null default 1,
  updated_by varchar(128),
  updated_at varchar(64),
  unique(sandbox_id, asset_id)
);
create index if not exists idx_ds_mount_control_sandbox
  on ds_sandbox_mount_control(sandbox_id);

create table if not exists ds_sandbox_result_control (
  id varchar(64) primary key,
  sandbox_id varchar(64) not null,
  table_name varchar(128) not null,
  task_id varchar(64),
  view_until varchar(64),
  allow_export integer not null default 0,
  export_until varchar(64),
  version integer not null default 1,
  updated_by varchar(128),
  updated_at varchar(64),
  unique(sandbox_id, table_name),
  unique(task_id)
);
create index if not exists idx_ds_result_control_sandbox
  on ds_sandbox_result_control(sandbox_id);

alter table ds_dev_task add column result_view_until varchar(64);
alter table ds_dev_task add column allow_result_export integer not null default 0;
alter table ds_dev_task add column result_export_until varchar(64);
