-- Unified data assets and project-scoped sandbox workflow.
alter table project add column development_modes varchar(256);

create table if not exists ds_data_asset (
 id varchar(64) primary key, name varchar(256) not null,
 provider_node_id varchar(64) not null, processor_node_id varchar(64),
 ingestion_type varchar(32) not null, modality varchar(32) not null,
 data_stage varchar(32) not null, source_asset_id varchar(64),
 datatable_id varchar(128), storage_uri varchar(1024), metadata_json text,
 sampling_method varchar(64), masking_json text, valid_from varchar(64),
 valid_until varchar(64), created_by varchar(128), created_at varchar(64),
 updated_at varchar(64), version integer default 1,
 status varchar(32) default 'ACTIVE', deleted integer default 0
);
create index if not exists idx_ds_asset_provider on ds_data_asset(provider_node_id,deleted);
create index if not exists idx_ds_asset_table on ds_data_asset(datatable_id,deleted);

create table if not exists ds_project_asset (
 project_id varchar(64) not null, asset_id varchar(64) not null,
 provider_node_id varchar(64) not null, attached_by varchar(128),
 attached_at varchar(64), expires_at varchar(64), deleted integer default 0,
 primary key(project_id,asset_id)
);
create index if not exists idx_ds_project_asset on ds_project_asset(project_id,deleted);

create table if not exists ds_asset_usage_control (
 asset_id varchar(64) primary key, valid_from varchar(64), valid_until varchar(64),
 allow_export integer default 0, access_start varchar(16), access_end varchar(16),
 version integer default 1, updated_by varchar(128), updated_at varchar(64)
);
create table if not exists ds_asset_usage_request (
 id varchar(64) primary key, asset_id varchar(64) not null,
 requester_node_id varchar(64) not null, provider_node_id varchar(64) not null,
 payload_json text, status varchar(32) not null, comment text,
 created_by varchar(128), created_at varchar(64), updated_at varchar(64),
 deleted integer default 0
);
create index if not exists idx_ds_usage_request on ds_asset_usage_request(provider_node_id,status,deleted);

create table if not exists ds_sandbox_dataset_mount (
 id varchar(64) primary key, sandbox_id varchar(64) not null,
 asset_id varchar(64) not null, asset_version integer not null,
 provider_node_id varchar(64) not null, staging_uri varchar(1024),
 mount_path varchar(512), checksum varchar(128), status varchar(32),
 expires_at varchar(64), created_at varchar(64), updated_at varchar(64),
 deleted integer default 0
);
create index if not exists idx_ds_sandbox_mount on ds_sandbox_dataset_mount(sandbox_id,deleted);

create table if not exists ds_sandbox_approval_vote (
 approval_id varchar(64) not null, voter_node_id varchar(64) not null,
 status varchar(32) not null, voter varchar(128), comment text,
 voted_at varchar(64), primary key(approval_id,voter_node_id)
);

alter table ds_sandbox_approval add column project_id varchar(64) default '';
alter table ds_sandbox_approval add column applicant_node_id varchar(64) default '';
alter table ds_sandbox_approval add column project_snapshot_at varchar(64) default '';
create index if not exists idx_ds_sandbox_apr_project on ds_sandbox_approval(project_id,status,deleted);
