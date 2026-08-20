-- Project-scoped approval snapshots are synchronized to every participant.
create table if not exists ds_sandbox_approval_sync (
 approval_id varchar(64) primary key,
 project_id varchar(64) not null,
 applicant_node_id varchar(128) not null,
 snapshot_json text not null default '{}',
 id integer,
 is_deleted integer not null default 0,
 gmt_create datetime,
 gmt_modified datetime
);
create index if not exists idx_ds_sandbox_approval_sync_project
 on ds_sandbox_approval_sync(project_id, is_deleted);
