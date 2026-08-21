-- Repair databases built before the complete resource migration chain was packaged.
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
