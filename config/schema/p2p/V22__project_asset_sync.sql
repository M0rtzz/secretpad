-- Synchronize project asset metadata and enrich sandbox cards.
alter table ds_project_asset add column asset_json text not null default '{}';
alter table ds_project_asset add column id integer;
alter table ds_project_asset add column is_deleted integer not null default 0;
alter table ds_project_asset add column gmt_create datetime;
alter table ds_project_asset add column gmt_modified datetime;
update ds_project_asset set is_deleted=deleted;
update ds_project_asset set gmt_create=CURRENT_TIMESTAMP,gmt_modified=CURRENT_TIMESTAMP;

alter table ds_sandbox add column description varchar(1024) default '';
