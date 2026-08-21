alter table ds_governance_policy add column source_asset_id varchar(64) default '';
alter table ds_governance_policy add column source_node_id varchar(64) default '';
alter table ds_governance_policy add column source_datatable_id varchar(64) default '';

create index if not exists idx_gp_source
    on ds_governance_policy(source_node_id, source_datatable_id, deleted);
