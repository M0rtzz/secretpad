-- Z-08 安全、备份恢复与运维增强
alter table ds_api_client add column secret_version integer not null default 1;
alter table ds_api_client add column rotated_at varchar(32) default '';
alter table ds_api_client add column expires_at varchar(32) default '';
alter table ds_backup add column scope_json varchar(2048) default '{}';
alter table ds_backup add column manifest_path varchar(1024) default '';
alter table ds_backup add column verified_at varchar(32) default '';
alter table ds_backup add column drill_status varchar(32) default 'NOT_RUN';
alter table ds_backup add column drill_report varchar(2048) default '';

create table if not exists ds_oidc_role_mapping (
    id varchar(64) primary key,
    claim_name varchar(128) not null default 'groups',
    claim_value varchar(256) not null,
    platform_role varchar(64) not null,
    owner_id varchar(128) default '',
    enabled integer not null default 1,
    created_by varchar(128) default '',
    created_at varchar(32) not null,
    updated_at varchar(32) not null
);
create unique index if not exists uk_ds_oidc_mapping on ds_oidc_role_mapping(claim_name, claim_value, owner_id);

create table if not exists ds_recovery_point (
    id varchar(64) primary key,
    backup_id varchar(64) not null,
    point_type varchar(32) not null default 'MANUAL',
    status varchar(32) not null default 'AVAILABLE',
    artifact_path varchar(1024) not null,
    checksum varchar(128) not null,
    verified_at varchar(32) default '',
    drill_status varchar(32) default 'NOT_RUN',
    drill_report varchar(2048) default '',
    created_by varchar(128) default '',
    created_at varchar(32) not null
);
create index if not exists idx_ds_recovery_status on ds_recovery_point(status, created_at);

create table if not exists ds_security_scan (
    id varchar(64) primary key,
    status varchar(32) not null,
    critical_count integer not null default 0,
    warning_count integer not null default 0,
    report_json varchar(8192) not null default '{}',
    created_by varchar(128) default '',
    created_at varchar(32) not null
);
