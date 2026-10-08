-- 系统管理：租户、角色与用户分配关系落库。仅新建表并写入预置数据，不修改任何既有表；
-- 存量账号不回填分配，保持“未分配”，其登录、菜单与沙箱申请行为与升级前一致。
-- 这些表为节点本地数据，不参与 P2P 同步。

create table if not exists ds_tenant (
    id                varchar(64)  primary key,
    code              varchar(64)  not null,
    name              varchar(128) not null,
    contact           varchar(64)  not null default '',
    phone             varchar(32)  not null default '',
    status            varchar(16)  not null default 'ACTIVE',
    cpu_cores         real         not null default 16,
    memory_gb         real         not null default 64,
    gpu_count         integer      not null default 0,
    storage_gb        real         not null default 1024,
    data_isolation    integer      not null default 1,
    compute_isolation integer      not null default 1,
    created_by        varchar(128) not null default '',
    created_at        varchar(32)  not null,
    updated_at        varchar(32)  not null,
    deleted           integer      not null default 0
);
create unique index if not exists uniq_ds_tenant_code on ds_tenant(lower(code)) where deleted = 0;

create table if not exists ds_role (
    id          varchar(64)  primary key,
    name        varchar(64)  not null,
    description varchar(256) not null default '',
    permissions text         not null default '[]',
    system_role integer      not null default 0,
    created_at  varchar(32)  not null,
    updated_at  varchar(32)  not null,
    deleted     integer      not null default 0
);
create unique index if not exists uniq_ds_role_name on ds_role(lower(name)) where deleted = 0;

-- tenant_id 为空串表示仅分配了角色、未分配租户
create table if not exists ds_user_assignment (
    account    varchar(64) primary key,
    tenant_id  varchar(64) not null default '',
    updated_at varchar(32) not null
);
create index if not exists idx_ds_user_assignment_tenant on ds_user_assignment(tenant_id);

create table if not exists ds_user_role (
    account varchar(64) not null,
    role_id varchar(64) not null,
    primary key (account, role_id)
);
create index if not exists idx_ds_user_role_role on ds_user_role(role_id);

insert or ignore into ds_tenant
    (id, code, name, contact, phone, status, cpu_cores, memory_gb, gpu_count, storage_gb,
     data_isolation, compute_isolation, created_by, created_at, updated_at, deleted)
values
    ('tenant-platform', 'platform', '平台运营租户', '平台管理员', '', 'ACTIVE', 32, 128, 2, 2000,
     1, 1, 'system', strftime('%Y-%m-%d %H:%M:%S', 'now', 'localtime'),
     strftime('%Y-%m-%d %H:%M:%S', 'now', 'localtime'), 0),
    ('tenant-research', 'joint-modeling', '联合建模租户', '项目负责人', '', 'ACTIVE', 16, 64, 0, 500,
     1, 1, 'system', strftime('%Y-%m-%d %H:%M:%S', 'now', 'localtime'),
     strftime('%Y-%m-%d %H:%M:%S', 'now', 'localtime'), 0);

insert or ignore into ds_role
    (id, name, description, permissions, system_role, created_at, updated_at, deleted)
values
    ('role-admin', '沙箱管理员', '拥有全部业务与系统管理权限；不归属租户，不受租户配额与冻结约束',
     '["workbench:view","project:manage","node:manage","data:catalog","data:governance","sandbox:apply","sandbox:review","compute:use","model:review","log:view","system:user","system:role","system:tenant"]',
     1, strftime('%Y-%m-%d %H:%M:%S', 'now', 'localtime'),
     strftime('%Y-%m-%d %H:%M:%S', 'now', 'localtime'), 0),
    ('role-project-manager', '项目管理员', '管理项目、本租户用户、资源申请与项目级审批',
     '["workbench:view","project:manage","node:manage","data:catalog","data:governance","sandbox:apply","sandbox:review","compute:use","system:user"]',
     1, strftime('%Y-%m-%d %H:%M:%S', 'now', 'localtime'),
     strftime('%Y-%m-%d %H:%M:%S', 'now', 'localtime'), 0),
    ('role-developer', '数据开发人员', '使用数据目录、数据计算和沙箱能力',
     '["workbench:view","data:catalog","data:governance","sandbox:apply","compute:use"]',
     1, strftime('%Y-%m-%d %H:%M:%S', 'now', 'localtime'),
     strftime('%Y-%m-%d %H:%M:%S', 'now', 'localtime'), 0),
    ('role-auditor', '审计员', '查看审批过程、模型记录和统一审计日志',
     '["workbench:view","sandbox:review","model:review","log:view"]',
     1, strftime('%Y-%m-%d %H:%M:%S', 'now', 'localtime'),
     strftime('%Y-%m-%d %H:%M:%S', 'now', 'localtime'), 0);
