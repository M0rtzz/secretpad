-- 可视化建模工作流模型：保存画布的不可变拓扑快照，并关联可执行模型。
alter table ds_compute_node_run add column model_id varchar(64) not null default '';

create table if not exists ds_compute_canvas_model (
    id             varchar(64)  primary key,
    canvas_id      varchar(64)  not null,
    canvas_version integer      not null,
    model_id       varchar(64)  not null default '',
    source_node_id varchar(128) not null default '',
    name           varchar(128) not null,
    description    varchar(512) default '',
    graph_json     text         not null default '{}',
    status         varchar(16)  not null default 'DRAFT', -- DRAFT/READY
    created_by     varchar(128) not null,
    created_at     varchar(32)  not null,
    updated_at     varchar(32)  not null,
    deleted        integer      not null default 0
);
create index if not exists idx_canvas_model_canvas on ds_compute_canvas_model(canvas_id, created_at, deleted);
create index if not exists idx_canvas_model_model on ds_compute_canvas_model(model_id, deleted);
