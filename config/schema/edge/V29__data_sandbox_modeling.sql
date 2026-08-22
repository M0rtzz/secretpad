-- Sandbox intelligent modeling (智能建模与服务化闭环):
--   visual modeling canvas execution + operator component library + model/artifact API publish.
--
-- ds_compute_run:            one row per canvas execution (整图运行 / 断点继续).
-- ds_compute_node_run:       per-node run record; status drives X6 canvas node coloring;
--                            task_id references the ds_dev_task (channel='canvas') that ran the node.
-- ds_compute_canvas_version: version snapshot on every canvas save (回滚/对比 support).
-- ds_compute_template:       preset business pipelines (银行信用风控二分类 / 客户流失K-Means /
--                            收入预测线性回归), seeded idempotently from backend constants.

create table if not exists ds_compute_run (
    id           varchar(64)  primary key,               -- 'cr-' + shortId()
    canvas_id    varchar(64)  not null,
    sandbox_id   varchar(64)  not null,
    status       varchar(16)  not null default 'PENDING', -- PENDING/RUNNING/SUCCEEDED/FAILED/CANCELLED
    mode         varchar(16)  not null default 'ALL',     -- ALL (整图) / SINGLE (单节点) / DOWN (单步向下) / CONTINUE (断点继续)
    node_ids     varchar(4096) default '[]',              -- JSON 数组：本次运行涉及的节点 id
    started_by   varchar(128) not null default '',
    started_at   varchar(32)  default '',
    finished_at  varchar(32)  default '',
    error_message varchar(2048) default '',
    created_at   varchar(32)  not null,
    updated_at   varchar(32)  not null,
    deleted      integer      not null default 0
);
create index if not exists idx_compute_run_canvas on ds_compute_run(canvas_id, created_at, deleted);

create table if not exists ds_compute_node_run (
    id              varchar(64)  primary key,             -- 'nr-' + shortId()
    run_id          varchar(64)  not null,
    canvas_id       varchar(64)  not null,
    sandbox_id      varchar(64)  not null,
    node_id         varchar(128) not null,                -- 画布节点 id（对应 graph_json 节点）
    component_code  varchar(128) not null,                -- 算子 code，如 ml.logistic_regression
    status          varchar(16)  not null default 'PENDING', -- PENDING/RUNNING/SUCCEEDED/FAILED
    task_id         varchar(64)  default '',              -- 关联 ds_dev_task（channel='canvas'）
    input_table     varchar(128) default '',              -- 节点输入表（上游 op_* 或挂载 asset_*）
    output_table    varchar(128) default '',              -- 节点输出表（op_{canvasId}_{nodeId}）
    result_summary  varchar(4096) default '{}',           -- {header,rowCount,columnCount}
    model_b64       text         default '',              -- 训练节点产出的 joblib(base64)（用于自动注册模型）
    error_message   varchar(2048) default '',
    started_at      varchar(32)  default '',
    finished_at     varchar(32)  default '',
    created_at      varchar(32)  not null,
    updated_at      varchar(32)  not null,
    deleted         integer      not null default 0
);
create index if not exists idx_compute_node_run on ds_compute_node_run(run_id, node_id, deleted);

create table if not exists ds_compute_canvas_version (
    id         varchar(64)  primary key,                  -- 'cv-' + shortId()
    canvas_id  varchar(64)  not null,
    version    integer      not null,
    name       varchar(128) not null,
    graph_json text         not null default '{}',
    created_by varchar(128) not null,
    created_at varchar(32)  not null,
    deleted    integer      not null default 0
);
create index if not exists idx_compute_canvas_version on ds_compute_canvas_version(canvas_id, version, deleted);

create table if not exists ds_compute_template (
    id          varchar(64)  primary key,                 -- 'tp-' + shortId()
    code        varchar(128) not null unique,             -- credit_risk / churn_kmeans / income_regression
    name        varchar(128) not null,
    category    varchar(64)  not null default '内置模板',
    description varchar(512) default '',
    graph_json  text         not null default '{}',
    created_at  varchar(32)  not null,
    updated_at  varchar(32)  not null,
    deleted     integer      not null default 0
);

-- 智能建模依赖白名单：predict 脚本（joblib 加载模型）与画布算子 modeling_ops 依赖的三方库，
-- v2-ml 镜像已预装；白名单供平台侧 validatePython 预检（import 守卫权威校验在 runner 内）。
insert or ignore into ds_dev_dependency(id,name,version_spec,description,enabled,created_by,created_at,updated_at,deleted)
values('dep-joblib','joblib','>=1.3','Joblib 模型持久化（predict 脚本依赖）',1,'system','2026-08-22 00:00:00','2026-08-22 00:00:00',0);
insert or ignore into ds_dev_dependency(id,name,version_spec,description,enabled,created_by,created_at,updated_at,deleted)
values('dep-scipy','scipy','>=1.10','SciPy 科学计算（模型算子依赖）',1,'system','2026-08-22 00:00:00','2026-08-22 00:00:00',0);
insert or ignore into ds_dev_dependency(id,name,version_spec,description,enabled,created_by,created_at,updated_at,deleted)
values('dep-sklearn','scikit-learn','>=1.3','Scikit-learn 机器学习算子',1,'system','2026-08-22 00:00:00','2026-08-22 00:00:00',0);
insert or ignore into ds_dev_dependency(id,name,version_spec,description,enabled,created_by,created_at,updated_at,deleted)
values('dep-xgboost','xgboost','>=2.0','XGBoost 梯度提升树',1,'system','2026-08-22 00:00:00','2026-08-22 00:00:00',0);
insert or ignore into ds_dev_dependency(id,name,version_spec,description,enabled,created_by,created_at,updated_at,deleted)
values('dep-lightgbm','lightgbm','>=4.0','LightGBM 梯度提升树',1,'system','2026-08-22 00:00:00','2026-08-22 00:00:00',0);
