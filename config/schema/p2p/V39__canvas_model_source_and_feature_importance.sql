-- 画布训练产物的来源标记：开发制品列表据此隔离，画布自动注册的制品不进入数据开发视图。
alter table ds_dev_artifact add column source varchar(16) not null default 'DEV';
create index if not exists idx_da_source on ds_dev_artifact(source, deleted);
update ds_dev_artifact set source = 'CANVAS'
where name like '画布模型-%' and description like '画布节点 %训练产物%';

-- 特征重要性与树结构缓存：joblib 模型只能在执行侧解析，按需计算一次后落库供模型报告复用。
alter table ds_compute_node_run add column feature_importance varchar(16384) not null default '';
alter table ds_compute_node_run add column tree_structure varchar(65535) not null default '';
