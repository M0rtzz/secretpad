-- 可视化建模工作流模型：记录工作流输入/输出数据（数据资源挂载表 + 列 / 终态输出表 + 列），
-- 供保存后发布 API 时确定必要的输入输出 schema。
alter table ds_compute_canvas_model add column input_table varchar(128) not null default '';
alter table ds_compute_canvas_model add column input_columns text not null default '';
alter table ds_compute_canvas_model add column output_table varchar(128) not null default '';
alter table ds_compute_canvas_model add column output_columns text not null default '';
