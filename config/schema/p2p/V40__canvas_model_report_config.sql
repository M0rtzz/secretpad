-- 工作流模型报告配置与全量评估结果。
alter table ds_compute_canvas_model add column task_type varchar(16) not null default '';
alter table ds_compute_canvas_model add column model_category varchar(16) not null default '';
alter table ds_compute_canvas_model add column report_config text not null default '{}';
alter table ds_compute_canvas_model add column evaluation_metrics text not null default '{}';
alter table ds_compute_canvas_model add column evaluation_status varchar(16) not null default '';
alter table ds_compute_canvas_model add column evaluation_error varchar(2048) not null default '';
