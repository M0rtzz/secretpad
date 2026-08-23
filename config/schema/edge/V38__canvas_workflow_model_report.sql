-- 工作流模型报告必须绑定产生模型的确切运行和任务，避免重复运行后读取到其他批次的数据。
alter table ds_compute_canvas_model add column source_run_id varchar(64) not null default '';
alter table ds_compute_canvas_model add column source_task_id varchar(64) not null default '';

create index if not exists idx_canvas_model_source_run
    on ds_compute_canvas_model(source_run_id, deleted);
