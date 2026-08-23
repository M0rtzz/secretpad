-- 模型 API 供数方审批：ds_model_api 增加审批关联列（PENDING 等待供数方节点审批）。
-- 审批工作流复用 ds_sandbox_approval（approval_type='MODEL_API'）+ ds_sandbox_approval_vote
-- + ds_sandbox_approval_history + ds_sandbox_approval_sync，不新增表。
alter table ds_model_api add column approval_id varchar(64) not null default '';
create index if not exists idx_map_approval on ds_model_api(approval_id, deleted);
