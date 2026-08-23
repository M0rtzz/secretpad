-- 模型 API 供数方审批单与沙箱资源申请共用 ds_sandbox_approval，沙箱审批执行引擎的轮询未按
-- approval_type 过滤，把 MODEL_API 申请单认领为 EXECUTING 并以「未知申请类型」置为 FAILED，
-- 导致临时 API 永远停在 PENDING、模型无法发布。此处按投票结果把存量申请单修回终态，
-- 修正后由 ModelApiApprovalService 的轮询完成 API 落库。

update ds_sandbox_approval
set status = 'REJECTED', current_stage = 'REJECTED', last_error = ''
where approval_type = 'MODEL_API'
  and status in ('EXECUTING', 'FAILED')
  and exists (select 1 from ds_sandbox_approval_vote v
              where v.approval_id = ds_sandbox_approval.id and v.status = 'REJECTED');

update ds_sandbox_approval
set status = 'APPROVED', current_stage = 'APPROVED', last_error = ''
where approval_type = 'MODEL_API'
  and status in ('EXECUTING', 'FAILED')
  and exists (select 1 from ds_sandbox_approval_vote v
              where v.approval_id = ds_sandbox_approval.id)
  and not exists (select 1 from ds_sandbox_approval_vote v
                  where v.approval_id = ds_sandbox_approval.id and v.status = 'PENDING');
