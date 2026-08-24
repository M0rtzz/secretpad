-- Archived projects are no longer valid consumers of data assets. Historical archive
-- flows removed project participants but left active data relations behind, which
-- caused later asset deletion to enter an approval flow that could never be completed.
update ds_project_asset
set deleted = 1, is_deleted = 1, gmt_modified = CURRENT_TIMESTAMP
where deleted = 0
  and coalesce(is_deleted, 0) = 0
  and exists (select 1 from project p
              where p.project_id = ds_project_asset.project_id
                and (p.status = 2 or p.is_deleted = 1));

update project_datatable
set is_deleted = 1, gmt_modified = CURRENT_TIMESTAMP
where is_deleted = 0
  and exists (select 1 from project p
              where p.project_id = project_datatable.project_id
                and (p.status = 2 or p.is_deleted = 1));

-- Archived-project approval rows can no longer receive votes. Mark them completed so
-- they preserve their audit trail without blocking a fresh deletion request.
update ds_sandbox_approval
set status = 'COMPLETED', current_stage = 'COMPLETED',
    completed_at = coalesce(nullif(completed_at, ''), CURRENT_TIMESTAMP),
    last_error = 'Archived project relation repaired by V42',
    updated_at = CURRENT_TIMESTAMP
where approval_type = 'ASSET_DELETE'
  and status in ('DATA_PROVIDER_REVIEW', 'APPROVED', 'EXECUTING', 'FAILED')
  and exists (select 1 from project p
              where p.project_id = ds_sandbox_approval.project_id
                and (p.status = 2 or p.is_deleted = 1));
