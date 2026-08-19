/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

-- Z-03 沙箱资源申请与审批：申请单主表 + 审批历史
--
-- approval_type: CREATE 创建 | RENEW 续期 | SPEC_CHANGE 规格变更 | RECYCLE 回收(销毁)
-- status:
--   DATA_PROVIDER_REVIEW 待供数方审核
--   OPERATOR_REVIEW      待运营方审核
--   APPROVED             已批准（等待执行）
--   EXECUTING            执行中（轮询器认领）
--   COMPLETED            已完成
--   REJECTED             已驳回（可 RESUBMIT 复审，version+1）
--   FAILED               执行失败（可人工 RETRY）
--   CANCELLED            已撤回
-- payload_json: 请求参数快照
--   CREATE      {name,imageId,networkPolicy,cpuCores,memoryGb,gpuCount,storageGb,validDays,projectId?,reason}
--   RENEW       {days,reason}
--   SPEC_CHANGE {cpuCores?,memoryGb?,gpuCount?,storageGb?,reason}
--   RECYCLE     {reason}
create table if not exists ds_sandbox_approval (
    id             varchar(64)  primary key,        -- 'apr-' + shortId()
    approval_type  varchar(32)  not null,
    sandbox_id     varchar(64)  not null default '', -- CREATE 执行时回填；变更类提交时填写
    owner_id       varchar(128) not null,            -- 申请方所属节点/owner
    submitter      varchar(128) not null,            -- 提交人登录名
    payload_json   varchar(4096) not null default '{}',
    status         varchar(32)  not null,
    current_stage  varchar(32)  not null,            -- 审核阶段展示，与 status 对齐
    version        integer      not null default 1,  -- RESUBMIT 复审 version+1
    executor       varchar(128) default '',          -- 执行引擎认领者
    reviewer       varchar(128) default '',          -- 最近一次审核人
    review_comment varchar(1024) default '',         -- 最近一次审核意见
    last_error     varchar(1024) default '',         -- 最近一次执行失败原因
    retry_count    integer      not null default 0,  -- 已自动重试次数（>=maxRetries 置 FAILED）
    submitted_at   varchar(32)  not null,
    approved_at    varchar(32)  default '',          -- 两级审批通过时间
    completed_at   varchar(32)  default '',          -- 执行完成/终止时间
    created_at     varchar(32)  not null,
    updated_at     varchar(32)  not null,
    deleted        integer      not null default 0
);
create index if not exists idx_ds_sandbox_apr_owner   on ds_sandbox_approval(owner_id, status, deleted);
create index if not exists idx_ds_sandbox_apr_status  on ds_sandbox_approval(status, approval_type, deleted);
create index if not exists idx_ds_sandbox_apr_sandbox on ds_sandbox_approval(sandbox_id, status, deleted);

create table if not exists ds_sandbox_approval_history (
    id          integer primary key autoincrement,
    approval_id varchar(64)  not null,
    action      varchar(32)  not null,   -- SUBMIT/APPROVE/REJECT/RESUBMIT/RETRY/CANCEL/EXECUTE/COMPLETE/FAIL
    from_status varchar(32)  default '',
    to_status   varchar(32)  not null,
    operator    varchar(128) not null,
    comment     varchar(1024) default '',
    created_at  varchar(32)  not null
);
create index if not exists idx_ds_sandbox_apr_his on ds_sandbox_approval_history(approval_id);

-- 存量迁移：无（新功能从零开始，不设回填）
