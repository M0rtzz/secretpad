-- Sandbox function (UDF) development: FUNCTION exec/artifact type + function columns.
--
-- ds_dev_task:              function columns for FUNCTION tasks. A FUNCTION task runs
--                           a backend-generated Python wrapper in the python-runner pod
--                           (conn.create_function + user SQL), reusing the PYTHON path.
-- ds_dev_artifact_version:  function columns persisted per saved FUNCTION version.
--
-- NOTE: scipy / scikit-learn whitelist seeds are deliberately NOT added here. They are
-- an optional switch that MUST stay in sync with the python-runner image (pip install);
-- the runner does not pre-install them, so a whitelist entry without a matching image
-- would pass validation but be blocked by the runtime import guard.
alter table ds_dev_task add column function_name   varchar(128)    not null default '';
alter table ds_dev_task add column function_nargs  integer         not null default 0;
alter table ds_dev_task add column function_source varchar(65535) default '';
alter table ds_dev_task add column sql_template    varchar(65535) default '';

alter table ds_dev_artifact_version add column function_name   varchar(128)    not null default '';
alter table ds_dev_artifact_version add column function_nargs  integer         not null default 0;
alter table ds_dev_artifact_version add column sql_template    varchar(65535) default '';
