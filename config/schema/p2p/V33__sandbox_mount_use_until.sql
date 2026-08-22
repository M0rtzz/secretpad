-- Optional deadline for mounted data usage inside a sandbox.
alter table ds_sandbox_mount_control add column use_until varchar(64);
