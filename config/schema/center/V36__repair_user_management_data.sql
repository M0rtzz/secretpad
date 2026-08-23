-- Keep pre-existing login accounts visible and manageable after the user-management
-- columns are added by V34/V35. No account or password data is moved to another table.
update user_accounts
set display_name = name
where trim(coalesce(display_name, '')) = '';

update user_accounts
set account_status = 'ENABLED'
where upper(trim(coalesce(account_status, ''))) not in ('ENABLED', 'DISABLED');
