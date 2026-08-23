alter table user_accounts add column display_name varchar(64) not null default '';
alter table user_accounts add column account_status varchar(16) not null default 'ENABLED';
alter table user_accounts add column last_login_at datetime default null;

update user_accounts
set display_name = name
where display_name = '';

create unique index if not exists uniq_active_user_accounts_name
    on user_accounts(lower(name))
    where is_deleted = 0;
