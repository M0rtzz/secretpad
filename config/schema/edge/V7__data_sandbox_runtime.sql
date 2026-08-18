/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

-- Z-01 真实沙箱运行时：意图保护、运行时状态与开发端点

-- 本地意图态（'' | START | STOP）：状态同步任务只在意图方向一致时推进本地状态，
-- 防止“未创建容器却标记为运行中”以及同步任务无条件覆盖本地状态
alter table ds_sandbox add column intent varchar(16) not null default '';

-- 最近一次 queryJob 返回的 Kuscia Job 原始 state（排障用）
alter table ds_sandbox add column kuscia_job_state varchar(32) not null default '';

-- 运行时摘要（JSON：task state、容器/存储摘要等）
alter table ds_sandbox add column runtime_meta varchar(2048) not null default '';

-- 开发端点一次性访问 token（DB 中存 sha256）与过期时间
alter table ds_sandbox add column endpoint_token varchar(128) not null default '';
alter table ds_sandbox add column endpoint_token_expires_at varchar(32) not null default '';
alter table ds_sandbox add column endpoint_updated_at varchar(32) not null default '';

-- 镜像的开发端口名：从 Kuscia Job party endpoints 中提取 endpoint 时按端口名匹配
-- （scope=Cluster 的端口由 Kuscia 分配集群外可达地址），默认 web，JAR 镜像为 app
alter table ds_sandbox_image add column dev_port_name varchar(16) not null default 'web';

-- 种子镜像关联到真实注册的 AppImage（对应 scripts/templates/data-sandbox-*.yaml）
update ds_sandbox_image set kuscia_app_image='data-sandbox-jupyter', dev_port_name='web' where id='img-jupyter-scipy';
update ds_sandbox_image set kuscia_app_image='data-sandbox-secretflow', dev_port_name='web' where id='img-secretflow';

-- JAR 运行环境镜像（AppImage: data-sandbox-jar，开发端口名 app）
insert or ignore into ds_sandbox_image(id, name, image_ref, kuscia_app_image, dev_port_name, description, enabled, created_by, created_at, updated_at)
values ('img-jar', 'Java Runtime (JAR)', 'eclipse-temurin:17-jre', 'data-sandbox-jar', 'app', '通用 JAR 运行环境', 1, 'system', datetime('now'), datetime('now'));
