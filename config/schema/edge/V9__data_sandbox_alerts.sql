/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

-- Z-02 告警与通知：ds_alert_event 增加去重键，支持按 (source, dedupe_key) 对 OPEN 告警去重，
-- 避免 NODE_METRIC/RESOURCE/SANDBOX 高频告警（每 30s / 每分钟）刷屏。
alter table ds_alert_event add column dedupe_key varchar(128) default '';
