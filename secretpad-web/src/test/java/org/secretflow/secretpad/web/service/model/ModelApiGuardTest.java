/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.service.model;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the pure Z-06 model API guard (no Spring dependency).
 *
 * <p>Coverage: ENABLED/DISABLED, the validity time window (fail-closed on unparsable dates),
 * IP / CIDR whitelist matching (empty list allows all), and the authorized-user list.</p>
 */
public class ModelApiGuardTest {

    /* ------------------------------- status ------------------------------- */

    @Test
    public void enabledForEnabledStatus() {
        assertTrue(ModelApiGuard.enabled("ENABLED"));
        assertTrue(ModelApiGuard.enabled("enabled"));
        assertTrue(ModelApiGuard.enabled(" ENABLED "));
    }

    @Test
    public void disabledForDisabledStatus() {
        assertFalse(ModelApiGuard.enabled("DISABLED"));
        assertFalse(ModelApiGuard.enabled("disabled"));
        assertFalse(ModelApiGuard.enabled(" DISABLED "));
    }

    @Test
    public void unknownOrNullStatusDefaultsToEnabled() {
        assertTrue(ModelApiGuard.enabled(null));
        assertTrue(ModelApiGuard.enabled(""));
        assertTrue(ModelApiGuard.enabled("BOGUS"));
    }

    /* ------------------------------- validity window ------------------------------- */

    @Test
    public void emptyWindowAllowsEverything() {
        assertTrue(ModelApiGuard.inValidityWindow("", "", "2026-08-19 12:00:00"));
        assertTrue(ModelApiGuard.inValidityWindow(null, null, "2026-08-19 12:00:00"));
    }

    @Test
    public void nowInsideWindowIsAllowed() {
        assertTrue(ModelApiGuard.inValidityWindow(
                "2026-08-19 10:00:00", "2026-08-19 18:00:00", "2026-08-19 12:00:00"));
        assertTrue(ModelApiGuard.inValidityWindow(
                "2026-08-19 10:00:00", "", "2026-08-19 12:00:00"));
        assertTrue(ModelApiGuard.inValidityWindow(
                "", "2026-08-19 18:00:00", "2026-08-19 12:00:00"));
    }

    @Test
    public void isoTSeparatorFormatIsParsedLikeSpaceFormat() {
        // ModelApiService.now() 用 LocalDateTime.toString()（'T' 分隔）；外部配置常用空格分隔，两者都须可解析
        assertTrue(ModelApiGuard.inValidityWindow(
                "2026-08-19 10:00:00", "2026-08-19 18:00:00", "2026-08-19T12:00:00"));
        assertTrue(ModelApiGuard.inValidityWindow(
                "2026-08-19T10:00:00", "2026-08-19T18:00:00", "2026-08-19T12:00:00"));
        assertTrue(ModelApiGuard.inValidityWindow(
                "", "2026-08-19T18:00:00", "2026-08-19T12:00:00"));
        assertFalse(ModelApiGuard.inValidityWindow(
                "2026-08-19T10:00:00", "2026-08-19T18:00:00", "2026-08-19T20:00:00"));
        assertFalse(ModelApiGuard.inValidityWindow(
                "", "2026-08-19T18:00:00", "2026-08-19T20:00:00"));
    }

    @Test
    public void nowBeforeWindowStartIsDenied() {
        assertFalse(ModelApiGuard.inValidityWindow(
                "2026-08-19 10:00:00", "2026-08-19 18:00:00", "2026-08-19 08:00:00"));
        assertFalse(ModelApiGuard.inValidityWindow(
                "2026-08-19 10:00:00", "", "2026-08-19 08:00:00"));
    }

    @Test
    public void nowAfterWindowEndIsDenied() {
        assertFalse(ModelApiGuard.inValidityWindow(
                "2026-08-19 10:00:00", "2026-08-19 18:00:00", "2026-08-19 20:00:00"));
        assertFalse(ModelApiGuard.inValidityWindow(
                "", "2026-08-19 18:00:00", "2026-08-19 20:00:00"));
    }

    @Test
    public void unparsableDatesAreFailClosed() {
        assertFalse(ModelApiGuard.inValidityWindow(
                "bogus", "2026-08-19 18:00:00", "2026-08-19 12:00:00"));
        assertFalse(ModelApiGuard.inValidityWindow(
                "2026-08-19 10:00:00", "bogus", "2026-08-19 12:00:00"));
        assertFalse(ModelApiGuard.inValidityWindow(
                "2026-08-19 10:00:00", "2026-08-19 18:00:00", "bogus"));
    }

    /* ------------------------------- IP whitelist ------------------------------- */

    @Test
    public void emptyWhitelistAllowsAnyIp() {
        assertTrue(ModelApiGuard.ipAllowed("192.168.1.1", List.of()));
        assertTrue(ModelApiGuard.ipAllowed("192.168.1.1", null));
    }

    @Test
    public void exactIpMatchIsAllowed() {
        assertTrue(ModelApiGuard.ipAllowed("192.168.1.15", List.of("192.168.1.15")));
        assertTrue(ModelApiGuard.ipAllowed("192.168.1.15", List.of("10.0.0.1", "192.168.1.15")));
    }

    @Test
    public void exactIpMismatchIsDenied() {
        assertFalse(ModelApiGuard.ipAllowed("192.168.1.16", List.of("192.168.1.15")));
    }

    @Test
    public void cidrMatchIsAllowed() {
        assertTrue(ModelApiGuard.ipAllowed("192.168.1.200", List.of("192.168.1.0/24")));
        assertTrue(ModelApiGuard.ipAllowed("10.20.30.40", List.of("10.20.0.0/16", "192.168.1.0/24")));
        assertTrue(ModelApiGuard.ipAllowed("203.0.113.7", List.of("203.0.113.7/32")));
    }

    @Test
    public void cidrOutsideRangeIsDenied() {
        assertFalse(ModelApiGuard.ipAllowed("192.168.2.1", List.of("192.168.1.0/24")));
        assertFalse(ModelApiGuard.ipAllowed("10.30.0.1", List.of("10.20.0.0/16")));
    }

    @Test
    public void nullOrBlankIpWithNonEmptyWhitelistIsAllowed() {
        // 拿不到 remote host 时按放行处理（守卫由服务层决定是否获取）
        assertTrue(ModelApiGuard.ipAllowed(null, List.of("192.168.1.15")));
        assertTrue(ModelApiGuard.ipAllowed("  ", List.of("192.168.1.15")));
    }

    @Test
    public void malformedCidrIsDeniedNotThrown() {
        assertFalse(ModelApiGuard.ipAllowed("192.168.1.1", List.of("192.168.1.0/99")));
        assertFalse(ModelApiGuard.ipAllowed("192.168.1.1", List.of("not-an-ip")));
    }

    /* ------------------------------- authorized users ------------------------------- */

    @Test
    public void emptyAuthorizedUsersDeniesTokenCaller() {
        assertFalse(ModelApiGuard.userAllowed("alice", List.of()));
        assertFalse(ModelApiGuard.userAllowed("alice", null));
    }

    @Test
    public void userInListIsAllowed() {
        assertTrue(ModelApiGuard.userAllowed("alice", List.of("bob", "alice")));
        // 名单条目允许空白噪音，调用方用户名按大小写不敏感匹配
        assertTrue(ModelApiGuard.userAllowed("alice", List.of(" bob ", " ALICE ")));
    }

    @Test
    public void userNotInListIsDenied() {
        assertFalse(ModelApiGuard.userAllowed("carol", List.of("bob", "alice")));
    }
}
