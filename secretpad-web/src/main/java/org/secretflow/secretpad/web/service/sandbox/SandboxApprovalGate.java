/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 */

package org.secretflow.secretpad.web.service.sandbox;

import org.secretflow.secretpad.common.dto.UserContextDTO;
import org.secretflow.secretpad.common.errorcode.AuthErrorCode;
import org.secretflow.secretpad.common.exception.SecretpadException;
import org.secretflow.secretpad.common.util.UserContext;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Objects;

/**
 * Z-03 沙箱资源申请审批门禁：只读配置 + {@link UserContext} 静态身份判定。
 *
 * <p>无任何服务依赖，只做两类事：1) 门禁开关（approval.required，默认开启）与直接操作拦截
 * （Controller 层调用，不动 Service，避免破坏既有直调语义）；2) 审批各动作的角色判定
 * （运营方 = admin 或与申请方 owner 同节点运维账号，供数方 = 与申请方不同 ownerId 的非本人非运营方）。
 */
@Component
public class SandboxApprovalGate {

    @Value("${secretpad.data-sandbox.approval.required:true}")
    private boolean approvalRequired;

    @Value("${secretpad.node-id:kuscia-system}")
    private String nodeId;

    public boolean isApprovalRequired() {
        return approvalRequired;
    }

    /** 当前登录用户（可能为空，异步/无会话场景）。 */
    public UserContextDTO currentUser() {
        return UserContext.getUserOrNotExist();
    }

    /** 平台管理员：kuscia-system/admin。 */
    public boolean isAdmin(UserContextDTO user) {
        return user != null && "kuscia-system".equals(user.getOwnerId()) && "admin".equals(user.getName());
    }

    /** 平台管理员，或与 ownerId 同节点的运维账号（复用 DataSandboxMvpService.requireOwner 判定）。 */
    public boolean isAdminOrOperator(UserContextDTO user, String ownerId) {
        if (isAdmin(user)) {
            return true;
        }
        if (user == null) {
            return false;
        }
        return Objects.equals(user.getOwnerId(), ownerId)
                || Objects.equals(user.getPlatformNodeId(), ownerId);
    }

    /**
     * 供数方：与申请方 ownerId 不同的登录用户，非申请方本人、非运营方/管理员。
     * （单管理员实例若无第二审核人，阶段1 会卡住——由部署提供多用户解决，见 Z-03 计划风险表。）
     */
    public boolean isDataProvider(UserContextDTO user, String submitter, String approvalOwnerId) {
        if (user == null) {
            return false;
        }
        if (user.getName() != null && user.getName().equals(submitter)) {
            return false;
        }
        if (isAdminOrOperator(user, approvalOwnerId)) {
            return false;
        }
        return !Objects.equals(user.getOwnerId(), approvalOwnerId);
    }

    /** 申请人本人（登录用户名 == 提交人）。 */
    public boolean isApplicant(UserContextDTO user, String submitter) {
        return user != null && user.getName() != null && user.getName().equals(submitter);
    }

    /** 当前用户可操作的目标 owner：未登录/无 ownerId 时回退节点（与 Service.currentOwner 一致）。 */
    public String effectiveOwner() {
        UserContextDTO user = UserContext.getUserOrNotExist();
        return user == null || user.getOwnerId() == null || user.getOwnerId().isBlank()
                ? nodeId : user.getOwnerId();
    }

    /** 门禁：审批开启且当前用户非 admin/运营方时，直接创建被拒，需提交申请单。 */
    public void assertDirectCreateAllowed() {
        if (!approvalRequired) {
            return;
        }
        if (isAdminOrOperator(currentUser(), effectiveOwner())) {
            return;
        }
        throw SecretpadException.of(AuthErrorCode.AUTH_FAILED,
                "创建沙箱需提交申请单审批（GET /approvals/config 查看门禁）");
    }

    /** 门禁：审批开启且当前用户非 admin/运营方时，RENEW/DESTROY 直接操作被拒，需走申请单。 */
    public void assertDirectActionAllowed(String action) {
        if (!approvalRequired) {
            return;
        }
        if (isAdminOrOperator(currentUser(), effectiveOwner())) {
            return;
        }
        if ("RENEW".equals(action)) {
            throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "续期沙箱需提交续期申请单审批");
        }
        if ("DESTROY".equals(action)) {
            throw SecretpadException.of(AuthErrorCode.AUTH_FAILED, "回收沙箱需提交回收申请单审批");
        }
    }
}
