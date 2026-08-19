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

import java.util.Locale;
import java.util.Set;

/**
 * Pure state machine for the Z-03 sandbox resource application / approval flow.
 *
 * <p>An application (创建/延期/规格变更/回收) goes through two review stages: 供数方审核
 * ({@code DATA_PROVIDER_REVIEW}) then 运营方审核 ({@code OPERATOR_REVIEW}). Once both approve
 * the application becomes {@code APPROVED} and an executor claims it
 * ({@code EXECUTING → COMPLETED}), with {@code REJECTED} (re-review via RESUBMIT, version+1),
 * {@code FAILED} (manual RETRY) and {@code CANCELLED} (withdrawal) branches.</p>
 *
 * <p>This class is pure (no Spring, no I/O) so every transition can be unit tested.</p>
 */
public final class SandboxApprovalStateMachine {

    private SandboxApprovalStateMachine() {
    }

    public enum Status {
        DATA_PROVIDER_REVIEW, OPERATOR_REVIEW, APPROVED, EXECUTING,
        COMPLETED, REJECTED, FAILED, CANCELLED
    }

    public enum Action {
        SUBMIT, APPROVE, REJECT, RESUBMIT, CANCEL, RETRY, EXECUTE, COMPLETE, FAIL
    }

    private static final Set<String> REVIEW_PENDING = Set.of("DATA_PROVIDER_REVIEW", "OPERATOR_REVIEW");

    /**
     * Whether the action is a legal transition from the given application status.
     */
    public static boolean canTransition(String from, Action action) {
        Status status;
        try {
            status = Status.valueOf(upper(from));
        } catch (IllegalArgumentException e) {
            return false;
        }
        return switch (action) {
            case SUBMIT -> false; // SUBMIT 创建新申请单，不是对已有记录的流转
            case APPROVE -> status == Status.DATA_PROVIDER_REVIEW || status == Status.OPERATOR_REVIEW;
            case REJECT -> status == Status.DATA_PROVIDER_REVIEW || status == Status.OPERATOR_REVIEW;
            case RESUBMIT -> status == Status.REJECTED;
            case CANCEL -> status == Status.DATA_PROVIDER_REVIEW || status == Status.OPERATOR_REVIEW
                    || status == Status.APPROVED;
            case RETRY -> status == Status.FAILED;
            case EXECUTE -> status == Status.APPROVED;
            case COMPLETE -> status == Status.EXECUTING;
            case FAIL -> status == Status.EXECUTING;
        };
    }

    /**
     * Apply an action to a status, returning the target status string.
     *
     * @throws IllegalStateException if the transition is illegal from the given status
     */
    public static String transition(String from, Action action) {
        if (!canTransition(from, action)) {
            throw new IllegalStateException("当前状态不允许操作: " + action.name() + " (当前 " + upper(from) + ")");
        }
        Status status = Status.valueOf(upper(from));
        return switch (action) {
            case APPROVE -> status == Status.DATA_PROVIDER_REVIEW ? "OPERATOR_REVIEW" : "APPROVED";
            case REJECT -> "REJECTED";
            case RESUBMIT -> "DATA_PROVIDER_REVIEW";
            case CANCEL -> "CANCELLED";
            case RETRY -> "EXECUTING";
            case EXECUTE -> "EXECUTING";
            case COMPLETE -> "COMPLETED";
            case FAIL -> "FAILED";
            case SUBMIT -> throw new IllegalStateException("SUBMIT 不是状态流转动作");
        };
    }

    /** Whether the application is awaiting a reviewer (either of the two review stages). */
    public static boolean isReviewPending(String status) {
        return REVIEW_PENDING.contains(upper(status));
    }

    private static String upper(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
    }
}
