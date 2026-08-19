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

import java.util.Locale;

/**
 * Z-06 模型审批单状态机（纯类）。
 *
 * <p>复用现有模型审批两段审核流程（与 V6 {@code ds_model_approval} 的
 * {@code MODEL_REVIEW → RESOURCE_REVIEW → APPROVED → PUBLISHED} 一致）：
 * <ul>
 *   <li>{@code SUBMIT} 创建审批单（模型/资源两级）——由服务层落库，不在此流转；</li>
 *   <li>{@code APPROVE}：MODEL_REVIEW → RESOURCE_REVIEW（一级通过进入资源审核）；
 *       RESOURCE_REVIEW → APPROVED（二级通过，服务层在此前强制测试门禁）；</li>
 *   <li>{@code REJECT}：两级任一阶段 → REJECTED；</li>
 *   <li>{@code RESUBMIT}：REJECTED → MODEL_REVIEW，{@code version+1}（复审次数自增）；</li>
 *   <li>{@code PUBLISH}：APPROVED → PUBLISHED。</li>
 * </ul>
 * 纯类（无 Spring / I/O），每个流转可单测。</p>
 */
public final class ModelApprovalStateMachine {

    private ModelApprovalStateMachine() {
    }

    public enum Status {
        MODEL_REVIEW, RESOURCE_REVIEW, APPROVED, REJECTED, PUBLISHED
    }

    public enum Action {
        APPROVE, REJECT, RESUBMIT, PUBLISH
    }

    /** 一次流转结果：目标状态、stage、是否 version 自增。 */
    public record Transition(String to, String stage, boolean versionBump) {
    }

    /** 是否允许从 {@code from} 状态执行 {@code action}。 */
    public static boolean canTransition(String from, Action action) {
        Status status;
        try {
            status = Status.valueOf(upper(from));
        } catch (IllegalArgumentException e) {
            return false;
        }
        return switch (action) {
            case APPROVE -> status == Status.MODEL_REVIEW || status == Status.RESOURCE_REVIEW;
            case REJECT -> status == Status.MODEL_REVIEW || status == Status.RESOURCE_REVIEW;
            case RESUBMIT -> status == Status.REJECTED;
            case PUBLISH -> status == Status.APPROVED;
        };
    }

    /**
     * 应用一次流转，返回 {@link Transition}。
     *
     * @param from   当前状态
     * @param action 动作
     * @return 目标状态 / stage / versionBump
     * @throws IllegalStateException 从当前状态不允许该动作
     */
    public static Transition next(String from, Action action) {
        if (!canTransition(from, action)) {
            throw new IllegalStateException("MODEL_STATE_CONFLICT: 当前审批状态不允许操作 "
                    + action.name() + " (当前 " + upper(from) + ")");
        }
        Status status = Status.valueOf(upper(from));
        return switch (action) {
            case APPROVE -> status == Status.MODEL_REVIEW
                    ? new Transition("RESOURCE_REVIEW", "RESOURCE_REVIEW", false)
                    : new Transition("APPROVED", "COMPLETED", false);
            case REJECT -> new Transition("REJECTED", status == Status.MODEL_REVIEW
                    ? "MODEL_REVIEW" : "RESOURCE_REVIEW", false);
            case RESUBMIT -> new Transition("MODEL_REVIEW", "MODEL_REVIEW", true);
            case PUBLISH -> new Transition("PUBLISHED", "COMPLETED", false);
        };
    }

    private static String upper(String s) {
        return (s == null ? "" : s).trim().toUpperCase(Locale.ROOT);
    }
}
