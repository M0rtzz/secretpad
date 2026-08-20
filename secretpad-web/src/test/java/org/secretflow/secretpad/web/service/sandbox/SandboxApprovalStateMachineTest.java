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

import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the pure Z-03 sandbox approval state machine (no Spring dependency).
 *
 * <p>Coverage: the full transition table from Z-03-plan.md Stage 0 cell-by-cell,
 * illegal transitions throwing, and invalid inputs (unknown / null / blank status).</p>
 */
public class SandboxApprovalStateMachineTest {

    private static final Map<SandboxApprovalStateMachine.Action, String> EXPECTED =
            new EnumMap<>(SandboxApprovalStateMachine.Action.class);

    static {
        // 全 null 默认：仅当 from 状态显式覆盖时才是合法流转（SUBMIT 创建新单，不流转已有记录）
        for (SandboxApprovalStateMachine.Action action : SandboxApprovalStateMachine.Action.values()) {
            EXPECTED.put(action, null);
        }
    }

    /* ------------------------------- full transition table ------------------------------- */

    /**
     * Grid (from × action) -> expected target status, or null when the transition is illegal.
     * Mirrors Z-03-plan.md Stage 0 transition table.
     */
    private static Map<SandboxApprovalStateMachine.Action, String> expected(String from) {
        Map<SandboxApprovalStateMachine.Action, String> m = new EnumMap<>(EXPECTED);
        switch (SandboxApprovalStateMachine.Status.valueOf(from)) {
            case DATA_PROVIDER_REVIEW -> {
                m.put(SandboxApprovalStateMachine.Action.APPROVE, "OPERATOR_REVIEW");
                m.put(SandboxApprovalStateMachine.Action.REJECT, "REJECTED");
                m.put(SandboxApprovalStateMachine.Action.CANCEL, "CANCELLED");
            }
            case OPERATOR_REVIEW -> {
                m.put(SandboxApprovalStateMachine.Action.APPROVE, "APPROVED");
                m.put(SandboxApprovalStateMachine.Action.REJECT, "REJECTED");
                m.put(SandboxApprovalStateMachine.Action.CANCEL, "CANCELLED");
            }
            case APPROVED -> {
                m.put(SandboxApprovalStateMachine.Action.CANCEL, "CANCELLED");
                m.put(SandboxApprovalStateMachine.Action.EXECUTE, "EXECUTING");
            }
            case EXECUTING -> {
                m.put(SandboxApprovalStateMachine.Action.COMPLETE, "COMPLETED");
                m.put(SandboxApprovalStateMachine.Action.FAIL, "FAILED");
            }
            case REJECTED -> {
                m.put(SandboxApprovalStateMachine.Action.RESUBMIT, "DATA_PROVIDER_REVIEW");
            }
            case FAILED -> {
                m.put(SandboxApprovalStateMachine.Action.RETRY, "EXECUTING");
            }
            case COMPLETED, CANCELLED -> {
                // 终态：任何动作均不允许
            }
        }
        return m;
    }

    @Test
    public void fullTransitionTableCellByCell() {
        for (SandboxApprovalStateMachine.Status from : SandboxApprovalStateMachine.Status.values()) {
            Map<SandboxApprovalStateMachine.Action, String> exp = expected(from.name());
            for (SandboxApprovalStateMachine.Action action : SandboxApprovalStateMachine.Action.values()) {
                String want = exp.get(action);
                String fromName = from.name();
                if (want == null) {
                    assertFalse(SandboxApprovalStateMachine.canTransition(fromName, action),
                            "should be illegal: " + fromName + " --" + action + "--> (any)");
                    assertThrows(IllegalStateException.class,
                            () -> SandboxApprovalStateMachine.transition(fromName, action),
                            "should throw: " + fromName + " --" + action + "-->");
                } else {
                    assertTrue(SandboxApprovalStateMachine.canTransition(fromName, action),
                            "should be legal: " + fromName + " --" + action + "--> " + want);
                    assertEquals(want, SandboxApprovalStateMachine.transition(fromName, action),
                            fromName + " --" + action + "-->");
                }
            }
        }
    }

    /* ------------------------------- targeted checks ------------------------------- */

    @Test
    public void firstStageApproveAdvancesToOperatorReview() {
        assertEquals("OPERATOR_REVIEW",
                SandboxApprovalStateMachine.transition("DATA_PROVIDER_REVIEW", SandboxApprovalStateMachine.Action.APPROVE));
    }

    @Test
    public void secondStageApproveAdvancesToApproved() {
        assertEquals("APPROVED",
                SandboxApprovalStateMachine.transition("OPERATOR_REVIEW", SandboxApprovalStateMachine.Action.APPROVE));
    }

    @Test
    public void rejectAllowedFromEitherReviewStage() {
        for (String from : new String[]{"DATA_PROVIDER_REVIEW", "OPERATOR_REVIEW"}) {
            assertEquals("REJECTED",
                    SandboxApprovalStateMachine.transition(from, SandboxApprovalStateMachine.Action.REJECT));
        }
    }

    @Test
    public void resubmitRestartsAtFirstStageWithNewReview() {
        assertEquals("DATA_PROVIDER_REVIEW",
                SandboxApprovalStateMachine.transition("REJECTED", SandboxApprovalStateMachine.Action.RESUBMIT));
    }

    @Test
    public void cancelAllowedUntilApprovedOnly() {
        for (String from : new String[]{"DATA_PROVIDER_REVIEW", "OPERATOR_REVIEW", "APPROVED"}) {
            assertEquals("CANCELLED",
                    SandboxApprovalStateMachine.transition(from, SandboxApprovalStateMachine.Action.CANCEL));
        }
        assertFalse(SandboxApprovalStateMachine.canTransition("EXECUTING", SandboxApprovalStateMachine.Action.CANCEL));
        assertFalse(SandboxApprovalStateMachine.canTransition("COMPLETED", SandboxApprovalStateMachine.Action.CANCEL));
        assertFalse(SandboxApprovalStateMachine.canTransition("FAILED", SandboxApprovalStateMachine.Action.CANCEL));
    }

    @Test
    public void executeOnlyFromApproved() {
        assertTrue(SandboxApprovalStateMachine.canTransition("APPROVED", SandboxApprovalStateMachine.Action.EXECUTE));
        assertFalse(SandboxApprovalStateMachine.canTransition("OPERATOR_REVIEW", SandboxApprovalStateMachine.Action.EXECUTE));
        assertEquals("EXECUTING",
                SandboxApprovalStateMachine.transition("APPROVED", SandboxApprovalStateMachine.Action.EXECUTE));
    }

    @Test
    public void executingCompletesOrFails() {
        assertEquals("COMPLETED",
                SandboxApprovalStateMachine.transition("EXECUTING", SandboxApprovalStateMachine.Action.COMPLETE));
        assertEquals("FAILED",
                SandboxApprovalStateMachine.transition("EXECUTING", SandboxApprovalStateMachine.Action.FAIL));
    }

    @Test
    public void retryOnlyFromFailed() {
        assertTrue(SandboxApprovalStateMachine.canTransition("FAILED", SandboxApprovalStateMachine.Action.RETRY));
        assertFalse(SandboxApprovalStateMachine.canTransition("REJECTED", SandboxApprovalStateMachine.Action.RETRY));
        assertEquals("EXECUTING",
                SandboxApprovalStateMachine.transition("FAILED", SandboxApprovalStateMachine.Action.RETRY));
    }

    @Test
    public void submitIsNeverATransition() {
        for (SandboxApprovalStateMachine.Status from : SandboxApprovalStateMachine.Status.values()) {
            assertFalse(SandboxApprovalStateMachine.canTransition(from.name(), SandboxApprovalStateMachine.Action.SUBMIT),
                    from.name() + " --SUBMIT-->");
        }
        assertThrows(IllegalStateException.class,
                () -> SandboxApprovalStateMachine.transition("DATA_PROVIDER_REVIEW", SandboxApprovalStateMachine.Action.SUBMIT));
    }

    @Test
    public void terminalStatesRejectEveryAction() {
        for (String terminal : new String[]{"COMPLETED", "CANCELLED"}) {
            for (SandboxApprovalStateMachine.Action action : SandboxApprovalStateMachine.Action.values()) {
                assertFalse(SandboxApprovalStateMachine.canTransition(terminal, action),
                        terminal + " --" + action + "--> should be illegal");
            }
        }
    }

    /* ------------------------------- review pending ------------------------------- */

    @Test
    public void reviewPendingOnlyForTheTwoReviewStages() {
        assertTrue(SandboxApprovalStateMachine.isReviewPending("DATA_PROVIDER_REVIEW"));
        assertTrue(SandboxApprovalStateMachine.isReviewPending("OPERATOR_REVIEW"));
        assertFalse(SandboxApprovalStateMachine.isReviewPending("APPROVED"));
        assertFalse(SandboxApprovalStateMachine.isReviewPending("EXECUTING"));
        assertFalse(SandboxApprovalStateMachine.isReviewPending("COMPLETED"));
        assertFalse(SandboxApprovalStateMachine.isReviewPending("REJECTED"));
        assertFalse(SandboxApprovalStateMachine.isReviewPending("FAILED"));
        assertFalse(SandboxApprovalStateMachine.isReviewPending("CANCELLED"));
    }

    /* ------------------------------- invalid inputs ------------------------------- */

    @Test
    public void unknownStatusIsRejectedNotThrown() {
        assertFalse(SandboxApprovalStateMachine.canTransition("BOGUS", SandboxApprovalStateMachine.Action.APPROVE));
        assertFalse(SandboxApprovalStateMachine.isReviewPending("BOGUS"));
        assertThrows(IllegalStateException.class,
                () -> SandboxApprovalStateMachine.transition("BOGUS", SandboxApprovalStateMachine.Action.APPROVE));
    }

    @Test
    public void nullAndBlankStatusAreRejected() {
        assertFalse(SandboxApprovalStateMachine.canTransition(null, SandboxApprovalStateMachine.Action.APPROVE));
        assertFalse(SandboxApprovalStateMachine.canTransition("", SandboxApprovalStateMachine.Action.APPROVE));
        assertFalse(SandboxApprovalStateMachine.canTransition("   ", SandboxApprovalStateMachine.Action.APPROVE));
        assertFalse(SandboxApprovalStateMachine.isReviewPending(null));
        assertFalse(SandboxApprovalStateMachine.isReviewPending(""));
        assertThrows(IllegalStateException.class,
                () -> SandboxApprovalStateMachine.transition(null, SandboxApprovalStateMachine.Action.APPROVE));
        assertThrows(IllegalStateException.class,
                () -> SandboxApprovalStateMachine.transition(null, SandboxApprovalStateMachine.Action.REJECT));
    }

    @Test
    public void lowercaseAndTrimmedStatusAreNormalized() {
        assertTrue(SandboxApprovalStateMachine.canTransition(" operator_review ", SandboxApprovalStateMachine.Action.APPROVE));
        assertEquals("APPROVED",
                SandboxApprovalStateMachine.transition(" operator_review ", SandboxApprovalStateMachine.Action.APPROVE));
        assertTrue(SandboxApprovalStateMachine.isReviewPending("data_provider_review"));
    }

    @Test
    public void illegalTransitionThrowsActionSpecificMessage() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SandboxApprovalStateMachine.transition("APPROVED", SandboxApprovalStateMachine.Action.APPROVE));
        assertTrue(e.getMessage().contains("APPROVE"), e.getMessage());
        assertTrue(e.getMessage().contains("APPROVED"), e.getMessage());
    }
}
