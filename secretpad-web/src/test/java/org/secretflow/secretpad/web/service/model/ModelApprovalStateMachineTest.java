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

import java.util.EnumMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the pure Z-06 model approval state machine (no Spring dependency).
 *
 * <p>Coverage: the two-stage approval flow MODEL_REVIEW → RESOURCE_REVIEW → APPROVED,
 * REJECT/RESUBMIT (with version bump), PUBLISH, illegal transitions, and terminal states.</p>
 */
public class ModelApprovalStateMachineTest {

    /* ------------------------------- full transition table ------------------------------- */

    /** Grid (from × action) → target status + stage + versionBump, or null when illegal. */
    private static Map<ModelApprovalStateMachine.Action, ModelApprovalStateMachine.Transition> expected(String from) {
        Map<ModelApprovalStateMachine.Action, ModelApprovalStateMachine.Transition> m =
                new EnumMap<>(ModelApprovalStateMachine.Action.class);
        switch (ModelApprovalStateMachine.Status.valueOf(from)) {
            case MODEL_REVIEW -> {
                m.put(ModelApprovalStateMachine.Action.APPROVE,
                        new ModelApprovalStateMachine.Transition("RESOURCE_REVIEW", "RESOURCE_REVIEW", false));
                m.put(ModelApprovalStateMachine.Action.REJECT,
                        new ModelApprovalStateMachine.Transition("REJECTED", "MODEL_REVIEW", false));
            }
            case RESOURCE_REVIEW -> {
                m.put(ModelApprovalStateMachine.Action.APPROVE,
                        new ModelApprovalStateMachine.Transition("APPROVED", "COMPLETED", false));
                m.put(ModelApprovalStateMachine.Action.REJECT,
                        new ModelApprovalStateMachine.Transition("REJECTED", "RESOURCE_REVIEW", false));
            }
            case APPROVED -> m.put(ModelApprovalStateMachine.Action.PUBLISH,
                    new ModelApprovalStateMachine.Transition("PUBLISHED", "COMPLETED", false));
            case REJECTED -> m.put(ModelApprovalStateMachine.Action.RESUBMIT,
                    new ModelApprovalStateMachine.Transition("MODEL_REVIEW", "MODEL_REVIEW", true));
            case PUBLISHED -> {
                // 终态：任何动作均不允许
            }
        }
        return m;
    }

    @Test
    public void fullTransitionTableCellByCell() {
        for (ModelApprovalStateMachine.Status from : ModelApprovalStateMachine.Status.values()) {
            Map<ModelApprovalStateMachine.Action, ModelApprovalStateMachine.Transition> exp = expected(from.name());
            for (ModelApprovalStateMachine.Action action : ModelApprovalStateMachine.Action.values()) {
                ModelApprovalStateMachine.Transition want = exp.get(action);
                String fromName = from.name();
                if (want == null) {
                    assertFalse(ModelApprovalStateMachine.canTransition(fromName, action),
                            "should be illegal: " + fromName + " --" + action + "--> (any)");
                    assertThrows(IllegalStateException.class,
                            () -> ModelApprovalStateMachine.next(fromName, action),
                            "should throw: " + fromName + " --" + action + "-->");
                } else {
                    assertTrue(ModelApprovalStateMachine.canTransition(fromName, action),
                            "should be legal: " + fromName + " --" + action + "--> " + want.to());
                    ModelApprovalStateMachine.Transition t = ModelApprovalStateMachine.next(fromName, action);
                    assertEquals(want.to(), t.to(), fromName + " --" + action + "--> to");
                    assertEquals(want.stage(), t.stage(), fromName + " --" + action + "--> stage");
                    assertEquals(want.versionBump(), t.versionBump(), fromName + " --" + action + "--> versionBump");
                }
            }
        }
    }

    /* ------------------------------- targeted checks ------------------------------- */

    @Test
    public void firstStageApproveAdvancesToResourceReview() {
        ModelApprovalStateMachine.Transition t =
                ModelApprovalStateMachine.next("MODEL_REVIEW", ModelApprovalStateMachine.Action.APPROVE);
        assertEquals("RESOURCE_REVIEW", t.to());
        assertEquals("RESOURCE_REVIEW", t.stage());
        assertFalse(t.versionBump());
    }

    @Test
    public void secondStageApproveCompletes() {
        ModelApprovalStateMachine.Transition t =
                ModelApprovalStateMachine.next("RESOURCE_REVIEW", ModelApprovalStateMachine.Action.APPROVE);
        assertEquals("APPROVED", t.to());
        assertEquals("COMPLETED", t.stage());
        assertFalse(t.versionBump());
    }

    @Test
    public void rejectAllowedFromEitherReviewStage() {
        assertEquals("MODEL_REVIEW", ModelApprovalStateMachine.next(
                "MODEL_REVIEW", ModelApprovalStateMachine.Action.REJECT).stage());
        assertEquals("RESOURCE_REVIEW", ModelApprovalStateMachine.next(
                "RESOURCE_REVIEW", ModelApprovalStateMachine.Action.REJECT).stage());
        for (String from : new String[]{"MODEL_REVIEW", "RESOURCE_REVIEW"}) {
            assertEquals("REJECTED", ModelApprovalStateMachine.next(
                    from, ModelApprovalStateMachine.Action.REJECT).to());
        }
    }

    @Test
    public void resubmitRestartsAtModelReviewWithVersionBump() {
        ModelApprovalStateMachine.Transition t =
                ModelApprovalStateMachine.next("REJECTED", ModelApprovalStateMachine.Action.RESUBMIT);
        assertEquals("MODEL_REVIEW", t.to());
        assertEquals("MODEL_REVIEW", t.stage());
        assertTrue(t.versionBump());
    }

    @Test
    public void publishOnlyFromApproved() {
        assertTrue(ModelApprovalStateMachine.canTransition("APPROVED", ModelApprovalStateMachine.Action.PUBLISH));
        assertFalse(ModelApprovalStateMachine.canTransition("RESOURCE_REVIEW", ModelApprovalStateMachine.Action.PUBLISH));
        ModelApprovalStateMachine.Transition t =
                ModelApprovalStateMachine.next("APPROVED", ModelApprovalStateMachine.Action.PUBLISH);
        assertEquals("PUBLISHED", t.to());
        assertFalse(t.versionBump());
    }

    @Test
    public void approvedAndPublishedDoNotAllowReviewActions() {
        for (String terminal : new String[]{"APPROVED", "PUBLISHED"}) {
            assertFalse(ModelApprovalStateMachine.canTransition(terminal, ModelApprovalStateMachine.Action.APPROVE));
            assertFalse(ModelApprovalStateMachine.canTransition(terminal, ModelApprovalStateMachine.Action.REJECT));
            assertFalse(ModelApprovalStateMachine.canTransition(terminal, ModelApprovalStateMachine.Action.RESUBMIT));
        }
    }

    /* ------------------------------- invalid inputs ------------------------------- */

    @Test
    public void unknownStatusIsRejectedNotThrown() {
        assertFalse(ModelApprovalStateMachine.canTransition("BOGUS", ModelApprovalStateMachine.Action.APPROVE));
        assertThrows(IllegalStateException.class,
                () -> ModelApprovalStateMachine.next("BOGUS", ModelApprovalStateMachine.Action.APPROVE));
    }

    @Test
    public void nullAndBlankStatusAreRejected() {
        assertFalse(ModelApprovalStateMachine.canTransition(null, ModelApprovalStateMachine.Action.APPROVE));
        assertFalse(ModelApprovalStateMachine.canTransition("", ModelApprovalStateMachine.Action.APPROVE));
        assertFalse(ModelApprovalStateMachine.canTransition("   ", ModelApprovalStateMachine.Action.APPROVE));
        assertThrows(IllegalStateException.class,
                () -> ModelApprovalStateMachine.next(null, ModelApprovalStateMachine.Action.APPROVE));
    }

    @Test
    public void lowercaseAndTrimmedStatusAreNormalized() {
        assertTrue(ModelApprovalStateMachine.canTransition(" resource_review ", ModelApprovalStateMachine.Action.APPROVE));
        assertEquals("APPROVED", ModelApprovalStateMachine.next(
                " resource_review ", ModelApprovalStateMachine.Action.APPROVE).to());
        assertTrue(ModelApprovalStateMachine.canTransition("rejected", ModelApprovalStateMachine.Action.RESUBMIT));
    }

    @Test
    public void illegalTransitionThrowsActionAndStateSpecificMessage() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> ModelApprovalStateMachine.next("APPROVED", ModelApprovalStateMachine.Action.APPROVE));
        assertTrue(e.getMessage().contains("APPROVE"), e.getMessage());
        assertTrue(e.getMessage().contains("APPROVED"), e.getMessage());
        assertTrue(e.getMessage().contains("MODEL_STATE_CONFLICT"), e.getMessage());
    }
}
