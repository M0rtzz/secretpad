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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the pure sandbox status machine (no Spring dependency).
 */
public class SandboxStatusMachineTest {

    /* ------------------------------- canAction ------------------------------- */

    @Test
    public void startAllowedFromStoppedAndError() {
        assertTrue(SandboxStatusMachine.canAction("STOPPED", SandboxStatusMachine.Action.START));
        assertTrue(SandboxStatusMachine.canAction("ERROR", SandboxStatusMachine.Action.START));
        assertFalse(SandboxStatusMachine.canAction("RUNNING", SandboxStatusMachine.Action.START));
        assertFalse(SandboxStatusMachine.canAction("STARTING", SandboxStatusMachine.Action.START));
        assertFalse(SandboxStatusMachine.canAction("STOPPING", SandboxStatusMachine.Action.START));
        assertFalse(SandboxStatusMachine.canAction("EXPIRED", SandboxStatusMachine.Action.START));
        assertFalse(SandboxStatusMachine.canAction("DESTROYED", SandboxStatusMachine.Action.START));
    }

    @Test
    public void stopAllowedFromRunningStartingError() {
        assertTrue(SandboxStatusMachine.canAction("RUNNING", SandboxStatusMachine.Action.STOP));
        assertTrue(SandboxStatusMachine.canAction("STARTING", SandboxStatusMachine.Action.STOP));
        assertTrue(SandboxStatusMachine.canAction("ERROR", SandboxStatusMachine.Action.STOP));
        assertFalse(SandboxStatusMachine.canAction("STOPPED", SandboxStatusMachine.Action.STOP));
        assertFalse(SandboxStatusMachine.canAction("STOPPING", SandboxStatusMachine.Action.STOP));
    }

    @Test
    public void destroyAllowedFromEveryNonDestroyedState() {
        for (SandboxStatusMachine.Status status : SandboxStatusMachine.Status.values()) {
            if (status == SandboxStatusMachine.Status.DESTROYED) {
                assertFalse(SandboxStatusMachine.canAction(status.name(), SandboxStatusMachine.Action.DESTROY));
            } else {
                assertTrue(SandboxStatusMachine.canAction(status.name(), SandboxStatusMachine.Action.DESTROY));
            }
        }
    }

    @Test
    public void snapshotOnlyWhenRunningOrStopped() {
        assertTrue(SandboxStatusMachine.canAction("RUNNING", SandboxStatusMachine.Action.SNAPSHOT));
        assertTrue(SandboxStatusMachine.canAction("STOPPED", SandboxStatusMachine.Action.SNAPSHOT));
        assertFalse(SandboxStatusMachine.canAction("STARTING", SandboxStatusMachine.Action.SNAPSHOT));
        assertFalse(SandboxStatusMachine.canAction("STOPPING", SandboxStatusMachine.Action.SNAPSHOT));
        assertFalse(SandboxStatusMachine.canAction("ERROR", SandboxStatusMachine.Action.SNAPSHOT));
    }

    @Test
    public void renewAlwaysAllowed() {
        for (SandboxStatusMachine.Status status : SandboxStatusMachine.Status.values()) {
            assertTrue(SandboxStatusMachine.canAction(status.name(), SandboxStatusMachine.Action.RENEW));
        }
    }

    /* ------------------------------- mapKusciaState ------------------------------- */

    @Test
    public void startingWithStartIntentAdvancesToRunningOnKusciaRunning() {
        SandboxStatusMachine.Decision d = SandboxStatusMachine.mapKusciaState("RUNNING", "STARTING", "START");
        assertEquals("RUNNING", d.targetStatus());
        assertTrue(d.clearIntent());
        assertNull(d.lastError());
    }

    @Test
    public void startingWithStartIntentStaysStartingWhilePending() {
        SandboxStatusMachine.Decision d = SandboxStatusMachine.mapKusciaState("PENDING", "STARTING", "START");
        assertNull(d.targetStatus());
        assertNull(d.lastError());
    }

    @Test
    public void startingWithStartIntentFailsOnKusciaFailure() {
        SandboxStatusMachine.Decision d = SandboxStatusMachine.mapKusciaState("FAILED", "STARTING", "START");
        assertEquals("ERROR", d.targetStatus());
        assertTrue(d.clearIntent());
        assertTrue(d.lastError() != null && !d.lastError().isBlank());
    }

    @Test
    public void stoppingWithStopIntentAdvancesToStoppedOnKusciaTerminal() {
        for (String terminal : new String[]{"SUCCEEDED", "CANCELLED", "SUSPENDED", "FAILED", "REJECTED"}) {
            SandboxStatusMachine.Decision d = SandboxStatusMachine.mapKusciaState(terminal, "STOPPING", "STOP");
            assertEquals("STOPPED", d.targetStatus(), "state=" + terminal);
            assertTrue(d.clearIntent());
        }
    }

    @Test
    public void stoppingWithStopIntentStaysWhileKusciaStillRunning() {
        SandboxStatusMachine.Decision d = SandboxStatusMachine.mapKusciaState("RUNNING", "STOPPING", "STOP");
        assertNull(d.targetStatus());
    }

    @Test
    public void runningWithoutIntentReflectsPendingRuntimeAsStarting() {
        SandboxStatusMachine.Decision d = SandboxStatusMachine.mapKusciaState("PENDING", "RUNNING", "");
        assertEquals("STARTING", d.targetStatus());
        SandboxStatusMachine.Decision d2 = SandboxStatusMachine.mapKusciaState("PENDING", "RUNNING", "NONE");
        assertEquals("STARTING", d2.targetStatus());
    }

    @Test
    public void startingWithoutIntentRecoversWhenRuntimeBecomesRunning() {
        SandboxStatusMachine.Decision d = SandboxStatusMachine.mapKusciaState("RUNNING", "STARTING", "");
        assertEquals("RUNNING", d.targetStatus());
        assertTrue(d.clearIntent());
    }

    @Test
    public void runningWithoutIntentStaysRunningOnKusciaRunning() {
        SandboxStatusMachine.Decision d = SandboxStatusMachine.mapKusciaState("RUNNING", "RUNNING", "");
        assertEquals("RUNNING", d.targetStatus());
        assertFalse(d.clearIntent());
    }

    @Test
    public void runningWithoutIntentStopsOnKusciaSuccess() {
        SandboxStatusMachine.Decision d = SandboxStatusMachine.mapKusciaState("SUCCEEDED", "RUNNING", "");
        assertEquals("STOPPED", d.targetStatus());
        assertTrue(d.clearIntent());
    }

    @Test
    public void runningWithoutIntentFailsOnKusciaFailure() {
        SandboxStatusMachine.Decision d = SandboxStatusMachine.mapKusciaState("FAILED", "RUNNING", "");
        assertEquals("ERROR", d.targetStatus());
        assertTrue(d.lastError() != null && !d.lastError().isBlank());
    }

    @Test
    public void runningWithStopIntentIsNeverOverwritten() {
        // STOPPING 尚未落库前，用户已点停止但 sync 先到：不覆盖
        SandboxStatusMachine.Decision d = SandboxStatusMachine.mapKusciaState("RUNNING", "RUNNING", "STOP");
        assertNull(d.targetStatus());
    }

    @Test
    public void stoppedOrErrorStatesAreNeverOverwritten() {
        for (String local : new String[]{"STOPPED", "ERROR", "EXPIRED", "DESTROYED"}) {
            for (String kuscia : new String[]{"PENDING", "RUNNING", "SUCCEEDED", "FAILED"}) {
                SandboxStatusMachine.Decision d = SandboxStatusMachine.mapKusciaState(kuscia, local, "");
                assertNull(d.targetStatus(), "local=" + local + " kuscia=" + kuscia);
            }
        }
    }

    @Test
    public void unknownLocalStatusIsIgnored() {
        SandboxStatusMachine.Decision d = SandboxStatusMachine.mapKusciaState("RUNNING", "BOGUS", "");
        assertNull(d.targetStatus());
    }

    @Test
    public void isKusciaTerminal() {
        assertTrue(SandboxStatusMachine.isKusciaTerminal("SUCCEEDED"));
        assertTrue(SandboxStatusMachine.isKusciaTerminal("failed"));
        assertFalse(SandboxStatusMachine.isKusciaTerminal("RUNNING"));
        assertFalse(SandboxStatusMachine.isKusciaTerminal("PENDING"));
    }
}
