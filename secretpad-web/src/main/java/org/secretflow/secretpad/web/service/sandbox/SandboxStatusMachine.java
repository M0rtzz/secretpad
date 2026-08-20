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
 * Pure status machine for the Data Sandbox runtime (Z-01).
 *
 * <p>Local sandbox states are intent-driven: a user action first persists an intent
 * (START / STOP), then the Kuscia status synchronizer advances the record only when the
 * real Kuscia task state confirms the intent. This prevents the MVP from marking a
 * sandbox RUNNING while its container is still pending or pulling an image.</p>
 */
public final class SandboxStatusMachine {

    private SandboxStatusMachine() {
    }

    public enum Status {
        STOPPED, STARTING, RUNNING, STOPPING, ERROR, EXPIRED, DESTROYED
    }

    public enum Intent {
        NONE, START, STOP
    }

    public enum Action {
        START, STOP, DESTROY, RENEW, SNAPSHOT
    }

    /** Result of mapping a Kuscia Job state onto a local sandbox state. */
    public record Decision(String targetStatus, boolean clearIntent, String lastError) {

        public static final Decision NO_OP = new Decision(null, false, null);

        public static Decision to(String targetStatus, boolean clearIntent) {
            return new Decision(targetStatus, clearIntent, null);
        }

        public static Decision failed(String lastError) {
            return new Decision("ERROR", true, lastError);
        }
    }

    private static final Set<String> KUSCIA_TERMINAL = Set.of(
            "SUCCEEDED", "SUSPENDED", "CANCELLED", "FAILED", "REJECTED", "FAILEDWITHISSUE");

    private static final Set<String> KUSCIA_PENDING = Set.of("PENDING", "AWAITINGAPPROVAL");

    private static final Set<String> KUSCIA_RUNNING = Set.of("RUNNING");

    /**
     * Whether the action is legal from the given local status.
     */
    public static boolean canAction(String localStatus, Action action) {
        Status from;
        try {
            from = Status.valueOf(upper(localStatus));
        } catch (IllegalArgumentException e) {
            return false;
        }
        return switch (action) {
            case START -> from == Status.STOPPED || from == Status.ERROR;
            case STOP -> from == Status.RUNNING || from == Status.STARTING || from == Status.ERROR;
            case DESTROY -> from != Status.DESTROYED;
            case RENEW -> true;
            case SNAPSHOT -> from == Status.RUNNING || from == Status.STOPPED;
        };
    }

    /**
     * Map a Kuscia Job state onto the local sandbox status, honouring the local intent.
     *
     * <p>Rules:
     * <ul>
     *   <li>STARTING + intent START: advance to RUNNING only on Kuscia RUNNING; on Kuscia
     *       terminal failure advance to ERROR; stay STARTING otherwise.</li>
     *   <li>STOPPING + intent STOP: advance to STOPPED on any Kuscia terminal state.</li>
     *   <li>RUNNING without intent: Kuscia PENDING reflects a restarting/unready runtime
     *       as STARTING; terminal success moves to STOPPED; failure moves to ERROR.</li>
     *   <li>Any other local status: never overwritten by the synchronizer.</li>
     * </ul>
     * </p>
     *
     * @param kusciaState raw Kuscia Job state (case-insensitive)
     * @param localStatus current local sandbox status
     * @param intent      current local intent (may be empty)
     * @return decision; targetStatus null means "do not touch local status"
     */
    public static Decision mapKusciaState(String kusciaState, String localStatus, String intent) {
        String state = upper(kusciaState);
        Status local;
        try {
            local = Status.valueOf(upper(localStatus));
        } catch (IllegalArgumentException e) {
            return Decision.NO_OP;
        }
        Intent intentValue = intentOf(intent);
        return switch (local) {
            case STARTING -> {
                if (intentValue == Intent.START || intentValue == Intent.NONE) {
                    if (KUSCIA_RUNNING.contains(state)) {
                        yield Decision.to("RUNNING", true);
                    }
                    if (KUSCIA_TERMINAL.contains(state)) {
                        yield Decision.failed("沙箱任务未能进入运行状态（Kuscia Job " + state + "）");
                    }
                    yield Decision.NO_OP; // PENDING 等：保持 STARTING
                }
                yield Decision.NO_OP;
            }
            case STOPPING -> {
                if (intentValue == Intent.STOP && KUSCIA_TERMINAL.contains(state)) {
                    yield Decision.to("STOPPED", true);
                }
                yield Decision.NO_OP;
            }
            case RUNNING -> {
                if (intentValue == Intent.STOP) {
                    yield Decision.NO_OP; // 停止中，等待终态
                }
                if (KUSCIA_RUNNING.contains(state)) {
                    yield Decision.to("RUNNING", false);
                }
                if (KUSCIA_PENDING.contains(state)) {
                    yield Decision.to("STARTING", false);
                }
                if (KUSCIA_TERMINAL.contains(state)) {
                    boolean failed = state.contains("FAIL") || state.contains("REJECT");
                    yield failed ? Decision.failed("Kuscia Job 运行失败: " + state)
                            : Decision.to("STOPPED", true);
                }
                yield Decision.NO_OP;
            }
            default -> Decision.NO_OP; // STOPPED / ERROR / EXPIRED / DESTROYED：不覆盖
        };
    }

    /** Whether the Kuscia Job state is terminal. */
    public static boolean isKusciaTerminal(String kusciaState) {
        return KUSCIA_TERMINAL.contains(upper(kusciaState));
    }

    private static Intent intentOf(String intent) {
        if (intent == null || intent.isBlank()) {
            return Intent.NONE;
        }
        try {
            return Intent.valueOf(upper(intent));
        } catch (IllegalArgumentException e) {
            return Intent.NONE;
        }
    }

    private static String upper(String value) {
        return value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
    }
}
