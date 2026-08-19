/*
 * Copyright 2026 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package org.secretflow.secretpad.web.service.sandbox;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SecretCipherTest {
    @Test
    void encryptsWithRandomIvAndRoundTrips() {
        SecretCipher cipher = new SecretCipher("test-master-key");
        String first = cipher.encrypt("webhook-secret");
        String second = cipher.encrypt("webhook-secret");

        assertTrue(cipher.encrypted(first));
        assertEquals("webhook-secret", cipher.decrypt(first));
        assertNotEquals(first, second);
        assertEquals("legacy-secret", cipher.decrypt("legacy-secret"));
    }

    @Test
    void rejectsWeakMasterKey() {
        assertThrows(IllegalStateException.class, () -> new SecretCipher("short"));
    }
}
