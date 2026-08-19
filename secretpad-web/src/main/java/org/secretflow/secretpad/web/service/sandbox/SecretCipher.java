/*
 * Copyright 2026 Ant Group Co., Ltd.
 * Licensed under the Apache License, Version 2.0.
 */
package org.secretflow.secretpad.web.service.sandbox;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;

/** AES-GCM envelope for secrets persisted by the data sandbox extension. */
@Component
public class SecretCipher {
    private static final String PREFIX = "enc:v1:";
    private final byte[] key;
    private final SecureRandom random = new SecureRandom();

    public SecretCipher(@Value("${secretpad.data-sandbox.master-key:${SECRETPAD_PASSWORD:}}") String masterKey) {
        if (masterKey == null || masterKey.length() < 8) {
            throw new IllegalStateException("DATA_SANDBOX_MASTER_KEY or SECRETPAD_PASSWORD must contain at least 8 characters");
        }
        try {
            key = MessageDigest.getInstance("SHA-256").digest(masterKey.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new IllegalStateException("Unable to derive data sandbox encryption key", e);
        }
    }

    public String encrypt(String plaintext) {
        if (plaintext == null || plaintext.isBlank()) return "";
        if (plaintext.startsWith(PREFIX)) return plaintext;
        try {
            byte[] iv = new byte[12];
            random.nextBytes(iv);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(128, iv));
            byte[] encrypted = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));
            byte[] envelope = new byte[iv.length + encrypted.length];
            System.arraycopy(iv, 0, envelope, 0, iv.length);
            System.arraycopy(encrypted, 0, envelope, iv.length, encrypted.length);
            return PREFIX + Base64.getEncoder().encodeToString(envelope);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to encrypt secret", e);
        }
    }

    public String decrypt(String stored) {
        if (stored == null || stored.isBlank()) return "";
        if (!stored.startsWith(PREFIX)) return stored;
        try {
            byte[] envelope = Base64.getDecoder().decode(stored.substring(PREFIX.length()));
            if (envelope.length < 29) throw new IllegalArgumentException("Invalid encrypted secret");
            byte[] iv = java.util.Arrays.copyOfRange(envelope, 0, 12);
            byte[] encrypted = java.util.Arrays.copyOfRange(envelope, 12, envelope.length);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(128, iv));
            return new String(cipher.doFinal(encrypted), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to decrypt secret", e);
        }
    }

    public boolean encrypted(String value) {
        return value != null && value.startsWith(PREFIX);
    }
}
