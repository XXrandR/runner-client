package com.maximus.runner.security;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;

public final class RunnerHmacSigner {

    private static final String HMAC_SHA256 = "HmacSHA256";

    private RunnerHmacSigner() {
    }

    public static String sign(String key, String credential, long timestampEpochMs) {
        try {
            Mac mac = Mac.getInstance(HMAC_SHA256);
            mac.init(new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), HMAC_SHA256));
            byte[] digest = mac.doFinal(
                    (credential + ":" + timestampEpochMs).getBytes(StandardCharsets.UTF_8)
            );
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException | InvalidKeyException exception) {
            throw new IllegalStateException("HMAC-SHA256 is not available", exception);
        }
    }
    public static byte[] signNonce(String secretKeyBase64, byte[] nonce) {
        if (secretKeyBase64 == null || secretKeyBase64.isBlank()) {
            throw new IllegalArgumentException("Handshake secret key is required");
        }
        if (nonce == null || nonce.length == 0) {
            throw new IllegalArgumentException("Handshake nonce is required");
        }

        byte[] secretKey;
        try {
            secretKey = Base64.getDecoder().decode(secretKeyBase64.trim());
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException(
                    "Handshake secret key must be valid Base64",
                    exception
            );
        }

        if (secretKey.length < 32) {
            Arrays.fill(secretKey, (byte) 0);
            throw new IllegalArgumentException(
                    "Handshake secret key must contain at least 32 bytes"
            );
        }

        try {
            Mac mac = Mac.getInstance(HMAC_SHA256);
            mac.init(new SecretKeySpec(secretKey, HMAC_SHA256));
            return mac.doFinal(nonce);
        } catch (NoSuchAlgorithmException | InvalidKeyException exception) {
            throw new IllegalStateException("HMAC-SHA256 is not available", exception);
        } finally {
            Arrays.fill(secretKey, (byte) 0);
        }
    }
}
