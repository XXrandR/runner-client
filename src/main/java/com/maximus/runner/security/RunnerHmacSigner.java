package com.maximus.runner.security;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;

public final class RunnerHmacSigner {

    private static final String HMAC_SHA256 = "HmacSHA256";

    private RunnerHmacSigner() {
    }

    /** Fail before persisting a malformed or too-short shared key. */
    public static void validateSecretKey(String secretKeyBase64) {
        byte[] decoded = decodeKey(secretKeyBase64);
        Arrays.fill(decoded, (byte) 0);
    }

    /**
     * Calcula la prueba del handshake utilizando exactamente los bytes
     * del nonce recibido desde el servidor.
     */
    public static byte[] signNonce(
            String secretKeyBase64,
            byte[] nonce
    ) {
        if (nonce == null || nonce.length == 0) {
            throw new IllegalArgumentException(
                    "Handshake nonce is required"
            );
        }

        byte[] secretKey = decodeKey(secretKeyBase64);

        try {
            Mac mac = Mac.getInstance(HMAC_SHA256);

            mac.init(
                    new SecretKeySpec(secretKey, HMAC_SHA256)
            );

            return mac.doFinal(nonce);

        } catch (
                NoSuchAlgorithmException |
                InvalidKeyException exception
        ) {
            throw new IllegalStateException(
                    "HMAC-SHA256 is not available",
                    exception
            );
        } finally {
            Arrays.fill(secretKey, (byte) 0);
        }
    }

    private static byte[] decodeKey(String encoded) {
        if (encoded == null || encoded.isBlank()) {
            throw new IllegalArgumentException("Handshake secret key is required");
        }
        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(encoded.trim());
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("Handshake secret key must be valid Base64", exception);
        }
        if (decoded.length < 32) {
            Arrays.fill(decoded, (byte) 0);
            throw new IllegalArgumentException("Handshake secret key must contain at least 32 bytes");
        }
        return decoded;
    }
}
