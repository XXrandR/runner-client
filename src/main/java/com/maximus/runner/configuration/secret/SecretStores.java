package com.maximus.runner.configuration.secret;

import java.util.Locale;

public final class SecretStores {

    private SecretStores() {
    }

    public static SecretStore create() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        if (os.contains("win")) {
            return new WindowsCredentialSecretStore();
        }
        if (os.contains("linux") || os.contains("nux") || os.contains("aix")) {
            return new LinuxLibsecretStore();
        }
        throw new IllegalStateException(
                "Almacén de secretos no soportado en este sistema operativo: "
                        + System.getProperty("os.name")
        );
    }
}
