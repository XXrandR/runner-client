package com.maximus.runner.configuration.secret;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.Structure.FieldOrder;
import com.sun.jna.ptr.PointerByReference;

import java.util.List;
import java.util.Optional;

/**
 * Persists the HMAC key in the Linux Secret Service via libsecret.
 */
final class LinuxLibsecretStore implements SecretStore {

    static final String SERVICE = "com.maximus.runner";
    static final String ACCOUNT = "hmac-key";
    private static final String SCHEMA_NAME = "com.maximus.runner.hmac";
    private static final SecretSchema SCHEMA = SecretSchema.create();
    private static final LibSecret LIB = LibSecret.load();

    @Override
    public Optional<String> load() {
        PointerByReference error = new PointerByReference();
        Pointer password = LIB.secret_password_lookup_sync(
                SCHEMA,
                Pointer.NULL,
                error,
                "service",
                SERVICE,
                "username",
                ACCOUNT,
                Pointer.NULL
        );
        raiseIfError("leer", error);
        if (password == null) {
            return Optional.empty();
        }
        try {
            String value = password.getString(0);
            return value == null || value.isBlank() ? Optional.empty() : Optional.of(value);
        } finally {
            LIB.secret_password_free(password);
        }
    }

    @Override
    public void save(String value) {
        PointerByReference error = new PointerByReference();
        boolean stored = LIB.secret_password_store_sync(
                SCHEMA,
                "default",
                "Maximus Runner HMAC key",
                value,
                Pointer.NULL,
                error,
                "service",
                SERVICE,
                "username",
                ACCOUNT,
                Pointer.NULL
        );
        raiseIfError("guardar", error);
        if (!stored) {
            throw new IllegalStateException(
                    "No se pudo guardar la key en libsecret. Desbloquea el keyring de la sesión."
            );
        }
    }

    private static void raiseIfError(String action, PointerByReference error) {
        Pointer pointer = error.getValue();
        if (pointer == null) {
            return;
        }
        GError gError = new GError(pointer);
        String message = gError.message;
        throw new IllegalStateException(
                "No se pudo " + action + " la key en libsecret: " + message
        );
    }

    interface LibSecret extends Library {

        boolean secret_password_store_sync(
                SecretSchema schema,
                String collection,
                String label,
                String password,
                Pointer cancellable,
                PointerByReference error,
                Object... attributes
        );

        Pointer secret_password_lookup_sync(
                SecretSchema schema,
                Pointer cancellable,
                PointerByReference error,
                Object... attributes
        );

        void secret_password_free(Pointer password);

        static LibSecret load() {
            UnsatisfiedLinkError last = null;
            for (String name : List.of(
                    "secret-1",
                    "secret-1.so.0",
                    "/lib/x86_64-linux-gnu/libsecret-1.so.0"
            )) {
                try {
                    return Native.load(name, LibSecret.class);
                } catch (UnsatisfiedLinkError error) {
                    last = error;
                }
            }
            throw new IllegalStateException(
                    "No se pudo cargar libsecret. Instala libsecret-1.",
                    last
            );
        }
    }

    @FieldOrder({"name", "type"})
    public static class SecretSchemaAttribute extends Structure {
        public String name;
        public int type;
    }

    @FieldOrder({"name", "flags", "attributes"})
    public static class SecretSchema extends Structure {
        public String name;
        public int flags;
        public SecretSchemaAttribute[] attributes =
                (SecretSchemaAttribute[]) new SecretSchemaAttribute().toArray(32);

        static SecretSchema create() {
            SecretSchema schema = new SecretSchema();
            schema.name = SCHEMA_NAME;
            schema.flags = 0;
            schema.attributes[0].name = "service";
            schema.attributes[0].type = 0;
            schema.attributes[1].name = "username";
            schema.attributes[1].type = 0;
            schema.write();
            return schema;
        }
    }

    @FieldOrder({"domain", "code", "message"})
    public static class GError extends Structure {
        public int domain;
        public int code;
        public String message;

        public GError(Pointer pointer) {
            super(pointer);
            read();
        }
    }
}
