package com.maximus.runner.configuration.secret;

import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.Structure.FieldOrder;
import com.sun.jna.platform.win32.WinBase.FILETIME;
import com.sun.jna.ptr.PointerByReference;
import com.sun.jna.win32.StdCallLibrary;
import com.sun.jna.win32.W32APIOptions;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Persists the HMAC key in Windows Credential Manager.
 */
final class WindowsCredentialSecretStore implements SecretStore {

    static final String TARGET = "Maximus/Runner/hmac-key";
    private static final int CRED_TYPE_GENERIC = 1;
    private static final int CRED_PERSIST_LOCAL_MACHINE = 2;

    @Override
    public Optional<String> load() {
        PointerByReference credentialRef = new PointerByReference();
        boolean read = Advapi32Cred.INSTANCE.CredRead(
                TARGET,
                CRED_TYPE_GENERIC,
                0,
                credentialRef
        );
        if (!read) {
            return Optional.empty();
        }
        try {
            CREDENTIAL credential = new CREDENTIAL(credentialRef.getValue());
            if (credential.CredentialBlob == null || credential.CredentialBlobSize <= 0) {
                return Optional.empty();
            }
            byte[] bytes = credential.CredentialBlob.getByteArray(0, credential.CredentialBlobSize);
            String value = new String(bytes, StandardCharsets.UTF_8).strip();
            return value.isEmpty() ? Optional.empty() : Optional.of(value);
        } finally {
            Advapi32Cred.INSTANCE.CredFree(credentialRef.getValue());
        }
    }

    @Override
    public void save(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        Memory blob = new Memory(bytes.length);
        blob.write(0, bytes, 0, bytes.length);

        CREDENTIAL credential = new CREDENTIAL();
        credential.Type = CRED_TYPE_GENERIC;
        credential.TargetName = TARGET;
        credential.UserName = "runner";
        credential.CredentialBlobSize = bytes.length;
        credential.CredentialBlob = blob;
        credential.Persist = CRED_PERSIST_LOCAL_MACHINE;
        credential.write();

        boolean written = Advapi32Cred.INSTANCE.CredWrite(credential, 0);
        if (!written) {
            throw new IllegalStateException(
                    "No se pudo guardar la key en Windows Credential Manager. Código: "
                            + Native.getLastError()
            );
        }
    }

    private interface Advapi32Cred extends StdCallLibrary {
        Advapi32Cred INSTANCE = Native.load("advapi32", Advapi32Cred.class, W32APIOptions.UNICODE_OPTIONS);

        boolean CredWrite(CREDENTIAL credential, int flags);

        boolean CredRead(String targetName, int type, int flags, PointerByReference credential);

        boolean CredFree(Pointer credential);
    }

    @FieldOrder({
            "Flags",
            "Type",
            "TargetName",
            "Comment",
            "LastWritten",
            "CredentialBlobSize",
            "CredentialBlob",
            "Persist",
            "AttributeCount",
            "Attributes",
            "TargetAlias",
            "UserName"
    })
    public static class CREDENTIAL extends Structure {
        public int Flags;
        public int Type;
        public String TargetName;
        public String Comment;
        public FILETIME LastWritten;
        public int CredentialBlobSize;
        public Pointer CredentialBlob;
        public int Persist;
        public int AttributeCount;
        public Pointer Attributes;
        public String TargetAlias;
        public String UserName;

        public CREDENTIAL() {
        }

        public CREDENTIAL(Pointer pointer) {
            super(pointer);
            read();
        }
    }
}
