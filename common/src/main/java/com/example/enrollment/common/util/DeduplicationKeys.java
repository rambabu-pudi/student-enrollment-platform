package com.example.enrollment.common.util;

import com.example.enrollment.common.model.EnrollmentRecord;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Deterministic key derivation for deduplication.
 *
 * <p>The same {@code (student, university, class)} triple always maps to the same UUID,
 * regardless of which service, which input format, or which retry produced it. This single
 * property is what makes deduplication work uniformly across all three ingestion channels
 * and across Kafka redeliveries.</p>
 *
 * <p>Implemented as an RFC-4122 name-based (version 3 / MD5) UUID over the natural key,
 * using a fixed namespace. MD5 is used purely as a name-to-UUID mapping function here,
 * not as a security primitive.</p>
 */
public final class DeduplicationKeys {

    /** Fixed namespace UUID for the enrollment domain. Must never change once in production. */
    private static final byte[] NAMESPACE = toBytes(UUID.fromString("7b1f3a2e-0c4d-4f7a-9e51-2b8d6c4a1f09"));

    private DeduplicationKeys() {
    }

    /** Deterministic event id for an enrollment fact. */
    public static UUID eventId(EnrollmentRecord record) {
        return nameUuidFromNamespace(record.naturalKey());
    }

    static UUID nameUuidFromNamespace(String name) {
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        byte[] combined = new byte[NAMESPACE.length + nameBytes.length];
        System.arraycopy(NAMESPACE, 0, combined, 0, NAMESPACE.length);
        System.arraycopy(nameBytes, 0, combined, NAMESPACE.length, nameBytes.length);
        return UUID.nameUUIDFromBytes(combined);
    }

    private static byte[] toBytes(UUID uuid) {
        byte[] out = new byte[16];
        long hi = uuid.getMostSignificantBits();
        long lo = uuid.getLeastSignificantBits();
        for (int i = 0; i < 8; i++) {
            out[i] = (byte) (hi >>> (8 * (7 - i)));
            out[8 + i] = (byte) (lo >>> (8 * (7 - i)));
        }
        return out;
    }
}
