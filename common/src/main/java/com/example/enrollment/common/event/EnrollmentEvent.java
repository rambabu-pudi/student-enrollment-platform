package com.example.enrollment.common.event;

import com.example.enrollment.common.model.EnrollmentRecord;
import com.example.enrollment.common.model.SourceType;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.UUID;

/**
 * Immutable integration event published to Kafka by the ingestion service and consumed
 * by the processing service.
 *
 * <p>Design notes:</p>
 * <ul>
 *   <li>{@code eventId} is <b>deterministic</b> — derived from the natural key via
 *       {@link com.example.enrollment.common.util.DeduplicationKeys}. Re-submitting the same
 *       fact (same file replayed, duplicate REST call, Kafka redelivery) always produces the
 *       same id, which lets the consumer implement the Idempotent Receiver pattern cheaply.</li>
 *   <li>{@code correlationId} traces one ingestion batch/request end to end.</li>
 *   <li>{@code schemaVersion} enables backward-compatible contract evolution.</li>
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record EnrollmentEvent(
        UUID eventId,
        String correlationId,
        int schemaVersion,
        SourceType source,
        Instant occurredAt,
        EnrollmentRecord payload) {

    public static final int CURRENT_SCHEMA_VERSION = 1;

    public String partitionKey() {
        return payload.partitionKey();
    }
}
