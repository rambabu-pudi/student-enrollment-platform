package com.example.enrollment.common.event;

import com.example.enrollment.common.model.EnrollmentRecord;
import com.example.enrollment.common.model.SourceType;
import com.example.enrollment.common.util.DeduplicationKeys;

import java.time.Clock;
import java.time.Instant;

/**
 * Factory for {@link EnrollmentEvent} instances (Factory Method pattern).
 * Centralises event-id derivation so no caller can accidentally create a non-deterministic id.
 */
public final class EnrollmentEventFactory {

    private final Clock clock;

    public EnrollmentEventFactory() {
        this(Clock.systemUTC());
    }

    public EnrollmentEventFactory(Clock clock) {
        this.clock = clock;
    }

    public EnrollmentEvent create(EnrollmentRecord record, SourceType source, String correlationId) {
        return new EnrollmentEvent(
                DeduplicationKeys.eventId(record),
                correlationId,
                EnrollmentEvent.CURRENT_SCHEMA_VERSION,
                source,
                Instant.now(clock),
                record);
    }
}
