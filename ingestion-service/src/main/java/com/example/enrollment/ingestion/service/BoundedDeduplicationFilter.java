package com.example.enrollment.ingestion.service;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

/**
 * First-line, in-request deduplication filter.
 *
 * <p>Deduplication in this platform is layered:</p>
 * <ol>
 *   <li><b>Here (L1)</b> — collapses repeats inside a single file/request so we do not even
 *       put them on the wire. Cheap and removes the bulk of real-world duplication.</li>
 *   <li><b>Kafka key (L2)</b> — the deterministic event id lets log compaction and
 *       consumer-side idempotency work.</li>
 *   <li><b>Postgres (L3)</b> — a unique constraint on
 *       {@code (student_id, university_id, class_code)} is the authoritative guarantee
 *       across files, sources, retries and concurrent pods.</li>
 * </ol>
 *
 * <p>This L1 filter is deliberately <b>bounded</b>: a 24M-record file must not be tracked
 * entirely in heap. It keeps the most recent {@code capacity} keys in insertion order and
 * evicts the oldest. Missing a duplicate here is harmless — L3 still catches it — so a
 * bounded, allocation-light structure is the right trade-off.</p>
 *
 * <p>Not thread-safe by design: one instance is created per ingestion request.</p>
 */
public class BoundedDeduplicationFilter {

    private final int capacity;
    private final Set<UUID> seen;
    private long duplicatesSuppressed;

    public BoundedDeduplicationFilter(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.capacity = capacity;
        this.seen = new LinkedHashSet<>(Math.min(capacity, 1 << 16));
    }

    /**
     * @return {@code true} when the key has not been seen in the current window
     *         (i.e. the caller should publish it)
     */
    public boolean firstOccurrence(UUID eventId) {
        if (!seen.add(eventId)) {
            duplicatesSuppressed++;
            return false;
        }
        if (seen.size() > capacity) {
            var iterator = seen.iterator();
            iterator.next();
            iterator.remove();
        }
        return true;
    }

    public long duplicatesSuppressed() {
        return duplicatesSuppressed;
    }
}
