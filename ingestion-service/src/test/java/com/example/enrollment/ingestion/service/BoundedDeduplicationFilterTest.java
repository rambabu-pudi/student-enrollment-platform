package com.example.enrollment.ingestion.service;

import com.example.enrollment.common.model.EnrollmentRecord;
import com.example.enrollment.common.util.DeduplicationKeys;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class BoundedDeduplicationFilterTest {

    @Test
    @DisplayName("The same (S1,U1,C1) fact is only accepted once")
    void suppressesRepeatsOfTheSameNaturalKey() {
        BoundedDeduplicationFilter filter = new BoundedDeduplicationFilter(1000);
        UUID key = DeduplicationKeys.eventId(new EnrollmentRecord("S1", "U1", "C1"));

        assertThat(filter.firstOccurrence(key)).isTrue();
        assertThat(filter.firstOccurrence(key)).isFalse();
        assertThat(filter.firstOccurrence(key)).isFalse();
        assertThat(filter.duplicatesSuppressed()).isEqualTo(2);
    }

    @Test
    @DisplayName("Case and whitespace variants collapse onto one key")
    void normalisedVariantsCollapse() {
        BoundedDeduplicationFilter filter = new BoundedDeduplicationFilter(1000);

        assertThat(filter.firstOccurrence(DeduplicationKeys.eventId(
                new EnrollmentRecord("S1", "U1", "C1")))).isTrue();
        assertThat(filter.firstOccurrence(DeduplicationKeys.eventId(
                new EnrollmentRecord(" s1 ", "u1", " c1 ")))).isFalse();
    }

    @Test
    @DisplayName("Different facts are all accepted")
    void distinctFactsPass() {
        BoundedDeduplicationFilter filter = new BoundedDeduplicationFilter(1000);

        assertThat(filter.firstOccurrence(DeduplicationKeys.eventId(
                new EnrollmentRecord("S1", "U1", "C1")))).isTrue();
        assertThat(filter.firstOccurrence(DeduplicationKeys.eventId(
                new EnrollmentRecord("S1", "U1", "C2")))).isTrue();
        assertThat(filter.firstOccurrence(DeduplicationKeys.eventId(
                new EnrollmentRecord("S1", "U2", "C1")))).isTrue();
        assertThat(filter.duplicatesSuppressed()).isZero();
    }

    @Test
    @DisplayName("The window is bounded: the oldest key is evicted once capacity is exceeded")
    void evictsOldestBeyondCapacity() {
        BoundedDeduplicationFilter filter = new BoundedDeduplicationFilter(2);
        UUID first = DeduplicationKeys.eventId(new EnrollmentRecord("S1", "U1", "C1"));

        assertThat(filter.firstOccurrence(first)).isTrue();
        filter.firstOccurrence(DeduplicationKeys.eventId(new EnrollmentRecord("S2", "U1", "C1")));
        filter.firstOccurrence(DeduplicationKeys.eventId(new EnrollmentRecord("S3", "U1", "C1")));

        // "first" fell out of the window; the database unique constraint remains the backstop.
        assertThat(filter.firstOccurrence(first)).isTrue();
    }

    @Test
    @DisplayName("Event ids are deterministic across JVMs and runs")
    void eventIdIsDeterministic() {
        UUID a = DeduplicationKeys.eventId(new EnrollmentRecord("S1", "U1", "C1"));
        UUID b = DeduplicationKeys.eventId(new EnrollmentRecord("S1", "U1", "C1"));
        assertThat(a).isEqualTo(b);
        assertThat(DeduplicationKeys.eventId(new EnrollmentRecord("S1", "U1", "C2"))).isNotEqualTo(a);
    }
}
