package com.example.enrollment.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.util.Objects;

/**
 * Canonical, transport-agnostic representation of one enrollment fact:
 * "student S is enrolled in class C at university U".
 *
 * <p>Every inbound format (CSV, XML, JSON/REST) is normalised into this record by the
 * ingestion parsers, so that all downstream deduplication and persistence logic operates
 * on exactly one shape. This is the Canonical Data Model pattern.</p>
 *
 * <p>The triple {@code (studentId, universityId, classCode)} is the natural key used for
 * deduplication. Values are normalised (trimmed + upper-cased) on construction so that
 * {@code "s1"}, {@code " S1 "} and {@code "S1"} collapse to the same key.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record EnrollmentRecord(
        @NotBlank @Size(max = 64) String studentId,
        @NotBlank @Size(max = 64) String universityId,
        @NotBlank @Size(max = 64) String classCode) {

    public EnrollmentRecord {
        studentId = normalise(studentId);
        universityId = normalise(universityId);
        classCode = normalise(classCode);
    }

    private static String normalise(String value) {
        return value == null ? null : value.trim().toUpperCase();
    }

    /**
     * Stable business key used for in-flight deduplication and for the Kafka message key.
     * Keying by student guarantees per-student ordering across partitions.
     */
    public String naturalKey() {
        return studentId + "|" + universityId + "|" + classCode;
    }

    /** Partition key: all facts for one student land on the same partition. */
    public String partitionKey() {
        return studentId;
    }

    public boolean isComplete() {
        return isPresent(studentId) && isPresent(universityId) && isPresent(classCode);
    }

    private static boolean isPresent(String value) {
        return Objects.nonNull(value) && !value.isBlank();
    }
}
