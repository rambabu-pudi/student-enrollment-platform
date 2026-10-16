package com.example.enrollment.ingestion.service;

/**
 * Outcome of one ingestion call. {@code accepted} counts what was put on the wire;
 * {@code duplicatesSuppressed} counts what was collapsed in-request (L1 dedup).
 */
public record IngestionResult(
        String correlationId,
        long parsed,
        long accepted,
        long duplicatesSuppressed,
        long durationMillis) {

    public double recordsPerSecond() {
        return durationMillis <= 0 ? parsed : (parsed * 1000.0) / durationMillis;
    }
}
