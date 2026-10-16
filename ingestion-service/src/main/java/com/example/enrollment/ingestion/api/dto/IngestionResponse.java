package com.example.enrollment.ingestion.api.dto;

import com.example.enrollment.ingestion.service.IngestionResult;

/** Response returned by every ingestion endpoint. */
public record IngestionResponse(
        String correlationId,
        long recordsParsed,
        long recordsAccepted,
        long duplicatesSuppressed,
        long durationMillis,
        String status) {

    public static IngestionResponse from(IngestionResult result) {
        return new IngestionResponse(
                result.correlationId(),
                result.parsed(),
                result.accepted(),
                result.duplicatesSuppressed(),
                result.durationMillis(),
                "ACCEPTED");
    }
}
