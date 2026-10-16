package com.example.enrollment.ingestion.api.dto;

import com.example.enrollment.common.model.EnrollmentRecord;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;

import java.util.List;

/**
 * Request body for the JSON/REST channel (the one Kafka-driven callers invoke).
 *
 * <p>{@code correlationId} is optional; when the caller is a Kafka consumer it should pass
 * its own message id so the whole flow can be traced in logs.</p>
 */
public record EnrollmentBatchRequest(
        String correlationId,

        @NotEmpty(message = "records must not be empty")
        @Size(max = 10_000, message = "a single batch may not exceed 10000 records")
        @Valid List<EnrollmentRecord> records) {
}
