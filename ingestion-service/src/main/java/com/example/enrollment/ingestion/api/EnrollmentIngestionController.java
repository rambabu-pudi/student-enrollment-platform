package com.example.enrollment.ingestion.api;

import com.example.enrollment.common.event.KafkaTopics;
import com.example.enrollment.ingestion.api.dto.EnrollmentBatchRequest;
import com.example.enrollment.ingestion.api.dto.IngestionResponse;
import com.example.enrollment.ingestion.service.EnrollmentIngestionService;
import com.example.enrollment.ingestion.service.IngestionResult;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

/**
 * Single entry point for all three ingestion channels.
 *
 * <ul>
 *   <li>{@code POST /api/v1/enrollments} — JSON batch. This is the endpoint a Kafka-driven
 *       upstream calls.</li>
 *   <li>{@code POST /api/v1/enrollments/files} — multipart CSV or XML (or JSON) upload,
 *       streamed straight off the request without buffering the whole file.</li>
 * </ul>
 *
 * <p>Ingestion is <b>accept-and-acknowledge</b>: the endpoint returns 202 once records are on
 * the Kafka topic. Persistence happens asynchronously in the processing service. This
 * decoupling is what lets the write path absorb bursts far above the 1M/hour baseline.</p>
 */
@RestController
@RequestMapping("/api/v1/enrollments")
@Tag(name = "Enrollment Ingestion", description = "CSV, XML and JSON enrollment intake")
public class EnrollmentIngestionController {

    private final EnrollmentIngestionService ingestionService;

    public EnrollmentIngestionController(EnrollmentIngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @Operation(summary = "Ingest a JSON batch of enrollment records")
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<IngestionResponse> ingestJson(
            @Valid @RequestBody EnrollmentBatchRequest request,
            @RequestHeader(value = KafkaTopics.HEADER_CORRELATION_ID, required = false) String correlationIdHeader) {

        String correlationId = request.correlationId() != null ? request.correlationId() : correlationIdHeader;
        IngestionResult result = ingestionService.ingestBatch(request.records(), correlationId);
        return ResponseEntity.accepted().body(IngestionResponse.from(result));
    }

    @Operation(summary = "Ingest a CSV, XML or JSON file; format is detected from content type or extension")
    @PostMapping(path = "/files", consumes = MediaType.MULTIPART_FORM_DATA_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<IngestionResponse> ingestFile(
            @RequestParam("file") MultipartFile file,
            @RequestHeader(value = KafkaTopics.HEADER_CORRELATION_ID, required = false) String correlationId)
            throws IOException {

        if (file.isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        }
        try (var input = file.getInputStream()) {
            IngestionResult result = ingestionService.ingestStream(
                    input, file.getContentType(), file.getOriginalFilename(), correlationId);
            return ResponseEntity.accepted().body(IngestionResponse.from(result));
        }
    }
}
