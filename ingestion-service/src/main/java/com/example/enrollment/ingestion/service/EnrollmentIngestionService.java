package com.example.enrollment.ingestion.service;

import com.example.enrollment.common.event.EnrollmentEvent;
import com.example.enrollment.common.event.EnrollmentEventFactory;
import com.example.enrollment.common.model.EnrollmentRecord;
import com.example.enrollment.common.model.SourceType;
import com.example.enrollment.ingestion.config.IngestionProperties;
import com.example.enrollment.ingestion.parser.EnrollmentPayloadParser;
import com.example.enrollment.ingestion.parser.EnrollmentParserFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Application service orchestrating parse -> deduplicate -> publish.
 *
 * <p>It is format-agnostic: the {@link EnrollmentParserFactory} hides which strategy runs,
 * so the CSV, XML and JSON paths share one identical deduplication and publication pipeline.
 * That is what guarantees {@code (S1,U1,C1)} is collapsed whether it arrives twice in one CSV
 * or once in a CSV and once over REST.</p>
 */
@Service
public class EnrollmentIngestionService {

    private static final Logger log = LoggerFactory.getLogger(EnrollmentIngestionService.class);

    private final EnrollmentParserFactory parserFactory;
    private final EnrollmentEventPublisher publisher;
    private final EnrollmentEventFactory eventFactory;
    private final IngestionProperties properties;
    private final Timer ingestionTimer;

    public EnrollmentIngestionService(EnrollmentParserFactory parserFactory,
                                      EnrollmentEventPublisher publisher,
                                      EnrollmentEventFactory eventFactory,
                                      IngestionProperties properties,
                                      MeterRegistry meterRegistry) {
        this.parserFactory = parserFactory;
        this.publisher = publisher;
        this.eventFactory = eventFactory;
        this.properties = properties;
        this.ingestionTimer = Timer.builder("enrollment.ingestion.duration")
                .description("End to end duration of one ingestion call")
                .register(meterRegistry);
    }

    /** Streams a file (CSV / XML / JSON) chosen by content type or extension. */
    public IngestionResult ingestStream(InputStream input,
                                        String contentType,
                                        String filename,
                                        String correlationId) throws IOException {
        EnrollmentPayloadParser parser = parserFactory.resolve(contentType, filename);
        return ingest(sink -> parser.parse(input, sink), parser.supportedSource(), correlationId);
    }

    /** Ingests an already-deserialised batch, used by the REST/Kafka JSON path. */
    public IngestionResult ingestBatch(Collection<EnrollmentRecord> records, String correlationId) {
        return ingest(sink -> {
            long count = 0;
            for (EnrollmentRecord record : records) {
                if (record != null && record.isComplete()) {
                    sink.accept(record);
                    count++;
                }
            }
            return count;
        }, SourceType.JSON_API, correlationId);
    }

    private IngestionResult ingest(ParseAction action, SourceType source, String correlationId) {
        String effectiveCorrelationId =
                correlationId == null || correlationId.isBlank() ? UUID.randomUUID().toString() : correlationId;

        BoundedDeduplicationFilter dedup =
                new BoundedDeduplicationFilter(properties.getDedupWindowSize());
        AtomicLong accepted = new AtomicLong();
        long startNanos = System.nanoTime();

        long parsed = action.run(record -> {
            EnrollmentEvent event = eventFactory.create(record, source, effectiveCorrelationId);
            if (dedup.firstOccurrence(event.eventId())) {
                publisher.publish(event);
                accepted.incrementAndGet();
            }
        });
        publisher.flush();

        long durationMillis = (System.nanoTime() - startNanos) / 1_000_000;
        ingestionTimer.record(java.time.Duration.ofMillis(durationMillis));

        IngestionResult result = new IngestionResult(
                effectiveCorrelationId, parsed, accepted.get(), dedup.duplicatesSuppressed(), durationMillis);

        log.info("Ingested source={} correlationId={} parsed={} accepted={} duplicatesSuppressed={} durationMs={}",
                source, effectiveCorrelationId, result.parsed(), result.accepted(),
                result.duplicatesSuppressed(), durationMillis);
        return result;
    }

    /** Adapter so both the streaming and the in-memory path share one pipeline. */
    @FunctionalInterface
    private interface ParseAction {
        long run(java.util.function.Consumer<EnrollmentRecord> sink);
    }
}
