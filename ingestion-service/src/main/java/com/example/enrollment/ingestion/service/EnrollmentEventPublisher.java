package com.example.enrollment.ingestion.service;

import com.example.enrollment.common.event.EnrollmentEvent;
import com.example.enrollment.common.event.KafkaTopics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Publishes canonical events to Kafka.
 *
 * <p>Throughput notes: {@code KafkaTemplate#send} is asynchronous and the producer batches
 * internally (see {@code linger.ms}/{@code batch.size} in application.yml). We therefore fire
 * records without blocking per message and only reconcile failures via the callback. Blocking
 * on each send would cap throughput at roughly one record per network round trip.</p>
 */
@Component
public class EnrollmentEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EnrollmentEventPublisher.class);

    private final KafkaTemplate<String, EnrollmentEvent> kafkaTemplate;
    private final Counter publishedCounter;
    private final Counter failedCounter;

    public EnrollmentEventPublisher(KafkaTemplate<String, EnrollmentEvent> kafkaTemplate,
                                    MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.publishedCounter = Counter.builder("enrollment.ingestion.published")
                .description("Events successfully published to Kafka")
                .register(meterRegistry);
        this.failedCounter = Counter.builder("enrollment.ingestion.publish.failed")
                .description("Events that could not be published to Kafka")
                .register(meterRegistry);
    }

    public void publish(EnrollmentEvent event) {
        ProducerRecord<String, EnrollmentEvent> record = new ProducerRecord<>(
                KafkaTopics.ENROLLMENT_INGESTED, event.partitionKey(), event);
        record.headers()
                .add(header(KafkaTopics.HEADER_CORRELATION_ID, event.correlationId()))
                .add(header(KafkaTopics.HEADER_SOURCE, event.source().name()))
                .add(header(KafkaTopics.HEADER_SCHEMA_VERSION, String.valueOf(event.schemaVersion())));

        kafkaTemplate.send(record).whenComplete((result, error) -> {
            if (error == null) {
                publishedCounter.increment();
            } else {
                failedCounter.increment();
                log.error("Failed to publish enrollment event eventId={} correlationId={}",
                        event.eventId(), event.correlationId(), error);
            }
        });
    }

    /** Flushes the producer buffer; called once at the end of a batch/file ingestion. */
    public void flush() {
        kafkaTemplate.flush();
    }

    private RecordHeader header(String key, String value) {
        return new RecordHeader(key, value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8));
    }
}
