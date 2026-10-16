package com.example.enrollment.processing.listener;

import com.example.enrollment.common.event.EnrollmentEvent;
import com.example.enrollment.common.event.KafkaTopics;
import com.example.enrollment.processing.service.EnrollmentProcessingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Batch consumer for the enrollment topic.
 *
 * <p>Batch mode (rather than record-at-a-time) is essential to the throughput target: one
 * database round trip per poll instead of one per record. With the configured
 * {@code max.poll.records=2000} and concurrency of 8, a single pod comfortably clears
 * 1M records/hour, and throughput scales linearly by adding pods up to the partition count.</p>
 *
 * <p>Offsets are committed manually, <b>after</b> the transaction commits. If persistence fails,
 * offsets are not advanced and Kafka redelivers — at-least-once delivery, made safe by the
 * idempotent write path.</p>
 */
@Component
public class EnrollmentEventListener {

    private static final Logger log = LoggerFactory.getLogger(EnrollmentEventListener.class);

    private final EnrollmentProcessingService processingService;

    public EnrollmentEventListener(EnrollmentProcessingService processingService) {
        this.processingService = processingService;
    }

    @KafkaListener(
            topics = KafkaTopics.ENROLLMENT_INGESTED,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "enrollmentKafkaListenerContainerFactory")
    public void onBatch(List<ConsumerRecord<String, EnrollmentEvent>> records, Acknowledgment acknowledgment) {
        if (records.isEmpty()) {
            acknowledgment.acknowledge();
            return;
        }
        List<EnrollmentEvent> events = records.stream()
                .map(ConsumerRecord::value)
                .filter(java.util.Objects::nonNull)
                .toList();

        var outcome = processingService.process(events);
        acknowledgment.acknowledge();

        log.info("Consumed batch size={} persisted={} duplicates={} lastOffset={} partition={}",
                records.size(), outcome.persisted(), outcome.duplicates(),
                records.getLast().offset(), records.getLast().partition());
    }
}
