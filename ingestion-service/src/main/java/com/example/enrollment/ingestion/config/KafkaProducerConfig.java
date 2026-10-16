package com.example.enrollment.ingestion.config;

import com.example.enrollment.common.event.EnrollmentEvent;
import com.example.enrollment.common.event.EnrollmentEventFactory;
import com.example.enrollment.common.event.KafkaTopics;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.HashMap;
import java.util.Map;

/**
 * Producer tuned for sustained high throughput with no data loss.
 *
 * <p>Key choices:</p>
 * <ul>
 *   <li>{@code acks=all} + {@code enable.idempotence=true} — exactly-once semantics at the
 *       producer level; broker-side retries can no longer create duplicates or reorder.</li>
 *   <li>{@code linger.ms=20} with a 64 KB batch — trades ~20 ms of latency for a very large
 *       throughput gain. At 280 msg/s (1M/hour) this fills batches nicely.</li>
 *   <li>{@code compression=lz4} — roughly 3-5x smaller payloads, which is the difference
 *       between saturating and not saturating the broker NIC at 24M records.</li>
 * </ul>
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, EnrollmentEvent> producerFactory(KafkaProperties kafkaProperties,
                                                                    ObjectMapper objectMapper) {
        Map<String, Object> config = new HashMap<>(kafkaProperties.buildProducerProperties(null));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        config.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 64L * 1024 * 1024);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        DefaultKafkaProducerFactory<String, EnrollmentEvent> factory =
                new DefaultKafkaProducerFactory<>(config);
        factory.setValueSerializer(new JsonSerializer<>(objectMapper));
        return factory;
    }

    @Bean
    public KafkaTemplate<String, EnrollmentEvent> kafkaTemplate(
            ProducerFactory<String, EnrollmentEvent> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public EnrollmentEventFactory enrollmentEventFactory() {
        return new EnrollmentEventFactory();
    }

    /**
     * Declared so a fresh environment is self-bootstrapping. Partition count is the primary
     * horizontal-scaling lever: it caps how many processing-service consumers can run in parallel.
     */
    @Bean
    public NewTopic enrollmentIngestedTopic(@Value("${enrollment.kafka.partitions:24}") int partitions,
                                            @Value("${enrollment.kafka.replication-factor:1}") short replicas) {
        return TopicBuilder.name(KafkaTopics.ENROLLMENT_INGESTED)
                .partitions(partitions)
                .replicas(replicas)
                .config("compression.type", "lz4")
                .config("retention.ms", String.valueOf(7L * 24 * 60 * 60 * 1000))
                .build();
    }

    @Bean
    public NewTopic enrollmentIngestedDltTopic(@Value("${enrollment.kafka.partitions:24}") int partitions,
                                               @Value("${enrollment.kafka.replication-factor:1}") short replicas) {
        return TopicBuilder.name(KafkaTopics.ENROLLMENT_INGESTED_DLT)
                .partitions(partitions)
                .replicas(replicas)
                .build();
    }
}
