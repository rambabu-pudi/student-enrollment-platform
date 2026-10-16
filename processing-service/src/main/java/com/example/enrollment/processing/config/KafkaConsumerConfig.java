package com.example.enrollment.processing.config;

import com.example.enrollment.common.event.EnrollmentEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Consumer wiring for high-throughput, failure-tolerant processing.
 *
 * <p>Resilience chain:</p>
 * <ol>
 *   <li>{@link ErrorHandlingDeserializer} — a poison message that cannot be deserialised is
 *       turned into a null payload instead of killing the container in an infinite loop.</li>
 *   <li>{@link DefaultErrorHandler} with exponential backoff — retries transient failures
 *       (database blip, lock timeout) without hammering the dependency.</li>
 *   <li>{@link DeadLetterPublishingRecoverer} — once the retry budget is spent the record is
 *       routed to the DLT so the partition keeps moving. This is the circuit that prevents one
 *       bad record from blocking the whole feed.</li>
 * </ol>
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, EnrollmentEvent> consumerFactory(KafkaProperties kafkaProperties,
                                                                    ObjectMapper objectMapper,
                                                                    @Value("${enrollment.processing.max-poll-records:2000}") int maxPollRecords) {
        Map<String, Object> config = new HashMap<>(kafkaProperties.buildConsumerProperties(null));
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, EnrollmentEvent.class.getName());
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.enrollment.common.*");
        config.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        // Generous poll interval: a 2000-record batch plus DB round trip must never trip a rebalance.
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);
        config.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 64 * 1024);
        config.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 200);

        JsonDeserializer<EnrollmentEvent> jsonDeserializer =
                new JsonDeserializer<>(EnrollmentEvent.class, objectMapper, false);
        jsonDeserializer.addTrustedPackages("com.example.enrollment.common.*");

        return new DefaultKafkaConsumerFactory<>(
                config, new StringDeserializer(), new ErrorHandlingDeserializer<>(jsonDeserializer));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, EnrollmentEvent> enrollmentKafkaListenerContainerFactory(
            ConsumerFactory<String, EnrollmentEvent> consumerFactory,
            DefaultErrorHandler errorHandler,
            @Value("${enrollment.processing.concurrency:8}") int concurrency) {

        ConcurrentKafkaListenerContainerFactory<String, EnrollmentEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(concurrency);
        factory.setCommonErrorHandler(errorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> deadLetterKafkaTemplate) {
        ExponentialBackOff backOff = new ExponentialBackOff(500L, 2.0);
        backOff.setMaxElapsedTime(60_000L);
        return new DefaultErrorHandler(new DeadLetterPublishingRecoverer(deadLetterKafkaTemplate), backOff);
    }
}
