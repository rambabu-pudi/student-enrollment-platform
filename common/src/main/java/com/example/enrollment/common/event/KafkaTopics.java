package com.example.enrollment.common.event;

/** Kafka topic and header name constants shared by producers and consumers. */
public final class KafkaTopics {

    /** Main ingestion topic. Recommended: 24 partitions, replication factor 3, compression=lz4. */
    public static final String ENROLLMENT_INGESTED = "enrollment.ingested.v1";

    /** Dead letter topic for records that fail after the retry budget is exhausted. */
    public static final String ENROLLMENT_INGESTED_DLT = "enrollment.ingested.v1.DLT";

    public static final String HEADER_CORRELATION_ID = "x-correlation-id";
    public static final String HEADER_SOURCE = "x-source";
    public static final String HEADER_SCHEMA_VERSION = "x-schema-version";

    private KafkaTopics() {
    }
}
