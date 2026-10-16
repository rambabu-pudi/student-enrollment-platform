package com.example.enrollment.ingestion.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/** Externalised ingestion tuning knobs (Externalised Configuration pattern). */
@Component
@ConfigurationProperties(prefix = "enrollment.ingestion")
public class IngestionProperties {

    /**
     * Size of the in-request deduplication window. Bounded so a very large file cannot
     * exhaust heap; the database unique constraint remains the authoritative guarantee.
     */
    private int dedupWindowSize = 1_000_000;

    /** Maximum records accepted in a single JSON REST batch. */
    private int maxBatchSize = 5_000;

    public int getDedupWindowSize() {
        return dedupWindowSize;
    }

    public void setDedupWindowSize(int dedupWindowSize) {
        this.dedupWindowSize = dedupWindowSize;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public void setMaxBatchSize(int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
    }
}
