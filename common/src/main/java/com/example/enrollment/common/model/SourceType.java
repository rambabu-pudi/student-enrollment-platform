package com.example.enrollment.common.model;

/**
 * Origin channel of an enrollment record. Retained end-to-end for lineage,
 * auditing and per-source throughput metrics.
 */
public enum SourceType {
    CSV,
    XML,
    JSON_API,
    UNKNOWN
}
