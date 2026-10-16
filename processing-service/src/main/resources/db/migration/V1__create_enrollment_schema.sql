-- =====================================================================================
-- Enrollment write model + dashboard read model
--
-- Sizing assumption: 1M records/hour, 24M records total.
--
-- Design decisions:
--   * The natural key (student_id, university_id, class_code) IS the primary key.
--     This makes deduplication a database-enforced invariant rather than application
--     logic that can be bypassed by a second pod, a replayed file or a Kafka redelivery.
--   * HASH partitioning on student_id keeps each partition's index small enough to stay
--     resident in shared_buffers at 24M rows, which is what keeps upsert latency flat.
--     student_id is part of the PK, which Postgres requires for a partitioned unique index.
--   * The dashboard projection is a separate, pre-aggregated table (CQRS read model) so the
--     dashboard never runs a GROUP BY over 24M rows.
-- =====================================================================================

CREATE TABLE enrollment (
    student_id      VARCHAR(64)  NOT NULL,
    university_id   VARCHAR(64)  NOT NULL,
    class_code      VARCHAR(64)  NOT NULL,
    source          VARCHAR(16)  NOT NULL,
    correlation_id  VARCHAR(64),
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT pk_enrollment PRIMARY KEY (student_id, university_id, class_code)
) PARTITION BY HASH (student_id);

CREATE TABLE enrollment_p0 PARTITION OF enrollment FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE enrollment_p1 PARTITION OF enrollment FOR VALUES WITH (MODULUS 8, REMAINDER 1);
CREATE TABLE enrollment_p2 PARTITION OF enrollment FOR VALUES WITH (MODULUS 8, REMAINDER 2);
CREATE TABLE enrollment_p3 PARTITION OF enrollment FOR VALUES WITH (MODULUS 8, REMAINDER 3);
CREATE TABLE enrollment_p4 PARTITION OF enrollment FOR VALUES WITH (MODULUS 8, REMAINDER 4);
CREATE TABLE enrollment_p5 PARTITION OF enrollment FOR VALUES WITH (MODULUS 8, REMAINDER 5);
CREATE TABLE enrollment_p6 PARTITION OF enrollment FOR VALUES WITH (MODULUS 8, REMAINDER 6);
CREATE TABLE enrollment_p7 PARTITION OF enrollment FOR VALUES WITH (MODULUS 8, REMAINDER 7);

-- Supports "which students attend university U" style drill-downs.
CREATE INDEX idx_enrollment_university ON enrollment (university_id, student_id);

-- =====================================================================================
-- Read model: exactly the dashboard shape  S1, U1, [C1, C2, C3]
-- =====================================================================================

CREATE TABLE dashboard_student_university (
    student_id     VARCHAR(64) NOT NULL,
    university_id  VARCHAR(64) NOT NULL,
    class_codes    TEXT[]      NOT NULL DEFAULT '{}',
    class_count    INTEGER     NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_dashboard_student_university PRIMARY KEY (student_id, university_id)
);

-- Dashboard default view is "latest data first".
CREATE INDEX idx_dashboard_updated_at ON dashboard_student_university (updated_at DESC);
CREATE INDEX idx_dashboard_university ON dashboard_student_university (university_id, student_id);

COMMENT ON TABLE enrollment IS
    'Authoritative write model. Primary key enforces global deduplication of (student, university, class).';
COMMENT ON TABLE dashboard_student_university IS
    'CQRS read model, maintained incrementally by processing-service. One row per (student, university).';
