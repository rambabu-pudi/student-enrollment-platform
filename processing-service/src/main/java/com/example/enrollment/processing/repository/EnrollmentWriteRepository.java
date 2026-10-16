package com.example.enrollment.processing.repository;

import com.example.enrollment.common.event.EnrollmentEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Write-model repository.
 *
 * <p>Uses plain JDBC batch statements rather than JPA on purpose: at 1M records/hour the
 * per-entity overhead of the persistence context (dirty checking, identity map, flush ordering)
 * is the dominant cost, and none of its features are needed for an append-only upsert.
 * Combined with {@code reWriteBatchedInserts=true} on the JDBC URL, Postgres collapses the
 * batch into multi-row INSERTs — typically a 3-5x throughput improvement.</p>
 *
 * <p>{@code ON CONFLICT DO NOTHING} against the natural-key primary key is the authoritative
 * deduplication step. It is race-free across concurrent consumer pods, which an
 * application-side "SELECT then INSERT" would not be.</p>
 */
@Repository
public class EnrollmentWriteRepository {

    private static final String UPSERT_SQL = """
            INSERT INTO enrollment (student_id, university_id, class_code, source, correlation_id, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (student_id, university_id, class_code) DO NOTHING
            """;

    private final JdbcTemplate jdbcTemplate;

    public EnrollmentWriteRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Inserts the batch, skipping rows that already exist.
     *
     * @return the subset of events that were genuinely new (affected row count 1). Only these
     *         need to be applied to the read model, which keeps projection writes proportional
     *         to real change rather than to traffic volume.
     */
    public List<EnrollmentEvent> insertIgnoringDuplicates(List<EnrollmentEvent> events) {
        if (events.isEmpty()) {
            return List.of();
        }

        int[] affected = jdbcTemplate.batchUpdate(UPSERT_SQL, new org.springframework.jdbc.core.BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                EnrollmentEvent event = events.get(i);
                ps.setString(1, event.payload().studentId());
                ps.setString(2, event.payload().universityId());
                ps.setString(3, event.payload().classCode());
                ps.setString(4, event.source().name());
                ps.setString(5, event.correlationId());
                ps.setTimestamp(6, Timestamp.from(event.occurredAt()));
            }

            @Override
            public int getBatchSize() {
                return events.size();
            }
        });

        List<EnrollmentEvent> inserted = new ArrayList<>(events.size());
        for (int i = 0; i < events.size(); i++) {
            // With reWriteBatchedInserts the driver may report SUCCESS_NO_INFO (-2); treat
            // anything that is not an explicit 0 as "inserted" and let the idempotent
            // projection merge absorb the rare false positive.
            if (i < affected.length && affected[i] != 0) {
                inserted.add(events.get(i));
            }
        }
        return inserted;
    }

    public long countAll() {
        Long count = jdbcTemplate.queryForObject("SELECT count(*) FROM enrollment", Long.class);
        return count == null ? 0L : count;
    }
}
