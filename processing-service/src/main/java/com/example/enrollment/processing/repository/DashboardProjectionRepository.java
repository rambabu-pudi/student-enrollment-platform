package com.example.enrollment.processing.repository;

import com.example.enrollment.common.event.EnrollmentEvent;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Maintains the CQRS read model that backs the dashboard.
 *
 * <p>The merge is expressed entirely in SQL and is <b>idempotent</b>: applying the same class
 * code twice leaves the row unchanged, because the array is re-aggregated with
 * {@code DISTINCT}. That property is what makes at-least-once Kafka delivery safe here —
 * a redelivered event cannot corrupt the projection.</p>
 *
 * <p>The trailing {@code WHERE NOT (... @> ...)} guard suppresses no-op updates. Without it,
 * every redelivery would create a dead tuple and drive autovacuum churn at 24M scale.</p>
 */
@Repository
public class DashboardProjectionRepository {

    private static final String MERGE_SQL = """
            INSERT INTO dashboard_student_university
                   (student_id, university_id, class_codes, class_count, updated_at)
            VALUES (?, ?, ARRAY[?]::text[], 1, now())
            ON CONFLICT (student_id, university_id) DO UPDATE
            SET class_codes = (
                    SELECT array_agg(DISTINCT c ORDER BY c)
                    FROM unnest(dashboard_student_university.class_codes || excluded.class_codes) AS c
                ),
                class_count = (
                    SELECT count(DISTINCT c)
                    FROM unnest(dashboard_student_university.class_codes || excluded.class_codes) AS c
                ),
                updated_at = now()
            WHERE NOT (dashboard_student_university.class_codes @> excluded.class_codes)
            """;

    private final JdbcTemplate jdbcTemplate;

    public DashboardProjectionRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void merge(List<EnrollmentEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        jdbcTemplate.batchUpdate(MERGE_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                EnrollmentEvent event = events.get(i);
                ps.setString(1, event.payload().studentId());
                ps.setString(2, event.payload().universityId());
                ps.setString(3, event.payload().classCode());
            }

            @Override
            public int getBatchSize() {
                return events.size();
            }
        });
    }
}
