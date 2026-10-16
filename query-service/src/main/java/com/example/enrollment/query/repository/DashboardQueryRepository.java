package com.example.enrollment.query.repository;

import com.example.enrollment.query.api.dto.DashboardRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Read-side repository. Every query hits the pre-aggregated projection table, so no
 * dashboard request ever touches the 24M-row write model.
 */
@Repository
public class DashboardQueryRepository {

    private static final String BASE_COLUMNS =
            "student_id, university_id, class_codes, class_count, updated_at";

    private static final RowMapper<DashboardRow> ROW_MAPPER = (ResultSet rs, int rowNum) -> {
        Array array = rs.getArray("class_codes");
        List<String> classes = array == null
                ? List.of()
                : Arrays.asList((String[]) array.getArray());
        return new DashboardRow(
                rs.getString("student_id"),
                rs.getString("university_id"),
                classes,
                rs.getInt("class_count"),
                rs.getTimestamp("updated_at").toInstant());
    };

    private final JdbcTemplate jdbcTemplate;

    public DashboardQueryRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Latest-first feed, keyset paginated on {@code (updated_at, student_id, university_id)}.
     * The composite tie-breaker makes the order total, which keyset paging requires to avoid
     * skipping or repeating rows when many rows share a timestamp.
     */
    public List<DashboardRow> findLatest(Cursor cursor, int limit) {
        if (cursor == null) {
            return jdbcTemplate.query("""
                    SELECT %s FROM dashboard_student_university
                    ORDER BY updated_at DESC, student_id DESC, university_id DESC
                    LIMIT ?
                    """.formatted(BASE_COLUMNS), ROW_MAPPER, limit);
        }
        return jdbcTemplate.query("""
                SELECT %s FROM dashboard_student_university
                WHERE (updated_at, student_id, university_id) < (?, ?, ?)
                ORDER BY updated_at DESC, student_id DESC, university_id DESC
                LIMIT ?
                """.formatted(BASE_COLUMNS),
                ROW_MAPPER,
                java.sql.Timestamp.from(cursor.updatedAt()),
                cursor.studentId(),
                cursor.universityId(),
                limit);
    }

    /** All universities and classes for one student — the drill-down view. */
    public List<DashboardRow> findByStudent(String studentId) {
        return jdbcTemplate.query("""
                SELECT %s FROM dashboard_student_university
                WHERE student_id = ?
                ORDER BY university_id
                """.formatted(BASE_COLUMNS), ROW_MAPPER, studentId.trim().toUpperCase());
    }

    public List<DashboardRow> findByUniversity(String universityId, int limit) {
        return jdbcTemplate.query("""
                SELECT %s FROM dashboard_student_university
                WHERE university_id = ?
                ORDER BY student_id
                LIMIT ?
                """.formatted(BASE_COLUMNS), ROW_MAPPER, universityId.trim().toUpperCase(), limit);
    }

    public Optional<DashboardRow> findOne(String studentId, String universityId) {
        List<DashboardRow> rows = jdbcTemplate.query("""
                SELECT %s FROM dashboard_student_university
                WHERE student_id = ? AND university_id = ?
                """.formatted(BASE_COLUMNS), ROW_MAPPER,
                studentId.trim().toUpperCase(), universityId.trim().toUpperCase());
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.getFirst());
    }

    /** Keyset cursor position. */
    public record Cursor(java.time.Instant updatedAt, String studentId, String universityId) {
    }
}
