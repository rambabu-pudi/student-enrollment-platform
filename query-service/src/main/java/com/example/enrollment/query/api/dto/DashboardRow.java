package com.example.enrollment.query.api.dto;

import java.time.Instant;
import java.util.List;

/**
 * One dashboard row, exactly the required shape:
 * <pre>
 * S1, U1, [C1, C2, C3]
 * S1, U2, [C3]
 * S2, U2, [C1, C3]
 * </pre>
 */
public record DashboardRow(
        String studentId,
        String universityId,
        List<String> classes,
        int classCount,
        Instant updatedAt) {
}
