package com.example.enrollment.query.api.dto;

import java.util.List;

/**
 * Keyset-paginated page.
 *
 * <p>The dashboard pages through a 24M-row projection. OFFSET pagination degrades linearly
 * (the database must walk and discard every skipped row), so the API returns an opaque
 * {@code nextCursor} instead. Cursor paging stays at constant cost on any page.</p>
 */
public record DashboardPage(
        List<DashboardRow> rows,
        String nextCursor,
        boolean hasMore) {
}
