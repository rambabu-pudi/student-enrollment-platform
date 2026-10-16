package com.example.enrollment.query.service;

import com.example.enrollment.query.api.dto.DashboardPage;
import com.example.enrollment.query.api.dto.DashboardRow;
import com.example.enrollment.query.repository.DashboardQueryRepository;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

/** Read-side application service: cursor encoding plus paging semantics. */
@Service
public class DashboardQueryService {

    private static final int MAX_LIMIT = 500;
    private static final int DEFAULT_LIMIT = 50;

    private final DashboardQueryRepository repository;

    public DashboardQueryService(DashboardQueryRepository repository) {
        this.repository = repository;
    }

    public DashboardPage latest(String cursorToken, Integer requestedLimit) {
        int limit = clampLimit(requestedLimit);
        var cursor = decodeCursor(cursorToken);

        // Fetch one extra row to determine hasMore without a second COUNT query.
        List<DashboardRow> rows = repository.findLatest(cursor, limit + 1);
        boolean hasMore = rows.size() > limit;
        List<DashboardRow> page = hasMore ? rows.subList(0, limit) : rows;

        String nextCursor = hasMore ? encodeCursor(page.getLast()) : null;
        return new DashboardPage(List.copyOf(page), nextCursor, hasMore);
    }

    public List<DashboardRow> byStudent(String studentId) {
        return repository.findByStudent(studentId);
    }

    public List<DashboardRow> byUniversity(String universityId, Integer requestedLimit) {
        return repository.findByUniversity(universityId, clampLimit(requestedLimit));
    }

    public Optional<DashboardRow> one(String studentId, String universityId) {
        return repository.findOne(studentId, universityId);
    }

    private int clampLimit(Integer requestedLimit) {
        if (requestedLimit == null || requestedLimit <= 0) {
            return DEFAULT_LIMIT;
        }
        return Math.min(requestedLimit, MAX_LIMIT);
    }

    private String encodeCursor(DashboardRow row) {
        String raw = row.updatedAt().toEpochMilli() + "|" + row.studentId() + "|" + row.universityId();
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    private DashboardQueryRepository.Cursor decodeCursor(String token) {
        if (token == null || token.isBlank()) {
            return null;
        }
        try {
            String raw = new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
            String[] parts = raw.split("\\|", 3);
            if (parts.length != 3) {
                throw new IllegalArgumentException("malformed cursor");
            }
            return new DashboardQueryRepository.Cursor(
                    Instant.ofEpochMilli(Long.parseLong(parts[0])), parts[1], parts[2]);
        } catch (IllegalArgumentException e) {
            throw new InvalidCursorException("Invalid pagination cursor");
        }
    }

    /** Raised for a cursor that cannot be decoded. */
    public static class InvalidCursorException extends RuntimeException {
        public InvalidCursorException(String message) {
            super(message);
        }
    }
}
