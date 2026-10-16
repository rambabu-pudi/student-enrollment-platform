package com.example.enrollment.query.api;

import com.example.enrollment.query.api.dto.DashboardPage;
import com.example.enrollment.query.api.dto.DashboardRow;
import com.example.enrollment.query.service.DashboardQueryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.CacheControl;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.List;

/**
 * Dashboard read API.
 *
 * <p>Every endpoint serves the pre-aggregated projection, so response time is independent of
 * the 24M-row corpus. A short {@code Cache-Control} window lets a CDN or gateway absorb the
 * refresh polling that dashboards generate.</p>
 */
@RestController
@RequestMapping("/api/v1/dashboard")
@Tag(name = "Dashboard", description = "Latest student / university / classes view")
public class DashboardController {

    private final DashboardQueryService queryService;

    public DashboardController(DashboardQueryService queryService) {
        this.queryService = queryService;
    }

    @Operation(summary = "Latest enrollment rows, newest first, keyset paginated")
    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<DashboardPage> latest(
            @RequestParam(required = false) String cursor,
            @RequestParam(required = false) Integer limit) {

        DashboardPage page = queryService.latest(cursor, limit);
        return ResponseEntity.ok()
                .cacheControl(CacheControl.maxAge(Duration.ofSeconds(5)).cachePublic())
                .body(page);
    }

    @Operation(summary = "All universities and classes for one student")
    @GetMapping(path = "/students/{studentId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<DashboardRow>> byStudent(@PathVariable String studentId) {
        return ResponseEntity.ok(queryService.byStudent(studentId));
    }

    @Operation(summary = "All students and classes for one university")
    @GetMapping(path = "/universities/{universityId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<DashboardRow>> byUniversity(
            @PathVariable String universityId,
            @RequestParam(required = false) Integer limit) {
        return ResponseEntity.ok(queryService.byUniversity(universityId, limit));
    }

    @Operation(summary = "Single student/university cell")
    @GetMapping(path = "/students/{studentId}/universities/{universityId}",
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<DashboardRow> one(@PathVariable String studentId,
                                            @PathVariable String universityId) {
        return queryService.one(studentId, universityId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
