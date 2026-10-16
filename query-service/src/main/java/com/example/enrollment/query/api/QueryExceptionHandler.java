package com.example.enrollment.query.api;

import com.example.enrollment.query.service.DashboardQueryService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** RFC-7807 errors for the read API. */
@RestControllerAdvice
public class QueryExceptionHandler {

    @ExceptionHandler(DashboardQueryService.InvalidCursorException.class)
    public ProblemDetail handleInvalidCursor(DashboardQueryService.InvalidCursorException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
        problem.setTitle("Invalid cursor");
        return problem;
    }
}
