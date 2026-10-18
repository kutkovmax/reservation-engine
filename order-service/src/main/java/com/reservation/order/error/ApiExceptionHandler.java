package com.reservation.order.error;

import com.reservation.order.exception.IdempotencyKeyReusedException;
import com.reservation.order.exception.OrderNotFoundException;
import com.reservation.order.exception.RequestValidationException;
import com.reservation.order.web.TraceIdFilter;
import org.slf4j.MDC;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<ApiError> handleBeanValidation(MethodArgumentNotValidException exception) {
        Map<String, String> details = new LinkedHashMap<>();
        exception.getBindingResult().getFieldErrors().forEach(error ->
            details.putIfAbsent(error.getField(), error.getDefaultMessage())
        );
        return error(HttpStatus.BAD_REQUEST, "VALIDATION_ERROR", "Request validation failed", details);
    }

    @ExceptionHandler(RequestValidationException.class)
    ResponseEntity<ApiError> handleRequestValidation(RequestValidationException exception) {
        return error(
            HttpStatus.BAD_REQUEST,
            "VALIDATION_ERROR",
            exception.getMessage(),
            exception.getDetails()
        );
    }

    @ExceptionHandler(MissingRequestHeaderException.class)
    ResponseEntity<ApiError> handleMissingHeader(MissingRequestHeaderException exception) {
        return error(
            HttpStatus.BAD_REQUEST,
            "VALIDATION_ERROR",
            "Request validation failed",
            Map.of(exception.getHeaderName(), "is required")
        );
    }

    @ExceptionHandler({HttpMessageNotReadableException.class, MethodArgumentTypeMismatchException.class})
    ResponseEntity<ApiError> handleMalformedRequest(Exception exception) {
        return error(
            HttpStatus.BAD_REQUEST,
            "VALIDATION_ERROR",
            "Request validation failed",
            Map.of("request", "is malformed")
        );
    }

    @ExceptionHandler(IdempotencyKeyReusedException.class)
    ResponseEntity<ApiError> handleIdempotencyConflict(IdempotencyKeyReusedException exception) {
        return error(
            HttpStatus.CONFLICT,
            "IDEMPOTENCY_KEY_REUSED",
            exception.getMessage(),
            Map.of()
        );
    }

    @ExceptionHandler(OrderNotFoundException.class)
    ResponseEntity<ApiError> handleNotFound(OrderNotFoundException exception) {
        return error(HttpStatus.NOT_FOUND, "ORDER_NOT_FOUND", exception.getMessage(), Map.of());
    }

    @ExceptionHandler(DataAccessException.class)
    ResponseEntity<ApiError> handleDatabaseUnavailable(DataAccessException exception) {
        return error(
            HttpStatus.SERVICE_UNAVAILABLE,
            "DATABASE_UNAVAILABLE",
            "Order database is unavailable",
            Map.of()
        );
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<ApiError> handleUnexpected(Exception exception) {
        return error(
            HttpStatus.INTERNAL_SERVER_ERROR,
            "INTERNAL_ERROR",
            "Unexpected internal error",
            Map.of()
        );
    }

    private ResponseEntity<ApiError> error(
        HttpStatus status,
        String code,
        String message,
        Map<String, String> details
    ) {
        String traceId = MDC.get(TraceIdFilter.TRACE_ID);
        if (traceId == null) {
            traceId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        }
        ApiError error = new ApiError(code, message, details, Instant.now(), traceId);
        return ResponseEntity.status(status).body(error);
    }
}
