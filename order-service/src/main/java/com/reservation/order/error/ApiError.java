package com.reservation.order.error;

import java.time.Instant;
import java.util.Map;

public record ApiError(
    String code,
    String message,
    Map<String, String> details,
    Instant timestamp,
    String traceId
) {}
