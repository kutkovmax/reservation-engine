package com.reservation.order.exception;

import java.util.Map;

public class RequestValidationException extends RuntimeException {

    private final Map<String, String> details;

    public RequestValidationException(Map<String, String> details) {
        super("Request validation failed");
        this.details = Map.copyOf(details);
    }

    public Map<String, String> getDetails() {
        return details;
    }
}
