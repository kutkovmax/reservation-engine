package com.reservation.order.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderCancelledEvent(
    UUID eventId,
    String eventType,
    int eventVersion,
    long aggregateVersion,
    UUID orderId,
    String sku,
    String reason,
    Instant occurredAt
) {
    public static final String EVENT_TYPE = "OrderCancelled";
    public static final int EVENT_VERSION = 1;

    public OrderCancelledEvent(
        UUID eventId,
        long aggregateVersion,
        UUID orderId,
        String sku,
        String reason,
        Instant occurredAt
    ) {
        this(eventId, EVENT_TYPE, EVENT_VERSION, aggregateVersion, orderId, sku, reason, occurredAt);
    }
}
