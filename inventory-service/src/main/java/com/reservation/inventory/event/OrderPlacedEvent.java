package com.reservation.inventory.event;

import java.time.Instant;
import java.util.UUID;

public record OrderPlacedEvent(
    UUID eventId,
    String eventType,
    int eventVersion,
    long aggregateVersion,
    UUID orderId,
    String sku,
    int quantity,
    Instant occurredAt
) {
    public OrderPlacedEvent(UUID eventId, UUID orderId, String sku, int quantity, Instant occurredAt) {
        this(eventId, "OrderPlaced", 1, 1L, orderId, sku, quantity, occurredAt);
    }
}

