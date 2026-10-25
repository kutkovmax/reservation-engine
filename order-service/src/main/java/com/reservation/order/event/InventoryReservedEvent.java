package com.reservation.order.event;

import java.time.Instant;
import java.util.UUID;

public record InventoryReservedEvent(
    UUID eventId,
    String eventType,
    int eventVersion,
    UUID causationId,
    UUID orderId,
    String sku,
    int quantity,
    Instant occurredAt
) {
    public static final String EVENT_TYPE = "InventoryReserved";
    public static final int EVENT_VERSION = 1;

    public InventoryReservedEvent(
        UUID eventId,
        UUID causationId,
        UUID orderId,
        String sku,
        int quantity,
        Instant occurredAt
    ) {
        this(eventId, EVENT_TYPE, EVENT_VERSION, causationId, orderId, sku, quantity, occurredAt);
    }
}
