package com.reservation.order.event;

import java.time.Instant;
import java.util.UUID;

public record InventoryReservationFailedEvent(
    UUID eventId,
    String eventType,
    int eventVersion,
    UUID causationId,
    UUID orderId,
    String sku,
    int quantity,
    String reason,
    Instant occurredAt
) {
    public static final String EVENT_TYPE = "InventoryReservationFailed";
    public static final int EVENT_VERSION = 1;

    public InventoryReservationFailedEvent(
        UUID eventId,
        UUID causationId,
        UUID orderId,
        String sku,
        int quantity,
        String reason,
        Instant occurredAt
    ) {
        this(eventId, EVENT_TYPE, EVENT_VERSION, causationId, orderId, sku, quantity, reason, occurredAt);
    }
}
