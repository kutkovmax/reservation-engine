package com.reservation.order.event;

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
) {}
