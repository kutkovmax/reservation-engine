package com.reservation.inventory.event;

import java.time.Instant;
import java.util.UUID;

public record OrderPlacedEvent(
    UUID eventId,
    UUID orderId,
    String sku,
    int quantity,
    Instant createdAt
) {}
