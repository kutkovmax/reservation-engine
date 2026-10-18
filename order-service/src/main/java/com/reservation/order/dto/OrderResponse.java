package com.reservation.order.dto;

import com.reservation.order.model.OrderEntity;
import com.reservation.order.model.OrderStatus;

import java.time.Instant;
import java.util.UUID;

public record OrderResponse(
    UUID orderId,
    String sku,
    int quantity,
    OrderStatus status,
    String rejectionReason,
    Instant createdAt,
    Instant updatedAt,
    Instant reservedAt,
    Instant expiresAt
) {
    public static OrderResponse from(OrderEntity order) {
        return new OrderResponse(
            order.getId(),
            order.getSku(),
            order.getQuantity(),
            order.getStatus(),
            order.getRejectionReason(),
            order.getCreatedAt(),
            order.getUpdatedAt(),
            order.getReservedAt(),
            order.getExpiresAt()
        );
    }
}
