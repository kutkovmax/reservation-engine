package com.reservation.inventory.service;

public enum ReservationOutcome {
    RESERVED,
    DUPLICATE,
    SKU_NOT_FOUND,
    INSUFFICIENT_STOCK,
    ORDER_ALREADY_CANCELLED
}
