package com.reservation.order.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record CreateOrderRequest(
    @NotBlank(message = "SKU cannot be blank")
    String sku,

    @Min(value = 1, message = "Quantity must be at least 1")
    int quantity
) {}
