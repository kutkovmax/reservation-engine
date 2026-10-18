package com.reservation.order.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record CreateOrderRequest(
    @NotBlank(message = "SKU cannot be blank")
    String sku,

    @NotNull(message = "must not be null")
    @Min(value = 1, message = "must be between 1 and 1000000")
    @Max(value = 1_000_000, message = "must be between 1 and 1000000")
    Integer quantity
) {}
