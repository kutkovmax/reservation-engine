package com.reservation.order.service;

import com.reservation.order.dto.OrderResponse;

public record CreateOrderResult(OrderResponse order, boolean created) {}
