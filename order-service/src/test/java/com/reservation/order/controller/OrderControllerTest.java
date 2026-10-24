package com.reservation.order.controller;

import com.reservation.order.dto.CreateOrderRequest;
import com.reservation.order.dto.OrderResponse;
import com.reservation.order.model.OrderStatus;
import com.reservation.order.service.CreateOrderResult;
import com.reservation.order.service.OrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderControllerTest {

    @Mock
    private OrderService orderService;

    private OrderController orderController;

    @BeforeEach
    void setUp() {
        orderController = new OrderController(orderService);
    }

    @Test
    void createReturnsAcceptedForNewOrder() {
        CreateOrderRequest request = new CreateOrderRequest("BOOK-001", 2);
        UUID orderId = UUID.randomUUID();
        Instant now = Instant.now();
        OrderResponse orderResponse = new OrderResponse(
            orderId, "BOOK-001", 2, OrderStatus.PENDING, null, now, now, null, null
        );
        when(orderService.create("key-1", request)).thenReturn(new CreateOrderResult(orderResponse, true));

        ResponseEntity<OrderResponse> response = orderController.create("key-1", request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody()).isEqualTo(orderResponse);
        verify(orderService).create("key-1", request);
    }

    @Test
    void duplicateOrderReturnsOk() {
        CreateOrderRequest request = new CreateOrderRequest("BOOK-001", 2);
        UUID orderId = UUID.randomUUID();
        Instant now = Instant.now();
        OrderResponse orderResponse = new OrderResponse(
            orderId, "BOOK-001", 2, OrderStatus.PENDING, null, now, now, null, null
        );
        when(orderService.create("key-1", request)).thenReturn(new CreateOrderResult(orderResponse, false));

        ResponseEntity<OrderResponse> response = orderController.create("key-1", request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo(orderResponse);
        verify(orderService).create("key-1", request);
    }

    @Test
    void getReturnsOrderById() {
        UUID orderId = UUID.randomUUID();
        Instant now = Instant.now();
        OrderResponse orderResponse = new OrderResponse(
            orderId, "BOOK-001", 2, OrderStatus.PENDING, null, now, now, null, null
        );
        when(orderService.get(orderId)).thenReturn(orderResponse);

        OrderResponse response = orderController.get(orderId);

        assertThat(response).isEqualTo(orderResponse);
        verify(orderService).get(orderId);
    }
}
