package com.reservation.order.controller;

import com.reservation.order.dto.CreateOrderRequest;
import com.reservation.order.dto.OrderResponse;
import com.reservation.order.event.OrderPlacedEvent;
import com.reservation.order.model.OrderStatus;
import com.reservation.order.service.CreateOrderResult;
import com.reservation.order.service.OrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderControllerTest {

    @Mock
    private OrderService orderService;

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    private OrderController orderController;

    @BeforeEach
    void setUp() {
        orderController = new OrderController(orderService, kafkaTemplate, false);
    }

    @Test
    void usesSkuAsKafkaPartitionKeyOnOrderCreation() {
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

        ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate).send(eq("order-events"), eq("BOOK-001"), eventCaptor.capture());

        assertThat(eventCaptor.getValue())
            .isInstanceOfSatisfying(OrderPlacedEvent.class, event -> {
                assertThat(event.orderId()).isEqualTo(orderId);
                assertThat(event.sku()).isEqualTo("BOOK-001");
                assertThat(event.quantity()).isEqualTo(2);
                assertThat(event.eventType()).isEqualTo("OrderPlaced");
                assertThat(event.eventVersion()).isEqualTo(1);
            });
    }

    @Test
    void duplicateOrderReturnsOkWithoutPublishingKafkaEvent() {
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
        verifyNoInteractions(kafkaTemplate);
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
    }

    @Test
    void simulatedKafkaFailureThrowsExceptionAfterOrderSavedLeavingGhostOrder() {
        OrderController controllerWithFailure = new OrderController(orderService, kafkaTemplate, true);
        CreateOrderRequest request = new CreateOrderRequest("BOOK-001", 2);
        UUID orderId = UUID.randomUUID();
        Instant now = Instant.now();
        OrderResponse orderResponse = new OrderResponse(
            orderId, "BOOK-001", 2, OrderStatus.PENDING, null, now, now, null, null
        );
        when(orderService.create("key-simulated-fail", request))
            .thenReturn(new CreateOrderResult(orderResponse, true));

        assertThatThrownBy(() -> controllerWithFailure.create("key-simulated-fail", request))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Simulated network failure before Kafka send");

        verify(orderService).create("key-simulated-fail", request);
        verifyNoInteractions(kafkaTemplate);
    }
}


