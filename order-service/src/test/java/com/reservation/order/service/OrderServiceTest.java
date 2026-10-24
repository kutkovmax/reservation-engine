package com.reservation.order.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.reservation.order.dto.CreateOrderRequest;
import com.reservation.order.exception.IdempotencyKeyReusedException;
import com.reservation.order.exception.OrderNotFoundException;
import com.reservation.order.model.OrderEntity;
import com.reservation.order.model.OrderStatus;
import com.reservation.order.repository.OrderRepository;
import com.reservation.order.repository.OutboxRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private OutboxRepository outboxRepository;

    private OrderService orderService;

    @BeforeEach
    void setUp() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        orderService = new OrderService(orderRepository, outboxRepository, objectMapper);
    }

    @Test
    void createPersistsOrderAndOutboxMessageInOneTransaction() {
        CreateOrderRequest request = new CreateOrderRequest("BOOK-001", 2);
        when(orderRepository.insertPendingIfAbsent(
            any(UUID.class),
            eq("idemp-key-1"),
            any(String.class),
            eq("BOOK-001"),
            eq(2),
            any(Instant.class)
        )).thenReturn(1);

        CreateOrderResult result = orderService.create("idemp-key-1", request);

        assertThat(result.created()).isTrue();
        assertThat(result.order().sku()).isEqualTo("BOOK-001");
        assertThat(result.order().quantity()).isEqualTo(2);
        assertThat(result.order().status()).isEqualTo(OrderStatus.PENDING);

        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        verify(outboxRepository).insertMessage(
            any(UUID.class),
            any(UUID.class),
            eq(result.order().orderId()),
            eq(1L),
            eq("order-events"),
            eq("BOOK-001"),
            eq("OrderPlaced"),
            payloadCaptor.capture(),
            any(Instant.class)
        );

        String payload = payloadCaptor.getValue();
        assertThat(payload)
            .contains("\"eventType\":\"OrderPlaced\"")
            .contains("\"sku\":\"BOOK-001\"")
            .contains("\"quantity\":2")
            .contains("\"aggregateVersion\":1")
            .contains(result.order().orderId().toString());
    }

    @Test
    void duplicateOrderReturnsExistingWithoutSavingOutboxMessage() {
        CreateOrderRequest request = new CreateOrderRequest("BOOK-001", 2);
        String requestHash = OrderService.requestHash("BOOK-001", 2);
        UUID orderId = UUID.randomUUID();
        Instant now = Instant.now();

        OrderEntity existingOrder = new OrderEntity();
        ReflectionTestUtils.setField(existingOrder, "id", orderId);
        ReflectionTestUtils.setField(existingOrder, "idempotencyKey", "idemp-key-1");
        ReflectionTestUtils.setField(existingOrder, "requestHash", requestHash);
        ReflectionTestUtils.setField(existingOrder, "sku", "BOOK-001");
        ReflectionTestUtils.setField(existingOrder, "quantity", 2);
        ReflectionTestUtils.setField(existingOrder, "status", OrderStatus.PENDING);
        ReflectionTestUtils.setField(existingOrder, "aggregateVersion", 1L);
        ReflectionTestUtils.setField(existingOrder, "createdAt", now);
        ReflectionTestUtils.setField(existingOrder, "updatedAt", now);

        when(orderRepository.insertPendingIfAbsent(
            any(UUID.class),
            eq("idemp-key-1"),
            eq(requestHash),
            eq("BOOK-001"),
            eq(2),
            any(Instant.class)
        )).thenReturn(0);
        when(orderRepository.findByIdempotencyKey("idemp-key-1")).thenReturn(Optional.of(existingOrder));

        CreateOrderResult result = orderService.create("idemp-key-1", request);

        assertThat(result.created()).isFalse();
        assertThat(result.order().orderId()).isEqualTo(orderId);
        verifyNoInteractions(outboxRepository);
    }

    @Test
    void duplicateIdempotencyKeyWithDifferentBodyThrowsConflict() {
        CreateOrderRequest request = new CreateOrderRequest("BOOK-001", 2);
        String requestHash = OrderService.requestHash("BOOK-001", 2);

        OrderEntity existingOrder = new OrderEntity();
        ReflectionTestUtils.setField(existingOrder, "requestHash", "different-hash-value-0000000000000000000000000000000000000000000000000");

        when(orderRepository.insertPendingIfAbsent(
            any(UUID.class),
            eq("idemp-key-1"),
            eq(requestHash),
            eq("BOOK-001"),
            eq(2),
            any(Instant.class)
        )).thenReturn(0);
        when(orderRepository.findByIdempotencyKey("idemp-key-1")).thenReturn(Optional.of(existingOrder));

        assertThatThrownBy(() -> orderService.create("idemp-key-1", request))
            .isInstanceOf(IdempotencyKeyReusedException.class);

        verifyNoInteractions(outboxRepository);
    }

    @Test
    void getThrowsNotFoundWhenOrderMissing() {
        UUID orderId = UUID.randomUUID();
        when(orderRepository.findById(orderId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> orderService.get(orderId))
            .isInstanceOf(OrderNotFoundException.class);
    }
}
