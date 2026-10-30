package com.reservation.order.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderExpirationServiceTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private OutboxRepository outboxRepository;

    private OrderExpirationService expirationService;

    @BeforeEach
    void setUp() {
        ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        expirationService = new OrderExpirationService(
            orderRepository,
            outboxRepository,
            objectMapper,
            50
        );
    }

    @Test
    void expireBatchTransitionsExpiredOrdersToStatusExpiredAndEmitsHoldExpiredOutbox() {
        UUID orderId1 = UUID.randomUUID();
        UUID orderId2 = UUID.randomUUID();
        Instant createdAt = Instant.parse("2026-11-01T12:00:00Z");
        Instant expiresAt = Instant.parse("2026-11-01T12:15:00Z");

        OrderEntity order1 = new OrderEntity(
            orderId1, "idemp-1", "hash-1", "BOOK-001", 2,
            OrderStatus.RESERVED, null, 2L, createdAt, createdAt, createdAt, expiresAt
        );
        OrderEntity order2 = new OrderEntity(
            orderId2, "idemp-2", "hash-2", "BOOK-002", 1,
            OrderStatus.RESERVED, null, 2L, createdAt, createdAt, createdAt, expiresAt
        );

        when(orderRepository.findExpiredOrdersForUpdate(any(Instant.class), eq(50)))
            .thenReturn(List.of(order1, order2));

        int expiredCount = expirationService.expireBatch();

        assertThat(expiredCount).isEqualTo(2);
        assertThat(order1.getStatus()).isEqualTo(OrderStatus.EXPIRED);
        assertThat(order1.getAggregateVersion()).isEqualTo(3L);
        assertThat(order2.getStatus()).isEqualTo(OrderStatus.EXPIRED);
        assertThat(order2.getAggregateVersion()).isEqualTo(3L);

        verify(orderRepository, times(2)).save(any(OrderEntity.class));

        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        verify(outboxRepository).insertMessage(
            any(UUID.class),
            any(UUID.class),
            eq(orderId1),
            eq(3L),
            eq("order-events"),
            eq("BOOK-001"),
            eq("OrderCancelled"),
            payloadCaptor.capture(),
            any(Instant.class)
        );

        assertThat(payloadCaptor.getValue())
            .contains("\"eventType\":\"OrderCancelled\"")
            .contains("\"reason\":\"HOLD_EXPIRED\"")
            .contains("\"aggregateVersion\":3");

        verify(outboxRepository).insertMessage(
            any(UUID.class),
            any(UUID.class),
            eq(orderId2),
            eq(3L),
            eq("order-events"),
            eq("BOOK-002"),
            eq("OrderCancelled"),
            any(String.class),
            any(Instant.class)
        );
    }

    @Test
    void expireBatchReturnsZeroWhenNoOrdersExpired() {
        when(orderRepository.findExpiredOrdersForUpdate(any(Instant.class), eq(50)))
            .thenReturn(Collections.emptyList());

        int expiredCount = expirationService.expireBatch();

        assertThat(expiredCount).isEqualTo(0);
        verify(orderRepository, never()).save(any());
        verifyNoInteractions(outboxRepository);
    }
}
