package com.reservation.inventory.listener;

import com.reservation.inventory.event.OrderPlacedEvent;
import com.reservation.inventory.service.InventoryReservationService;
import com.reservation.inventory.service.ReservationOutcome;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderEventListenerTest {

    @Mock
    private InventoryReservationService reservationService;

    @Test
    void redeliveryAfterSimulatedCrashSkipsDuplicateReservation() {
        OrderPlacedEvent event = new OrderPlacedEvent(
            UUID.randomUUID(),
            UUID.randomUUID(),
            "BOOK-001",
            2,
            Instant.now()
        );
        OrderEventListener listener = new OrderEventListener(reservationService, true);
        when(reservationService.reserve(event))
            .thenReturn(ReservationOutcome.RESERVED, ReservationOutcome.DUPLICATE);

        assertThatThrownBy(() -> listener.handleOrderPlaced(event))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Simulated consumer crash after stock reservation");

        assertThatCode(() -> listener.handleOrderPlaced(event)).doesNotThrowAnyException();

        verify(reservationService, times(2)).reserve(event);
    }

    @Test
    void handleOrderCancelledDelegatesToService() {
        OrderEventListener listener = new OrderEventListener(reservationService, false);
        com.reservation.inventory.event.OrderCancelledEvent event = new com.reservation.inventory.event.OrderCancelledEvent(
            UUID.randomUUID(),
            2L,
            UUID.randomUUID(),
            "BOOK-001",
            "USER_REQUESTED",
            Instant.now()
        );

        listener.handleOrderCancelled(event);

        verify(reservationService).cancel(event);
    }

    @Test
    void handleOrderEventRoutesOrderCancelledJson() {
        OrderEventListener listener = new OrderEventListener(reservationService, false);
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        String json = """
            {
              "eventId": "%s",
              "eventType": "OrderCancelled",
              "eventVersion": 1,
              "aggregateVersion": 2,
              "orderId": "%s",
              "sku": "BOOK-001",
              "reason": "USER_REQUESTED",
              "occurredAt": "2026-11-01T12:35:00Z"
            }
            """.formatted(eventId, orderId);

        listener.handleOrderEvent(json);

        org.mockito.ArgumentCaptor<com.reservation.inventory.event.OrderCancelledEvent> captor =
            org.mockito.ArgumentCaptor.forClass(com.reservation.inventory.event.OrderCancelledEvent.class);
        verify(reservationService).cancel(captor.capture());
        assertThat(captor.getValue().eventId()).isEqualTo(eventId);
        assertThat(captor.getValue().orderId()).isEqualTo(orderId);
        assertThat(captor.getValue().reason()).isEqualTo("USER_REQUESTED");
    }
}
