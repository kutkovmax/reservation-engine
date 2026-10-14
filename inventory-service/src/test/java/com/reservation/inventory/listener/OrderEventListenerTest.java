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
    void redeliveryAfterSimulatedCrashReservesStockTwice() {
        OrderPlacedEvent event = new OrderPlacedEvent(
            UUID.randomUUID(),
            UUID.randomUUID(),
            "BOOK-001",
            2,
            Instant.now()
        );
        OrderEventListener listener = new OrderEventListener(reservationService, true);
        when(reservationService.reserve(event)).thenReturn(ReservationOutcome.RESERVED);

        assertThatThrownBy(() -> listener.handleOrderPlaced(event))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Simulated consumer crash after stock reservation");

        assertThatCode(() -> listener.handleOrderPlaced(event)).doesNotThrowAnyException();

        verify(reservationService, times(2)).reserve(event);
    }
}
