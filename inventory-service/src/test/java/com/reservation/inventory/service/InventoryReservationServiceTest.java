package com.reservation.inventory.service;

import com.reservation.inventory.event.OrderPlacedEvent;
import com.reservation.inventory.repository.InventoryRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InventoryReservationServiceTest {

    @Mock
    private InventoryRepository inventoryRepository;

    private InventoryReservationService reservationService;

    @BeforeEach
    void setUp() {
        reservationService = new InventoryReservationService(inventoryRepository);
    }

    @Test
    void reservesAvailableStock() {
        OrderPlacedEvent event = event("BOOK-001", 2);
        when(inventoryRepository.reserveStockIfAvailable(eq("BOOK-001"), eq(2), any(Instant.class)))
            .thenReturn(1);

        ReservationOutcome outcome = reservationService.reserve(event);

        assertThat(outcome).isEqualTo(ReservationOutcome.RESERVED);
        verify(inventoryRepository).reserveStockIfAvailable(eq("BOOK-001"), eq(2), any(Instant.class));
    }

    @Test
    void rejectsInsufficientStock() {
        OrderPlacedEvent event = event("BOOK-001", 11);
        when(inventoryRepository.reserveStockIfAvailable(eq("BOOK-001"), eq(11), any(Instant.class)))
            .thenReturn(0);
        when(inventoryRepository.existsById("BOOK-001")).thenReturn(true);

        ReservationOutcome outcome = reservationService.reserve(event);

        assertThat(outcome).isEqualTo(ReservationOutcome.INSUFFICIENT_STOCK);
        verify(inventoryRepository).reserveStockIfAvailable(eq("BOOK-001"), eq(11), any(Instant.class));
    }

    @Test
    void rejectsUnknownSku() {
        OrderPlacedEvent event = event("UNKNOWN-001", 1);
        when(inventoryRepository.reserveStockIfAvailable(eq("UNKNOWN-001"), eq(1), any(Instant.class)))
            .thenReturn(0);
        when(inventoryRepository.existsById("UNKNOWN-001")).thenReturn(false);

        ReservationOutcome outcome = reservationService.reserve(event);

        assertThat(outcome).isEqualTo(ReservationOutcome.SKU_NOT_FOUND);
        verify(inventoryRepository).reserveStockIfAvailable(eq("UNKNOWN-001"), eq(1), any(Instant.class));
    }

    private OrderPlacedEvent event(String sku, int quantity) {
        return new OrderPlacedEvent(
            UUID.randomUUID(),
            UUID.randomUUID(),
            sku,
            quantity,
            Instant.now()
        );
    }
}
