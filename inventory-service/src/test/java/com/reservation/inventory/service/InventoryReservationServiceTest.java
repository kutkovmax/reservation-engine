package com.reservation.inventory.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.reservation.inventory.event.OrderPlacedEvent;
import com.reservation.inventory.model.ReservationEntity;
import com.reservation.inventory.model.ReservationStatus;
import com.reservation.inventory.repository.InventoryRepository;
import com.reservation.inventory.repository.OutboxRepository;
import com.reservation.inventory.repository.ProcessedEventRepository;
import com.reservation.inventory.repository.ReservationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class InventoryReservationServiceTest {

    @Mock
    private InventoryRepository inventoryRepository;

    @Mock
    private ProcessedEventRepository processedEventRepository;

    @Mock
    private ReservationRepository reservationRepository;

    @Mock
    private OutboxRepository outboxRepository;

    private final ObjectMapper objectMapper = new ObjectMapper()
        .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule())
        .disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private InventoryReservationService reservationService;

    @BeforeEach
    void setUp() {
        reservationService = new InventoryReservationService(
            inventoryRepository,
            processedEventRepository,
            reservationRepository,
            outboxRepository,
            objectMapper
        );
    }

    @Test
    void reservesAvailableStockAndWritesOutbox() {
        OrderPlacedEvent event = event("BOOK-001", 2);
        acceptNewEvent(event);
        when(inventoryRepository.reserveStockIfAvailable(eq("BOOK-001"), eq(2), any(Instant.class)))
            .thenReturn(1);

        ReservationOutcome outcome = reservationService.reserve(event);

        assertThat(outcome).isEqualTo(ReservationOutcome.RESERVED);
        verify(inventoryRepository).reserveStockIfAvailable(eq("BOOK-001"), eq(2), any(Instant.class));

        ArgumentCaptor<ReservationEntity> reservationCaptor = ArgumentCaptor.forClass(ReservationEntity.class);
        verify(reservationRepository).save(reservationCaptor.capture());
        ReservationEntity savedReservation = reservationCaptor.getValue();
        assertThat(savedReservation.getOrderId()).isEqualTo(event.orderId());
        assertThat(savedReservation.getStatus()).isEqualTo(ReservationStatus.HELD);
        assertThat(savedReservation.getFailureReason()).isNull();

        verify(outboxRepository).insertMessage(
            any(UUID.class),
            any(UUID.class),
            eq(event.orderId()),
            eq(1L),
            eq("inventory-events"),
            eq(event.orderId().toString()),
            eq("InventoryReserved"),
            any(String.class),
            any(Instant.class)
        );
    }

    @Test
    void rejectsInsufficientStockAndWritesOutbox() {
        OrderPlacedEvent event = event("BOOK-001", 11);
        acceptNewEvent(event);
        when(inventoryRepository.reserveStockIfAvailable(eq("BOOK-001"), eq(11), any(Instant.class)))
            .thenReturn(0);
        when(inventoryRepository.existsById("BOOK-001")).thenReturn(true);

        ReservationOutcome outcome = reservationService.reserve(event);

        assertThat(outcome).isEqualTo(ReservationOutcome.INSUFFICIENT_STOCK);
        verify(inventoryRepository).reserveStockIfAvailable(eq("BOOK-001"), eq(11), any(Instant.class));

        ArgumentCaptor<ReservationEntity> reservationCaptor = ArgumentCaptor.forClass(ReservationEntity.class);
        verify(reservationRepository).save(reservationCaptor.capture());
        ReservationEntity savedReservation = reservationCaptor.getValue();
        assertThat(savedReservation.getOrderId()).isEqualTo(event.orderId());
        assertThat(savedReservation.getStatus()).isEqualTo(ReservationStatus.REJECTED);
        assertThat(savedReservation.getFailureReason()).isEqualTo("INSUFFICIENT_STOCK");

        verify(outboxRepository).insertMessage(
            any(UUID.class),
            any(UUID.class),
            eq(event.orderId()),
            eq(1L),
            eq("inventory-events"),
            eq(event.orderId().toString()),
            eq("InventoryReservationFailed"),
            any(String.class),
            any(Instant.class)
        );
    }

    @Test
    void rejectsUnknownSkuAndWritesOutbox() {
        OrderPlacedEvent event = event("UNKNOWN-001", 1);
        acceptNewEvent(event);
        when(inventoryRepository.reserveStockIfAvailable(eq("UNKNOWN-001"), eq(1), any(Instant.class)))
            .thenReturn(0);
        when(inventoryRepository.existsById("UNKNOWN-001")).thenReturn(false);

        ReservationOutcome outcome = reservationService.reserve(event);

        assertThat(outcome).isEqualTo(ReservationOutcome.SKU_NOT_FOUND);
        verify(inventoryRepository).reserveStockIfAvailable(eq("UNKNOWN-001"), eq(1), any(Instant.class));

        ArgumentCaptor<ReservationEntity> reservationCaptor = ArgumentCaptor.forClass(ReservationEntity.class);
        verify(reservationRepository).save(reservationCaptor.capture());
        ReservationEntity savedReservation = reservationCaptor.getValue();
        assertThat(savedReservation.getOrderId()).isEqualTo(event.orderId());
        assertThat(savedReservation.getStatus()).isEqualTo(ReservationStatus.REJECTED);
        assertThat(savedReservation.getFailureReason()).isEqualTo("SKU_NOT_FOUND");

        verify(outboxRepository).insertMessage(
            any(UUID.class),
            any(UUID.class),
            eq(event.orderId()),
            eq(1L),
            eq("inventory-events"),
            eq(event.orderId().toString()),
            eq("InventoryReservationFailed"),
            any(String.class),
            any(Instant.class)
        );
    }

    @Test
    void skipsStockReservationForDuplicateEvent() {
        OrderPlacedEvent event = event("BOOK-001", 2);
        when(processedEventRepository.insertIfAbsent(
            eq(event.eventId()),
            eq("OrderPlaced"),
            any(Instant.class)
        )).thenReturn(0);

        ReservationOutcome outcome = reservationService.reserve(event);

        assertThat(outcome).isEqualTo(ReservationOutcome.DUPLICATE);
        verifyNoInteractions(inventoryRepository);
        verifyNoInteractions(reservationRepository);
        verifyNoInteractions(outboxRepository);
    }

    @Test
    void skipsReservationWhenOrderIdAlreadyExists() {
        OrderPlacedEvent event = event("BOOK-001", 2);
        acceptNewEvent(event);
        when(reservationRepository.existsById(event.orderId())).thenReturn(true);

        ReservationOutcome outcome = reservationService.reserve(event);

        assertThat(outcome).isEqualTo(ReservationOutcome.DUPLICATE);
        verifyNoInteractions(inventoryRepository);
        verifyNoInteractions(outboxRepository);
    }

    private void acceptNewEvent(OrderPlacedEvent event) {
        when(processedEventRepository.insertIfAbsent(
            eq(event.eventId()),
            eq("OrderPlaced"),
            any(Instant.class)
        )).thenReturn(1);
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
