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
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
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
        ReservationEntity existing = new ReservationEntity(
            event.orderId(), "BOOK-001", 2, ReservationStatus.HELD, null, Instant.now(), Instant.now()
        );
        when(reservationRepository.findById(event.orderId())).thenReturn(Optional.of(existing));

        ReservationOutcome outcome = reservationService.reserve(event);

        assertThat(outcome).isEqualTo(ReservationOutcome.DUPLICATE);
        verifyNoInteractions(inventoryRepository);
        verifyNoInteractions(outboxRepository);
    }

    @Test
    void reserveFailsWithOrderAlreadyCancelledWhenTombstoneExists() {
        OrderPlacedEvent event = event("BOOK-001", 2);
        acceptNewEvent(event);
        ReservationEntity tombstone = new ReservationEntity(
            event.orderId(), "BOOK-001", 0, ReservationStatus.CANCELLED, "ORDER_ALREADY_CANCELLED", Instant.now(), Instant.now()
        );
        when(reservationRepository.findById(event.orderId())).thenReturn(Optional.of(tombstone));

        ReservationOutcome outcome = reservationService.reserve(event);

        assertThat(outcome).isEqualTo(ReservationOutcome.ORDER_ALREADY_CANCELLED);
        verifyNoInteractions(inventoryRepository);

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
    void cancelReleasesHeldStockAndUpdatesReservationToReleased() {
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        com.reservation.inventory.event.OrderCancelledEvent event = new com.reservation.inventory.event.OrderCancelledEvent(
            eventId, 2L, orderId, "BOOK-001", "USER_REQUESTED", Instant.now()
        );

        when(processedEventRepository.insertIfAbsent(eq(eventId), eq("OrderCancelled"), any(Instant.class)))
            .thenReturn(1);
        ReservationEntity reservation = new ReservationEntity(
            orderId, "BOOK-001", 2, ReservationStatus.HELD, null, Instant.now(), Instant.now()
        );
        when(reservationRepository.findById(orderId)).thenReturn(Optional.of(reservation));

        reservationService.cancel(event);

        verify(inventoryRepository).releaseStock(eq("BOOK-001"), eq(2), any(Instant.class));
        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.RELEASED);
        verify(reservationRepository).save(reservation);
    }

    @Test
    void cancelCreatesTombstoneWhenReservationMissing() {
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        com.reservation.inventory.event.OrderCancelledEvent event = new com.reservation.inventory.event.OrderCancelledEvent(
            eventId, 2L, orderId, "BOOK-001", "USER_REQUESTED", Instant.now()
        );

        when(processedEventRepository.insertIfAbsent(eq(eventId), eq("OrderCancelled"), any(Instant.class)))
            .thenReturn(1);
        when(reservationRepository.findById(orderId)).thenReturn(Optional.empty());

        reservationService.cancel(event);

        verifyNoInteractions(inventoryRepository);
        ArgumentCaptor<ReservationEntity> captor = ArgumentCaptor.forClass(ReservationEntity.class);
        verify(reservationRepository).save(captor.capture());
        ReservationEntity saved = captor.getValue();
        assertThat(saved.getOrderId()).isEqualTo(orderId);
        assertThat(saved.getStatus()).isEqualTo(ReservationStatus.CANCELLED);
        assertThat(saved.getFailureReason()).isEqualTo("ORDER_ALREADY_CANCELLED");
    }

    @Test
    void cancelDoesNotReleaseStockWhenAlreadyRejected() {
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        com.reservation.inventory.event.OrderCancelledEvent event = new com.reservation.inventory.event.OrderCancelledEvent(
            eventId, 2L, orderId, "BOOK-001", "USER_REQUESTED", Instant.now()
        );

        when(processedEventRepository.insertIfAbsent(eq(eventId), eq("OrderCancelled"), any(Instant.class)))
            .thenReturn(1);
        ReservationEntity reservation = new ReservationEntity(
            orderId, "BOOK-001", 2, ReservationStatus.REJECTED, "INSUFFICIENT_STOCK", Instant.now(), Instant.now()
        );
        when(reservationRepository.findById(orderId)).thenReturn(Optional.of(reservation));

        reservationService.cancel(event);

        verifyNoInteractions(inventoryRepository);
    }

    @Test
    void cancelSkipsDuplicateEvent() {
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        com.reservation.inventory.event.OrderCancelledEvent event = new com.reservation.inventory.event.OrderCancelledEvent(
            eventId, 2L, orderId, "BOOK-001", "USER_REQUESTED", Instant.now()
        );

        when(processedEventRepository.insertIfAbsent(eq(eventId), eq("OrderCancelled"), any(Instant.class)))
            .thenReturn(0);

        reservationService.cancel(event);

        verifyNoInteractions(inventoryRepository);
        verify(reservationRepository, never()).findById(any());
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
