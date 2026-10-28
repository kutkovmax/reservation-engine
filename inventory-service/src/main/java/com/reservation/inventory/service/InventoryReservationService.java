package com.reservation.inventory.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.reservation.inventory.event.InventoryReservationFailedEvent;
import com.reservation.inventory.event.InventoryReservedEvent;
import com.reservation.inventory.event.OrderCancelledEvent;
import com.reservation.inventory.event.OrderPlacedEvent;
import com.reservation.inventory.model.ReservationEntity;
import com.reservation.inventory.model.ReservationStatus;
import com.reservation.inventory.repository.InventoryRepository;
import com.reservation.inventory.repository.OutboxRepository;
import com.reservation.inventory.repository.ProcessedEventRepository;
import com.reservation.inventory.repository.ReservationRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
public class InventoryReservationService {

    private static final String ORDER_PLACED_EVENT_TYPE = "OrderPlaced";
    private static final String INVENTORY_EVENTS_TOPIC = "inventory-events";

    private final InventoryRepository inventoryRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final ReservationRepository reservationRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public InventoryReservationService(
        InventoryRepository inventoryRepository,
        ProcessedEventRepository processedEventRepository,
        ReservationRepository reservationRepository,
        OutboxRepository outboxRepository,
        ObjectMapper objectMapper
    ) {
        this.inventoryRepository = inventoryRepository;
        this.processedEventRepository = processedEventRepository;
        this.reservationRepository = reservationRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public ReservationOutcome reserve(OrderPlacedEvent event) {
        if (event.quantity() < 1) {
            throw new IllegalArgumentException("Reservation quantity must be positive");
        }

        Instant now = Instant.now();
        int insertedEvents = processedEventRepository.insertIfAbsent(
            event.eventId(),
            ORDER_PLACED_EVENT_TYPE,
            now
        );
        if (insertedEvents == 0) {
            return ReservationOutcome.DUPLICATE;
        }

        Optional<ReservationEntity> existingOpt = reservationRepository.findById(event.orderId());
        if (existingOpt.isPresent()) {
            ReservationEntity existing = existingOpt.get();
            if (existing.getStatus() == ReservationStatus.CANCELLED) {
                UUID outEventId = UUID.randomUUID();
                InventoryReservationFailedEvent outEvent = new InventoryReservationFailedEvent(
                    outEventId,
                    event.eventId(),
                    event.orderId(),
                    event.sku(),
                    event.quantity(),
                    "ORDER_ALREADY_CANCELLED",
                    now
                );
                outboxRepository.insertMessage(
                    UUID.randomUUID(),
                    outEventId,
                    event.orderId(),
                    1L,
                    INVENTORY_EVENTS_TOPIC,
                    event.orderId().toString(),
                    InventoryReservationFailedEvent.EVENT_TYPE,
                    writePayload(outEvent),
                    now
                );
                return ReservationOutcome.ORDER_ALREADY_CANCELLED;
            }
            return ReservationOutcome.DUPLICATE;
        }

        int updatedRows = inventoryRepository.reserveStockIfAvailable(
            event.sku(),
            event.quantity(),
            now
        );
        if (updatedRows == 1) {
            ReservationEntity reservation = new ReservationEntity(
                event.orderId(),
                event.sku(),
                event.quantity(),
                ReservationStatus.HELD,
                null,
                now,
                now
            );
            reservationRepository.save(reservation);

            UUID outEventId = UUID.randomUUID();
            InventoryReservedEvent outEvent = new InventoryReservedEvent(
                outEventId,
                event.eventId(),
                event.orderId(),
                event.sku(),
                event.quantity(),
                now
            );
            outboxRepository.insertMessage(
                UUID.randomUUID(),
                outEventId,
                event.orderId(),
                1L,
                INVENTORY_EVENTS_TOPIC,
                event.orderId().toString(),
                InventoryReservedEvent.EVENT_TYPE,
                writePayload(outEvent),
                now
            );

            return ReservationOutcome.RESERVED;
        }

        ReservationOutcome outcome = inventoryRepository.existsById(event.sku())
            ? ReservationOutcome.INSUFFICIENT_STOCK
            : ReservationOutcome.SKU_NOT_FOUND;

        String failureReason = outcome.name();
        ReservationEntity reservation = new ReservationEntity(
            event.orderId(),
            event.sku(),
            event.quantity(),
            ReservationStatus.REJECTED,
            failureReason,
            now,
            now
        );
        reservationRepository.save(reservation);

        UUID outEventId = UUID.randomUUID();
        InventoryReservationFailedEvent outEvent = new InventoryReservationFailedEvent(
            outEventId,
            event.eventId(),
            event.orderId(),
            event.sku(),
            event.quantity(),
            failureReason,
            now
        );
        outboxRepository.insertMessage(
            UUID.randomUUID(),
            outEventId,
            event.orderId(),
            1L,
            INVENTORY_EVENTS_TOPIC,
            event.orderId().toString(),
            InventoryReservationFailedEvent.EVENT_TYPE,
            writePayload(outEvent),
            now
        );

        return outcome;
    }

    @Transactional
    public void cancel(OrderCancelledEvent event) {
        Instant now = Instant.now();
        int insertedEvents = processedEventRepository.insertIfAbsent(
            event.eventId(),
            event.eventType(),
            now
        );
        if (insertedEvents == 0) {
            return;
        }

        Optional<ReservationEntity> reservationOpt = reservationRepository.findById(event.orderId());
        if (reservationOpt.isEmpty()) {
            ReservationEntity tombstone = new ReservationEntity(
                event.orderId(),
                event.sku(),
                0,
                ReservationStatus.CANCELLED,
                "ORDER_ALREADY_CANCELLED",
                now,
                now
            );
            reservationRepository.save(tombstone);
            return;
        }

        ReservationEntity reservation = reservationOpt.get();
        if (reservation.getStatus() == ReservationStatus.HELD) {
            inventoryRepository.releaseStock(
                event.sku(),
                reservation.getQuantity(),
                now
            );
            reservation.setStatus(ReservationStatus.RELEASED);
            reservation.setUpdatedAt(now);
            reservationRepository.save(reservation);
        }
    }

    private String writePayload(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize event payload", e);
        }
    }
}
