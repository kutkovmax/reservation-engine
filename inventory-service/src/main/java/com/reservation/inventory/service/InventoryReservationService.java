package com.reservation.inventory.service;

import com.reservation.inventory.event.OrderPlacedEvent;
import com.reservation.inventory.repository.InventoryRepository;
import com.reservation.inventory.repository.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
public class InventoryReservationService {

    private static final String ORDER_PLACED_EVENT_TYPE = "OrderPlaced";

    private final InventoryRepository inventoryRepository;
    private final ProcessedEventRepository processedEventRepository;

    public InventoryReservationService(
        InventoryRepository inventoryRepository,
        ProcessedEventRepository processedEventRepository
    ) {
        this.inventoryRepository = inventoryRepository;
        this.processedEventRepository = processedEventRepository;
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

        int updatedRows = inventoryRepository.reserveStockIfAvailable(
            event.sku(),
            event.quantity(),
            now
        );
        if (updatedRows == 1) {
            return ReservationOutcome.RESERVED;
        }

        return inventoryRepository.existsById(event.sku())
            ? ReservationOutcome.INSUFFICIENT_STOCK
            : ReservationOutcome.SKU_NOT_FOUND;
    }
}
