package com.reservation.inventory.service;

import com.reservation.inventory.event.OrderPlacedEvent;
import com.reservation.inventory.model.Inventory;
import com.reservation.inventory.repository.InventoryRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

@Service
public class InventoryReservationService {

    private final InventoryRepository inventoryRepository;
    private final Duration processingDelay;

    public InventoryReservationService(
        InventoryRepository inventoryRepository,
        @Value("${reservation.processing-delay:0ms}") Duration processingDelay
    ) {
        this.inventoryRepository = inventoryRepository;
        this.processingDelay = processingDelay;
    }

    @Transactional
    public ReservationOutcome reserve(OrderPlacedEvent event) {
        if (event.quantity() < 1) {
            throw new IllegalArgumentException("Reservation quantity must be positive");
        }

        Optional<Inventory> inventory = inventoryRepository.findById(event.sku());
        if (inventory.isEmpty()) {
            return ReservationOutcome.SKU_NOT_FOUND;
        }

        Inventory stock = inventory.get();
        int availableStock = stock.getTotalStock() - stock.getReservedStock();
        if (availableStock < event.quantity()) {
            return ReservationOutcome.INSUFFICIENT_STOCK;
        }

        pauseBeforeUpdate();

        int updatedRows = inventoryRepository.incrementReservedStock(
            event.sku(),
            event.quantity(),
            Instant.now()
        );
        if (updatedRows != 1) {
            throw new IllegalStateException("Inventory row disappeared during reservation");
        }

        return ReservationOutcome.RESERVED;
    }

    private void pauseBeforeUpdate() {
        if (processingDelay.isZero() || processingDelay.isNegative()) {
            return;
        }

        try {
            Thread.sleep(processingDelay);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Reservation processing was interrupted", exception);
        }
    }
}
