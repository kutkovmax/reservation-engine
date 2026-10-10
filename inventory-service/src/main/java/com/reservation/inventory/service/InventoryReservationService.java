package com.reservation.inventory.service;

import com.reservation.inventory.event.OrderPlacedEvent;
import com.reservation.inventory.model.Inventory;
import com.reservation.inventory.repository.InventoryRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;

@Service
public class InventoryReservationService {

    private final InventoryRepository inventoryRepository;

    public InventoryReservationService(InventoryRepository inventoryRepository) {
        this.inventoryRepository = inventoryRepository;
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
}
