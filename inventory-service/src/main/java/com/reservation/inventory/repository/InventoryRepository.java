package com.reservation.inventory.repository;

import com.reservation.inventory.model.Inventory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;

public interface InventoryRepository extends JpaRepository<Inventory, String> {

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
        UPDATE Inventory inventory
        SET inventory.reservedStock = inventory.reservedStock + :quantity,
            inventory.version = inventory.version + 1,
            inventory.updatedAt = :updatedAt
        WHERE inventory.sku = :sku
          AND inventory.totalStock - inventory.reservedStock >= :quantity
        """)
    int reserveStockIfAvailable(
        @Param("sku") String sku,
        @Param("quantity") int quantity,
        @Param("updatedAt") Instant updatedAt
    );
}
