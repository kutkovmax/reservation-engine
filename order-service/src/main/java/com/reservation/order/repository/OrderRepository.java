package com.reservation.order.repository;

import com.reservation.order.model.OrderEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface OrderRepository extends JpaRepository<OrderEntity, UUID> {

    Optional<OrderEntity> findByIdempotencyKey(String idempotencyKey);

    @Modifying
    @Query(
        value = """
            INSERT INTO orders (
                id, idempotency_key, request_hash, sku, quantity, status,
                aggregate_version, created_at, updated_at
            )
            VALUES (
                :id, :idempotencyKey, :requestHash, :sku, :quantity, 'PENDING',
                1, :createdAt, :createdAt
            )
            ON CONFLICT (idempotency_key) DO NOTHING
            """,
        nativeQuery = true
    )
    int insertPendingIfAbsent(
        @Param("id") UUID id,
        @Param("idempotencyKey") String idempotencyKey,
        @Param("requestHash") String requestHash,
        @Param("sku") String sku,
        @Param("quantity") int quantity,
        @Param("createdAt") Instant createdAt
    );
}
