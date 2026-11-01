package com.reservation.inventory.repository;

import com.reservation.inventory.model.ProcessedEvent;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.UUID;

public interface ProcessedEventRepository extends Repository<ProcessedEvent, UUID> {

    @Modifying
    @Query(
        value = """
            INSERT INTO processed_events (event_id, event_type, processed_at)
            VALUES (:eventId, :eventType, :processedAt)
            ON CONFLICT (event_id) DO NOTHING
            """,
        nativeQuery = true
    )
    int insertIfAbsent(
        @Param("eventId") UUID eventId,
        @Param("eventType") String eventType,
        @Param("processedAt") Instant processedAt
    );

    boolean existsById(UUID eventId);
}
