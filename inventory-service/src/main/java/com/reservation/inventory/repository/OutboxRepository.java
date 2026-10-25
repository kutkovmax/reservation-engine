package com.reservation.inventory.repository;

import com.reservation.inventory.model.OutboxMessageEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface OutboxRepository extends JpaRepository<OutboxMessageEntity, UUID> {

    @Modifying
    @Query(
        value = """
            INSERT INTO outbox_messages (
                id, event_id, aggregate_id, aggregate_version,
                topic, message_key, event_type, payload,
                created_at, attempts
            )
            VALUES (
                :id, :eventId, :aggregateId, :aggregateVersion,
                :topic, :messageKey, :eventType, CAST(:payload AS jsonb),
                :createdAt, 0
            )
            """,
        nativeQuery = true
    )
    void insertMessage(
        @Param("id") UUID id,
        @Param("eventId") UUID eventId,
        @Param("aggregateId") UUID aggregateId,
        @Param("aggregateVersion") long aggregateVersion,
        @Param("topic") String topic,
        @Param("messageKey") String messageKey,
        @Param("eventType") String eventType,
        @Param("payload") String payload,
        @Param("createdAt") Instant createdAt
    );

    @Query(
        value = """
            SELECT * FROM outbox_messages
            WHERE published_at IS NULL
            ORDER BY created_at ASC, aggregate_version ASC
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """,
        nativeQuery = true
    )
    List<OutboxMessageEntity> findUnpublishedForUpdate(@Param("batchSize") int batchSize);

    @Modifying
    @Query(
        value = """
            UPDATE outbox_messages
            SET published_at = :publishedAt,
                last_error = NULL
            WHERE id = :id
            """,
        nativeQuery = true
    )
    int markPublished(@Param("id") UUID id, @Param("publishedAt") Instant publishedAt);

    @Modifying
    @Query(
        value = """
            UPDATE outbox_messages
            SET attempts = attempts + 1,
                last_error = :lastError
            WHERE id = :id
            """,
        nativeQuery = true
    )
    int recordFailure(@Param("id") UUID id, @Param("lastError") String lastError);
}
