package com.reservation.order.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.reservation.order.model.OutboxMessageEntity;
import com.reservation.order.repository.OutboxRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxPublisherTest {

    @Mock
    private OutboxRepository outboxRepository;

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    private OutboxPublisher outboxPublisher;

    @BeforeEach
    void setUp() {
        outboxPublisher = new OutboxPublisher(outboxRepository, kafkaTemplate, new ObjectMapper(), 50);
    }

    @Test
    void publishBatchPublishesUnpublishedMessagesAndMarksThemPublished() {
        UUID messageId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        String payloadJson = "{\"eventId\":\"" + eventId + "\",\"orderId\":\"" + orderId + "\",\"sku\":\"BOOK-001\",\"quantity\":2}";

        OutboxMessageEntity message = new OutboxMessageEntity(
            messageId,
            eventId,
            orderId,
            1L,
            "order-events",
            "BOOK-001",
            "OrderPlaced",
            payloadJson,
            Instant.now()
        );

        when(outboxRepository.findUnpublishedForUpdate(50)).thenReturn(List.of(message));
        CompletableFuture<SendResult<String, Object>> future = CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(eq("order-events"), eq("BOOK-001"), any(JsonNode.class))).thenReturn(future);

        int publishedCount = outboxPublisher.publishBatch();

        assertThat(publishedCount).isEqualTo(1);

        ArgumentCaptor<JsonNode> payloadCaptor = ArgumentCaptor.forClass(JsonNode.class);
        verify(kafkaTemplate).send(eq("order-events"), eq("BOOK-001"), payloadCaptor.capture());
        assertThat(payloadCaptor.getValue().get("sku").asText()).isEqualTo("BOOK-001");
        assertThat(payloadCaptor.getValue().get("quantity").asInt()).isEqualTo(2);

        verify(outboxRepository).markPublished(eq(messageId), any(Instant.class));
        verify(outboxRepository, never()).recordFailure(any(), any());
    }

    @Test
    void publishBatchReturnsZeroWhenNoUnpublishedMessages() {
        when(outboxRepository.findUnpublishedForUpdate(50)).thenReturn(Collections.emptyList());

        int publishedCount = outboxPublisher.publishBatch();

        assertThat(publishedCount).isEqualTo(0);
        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    void publishBatchRecordsFailureWhenKafkaSendFails() {
        UUID messageId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        String payloadJson = "{\"eventId\":\"" + eventId + "\"}";

        OutboxMessageEntity message = new OutboxMessageEntity(
            messageId,
            eventId,
            orderId,
            1L,
            "order-events",
            "BOOK-001",
            "OrderPlaced",
            payloadJson,
            Instant.now()
        );

        when(outboxRepository.findUnpublishedForUpdate(50)).thenReturn(List.of(message));
        CompletableFuture<SendResult<String, Object>> failedFuture = CompletableFuture.failedFuture(
            new RuntimeException("Kafka broker unreachable")
        );
        when(kafkaTemplate.send(eq("order-events"), eq("BOOK-001"), any(JsonNode.class))).thenReturn(failedFuture);

        int publishedCount = outboxPublisher.publishBatch();

        assertThat(publishedCount).isEqualTo(0);
        verify(outboxRepository).recordFailure(eq(messageId), eq("java.lang.RuntimeException: Kafka broker unreachable"));
        verify(outboxRepository, never()).markPublished(any(), any());
    }
}
