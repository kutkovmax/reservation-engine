package com.reservation.inventory.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.reservation.inventory.model.OutboxMessageEntity;
import com.reservation.inventory.repository.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final int batchSize;

    public OutboxPublisher(
        OutboxRepository outboxRepository,
        KafkaTemplate<String, Object> kafkaTemplate,
        ObjectMapper objectMapper,
        @Value("${reservation.outbox.batch-size:50}") int batchSize
    ) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${reservation.outbox.poll-interval-ms:500}")
    @Transactional
    public void publishScheduled() {
        publishBatch();
    }

    @Transactional
    public int publishBatch() {
        List<OutboxMessageEntity> messages = outboxRepository.findUnpublishedForUpdate(batchSize);
        if (messages.isEmpty()) {
            return 0;
        }

        int publishedCount = 0;
        for (OutboxMessageEntity message : messages) {
            try {
                JsonNode payload = objectMapper.readTree(message.getPayload());
                kafkaTemplate.send(message.getTopic(), message.getMessageKey(), payload)
                    .get(5, TimeUnit.SECONDS);

                outboxRepository.markPublished(message.getId(), Instant.now());
                publishedCount++;
                log.info(
                    "Опубликовано событие остатков из outbox в Kafka: eventId={}, topic={}, key={}",
                    message.getEventId(),
                    message.getTopic(),
                    message.getMessageKey()
                );
            } catch (Exception exception) {
                String error = exception.getMessage() != null
                    ? exception.getMessage()
                    : exception.getClass().getSimpleName();
                outboxRepository.recordFailure(message.getId(), error);
                log.warn(
                    "Ошибка публикации события остатков из outbox в Kafka: eventId={}, topic={}, попытка={}",
                    message.getEventId(),
                    message.getTopic(),
                    message.getAttempts() + 1,
                    exception
                );
            }
        }
        return publishedCount;
    }
}
