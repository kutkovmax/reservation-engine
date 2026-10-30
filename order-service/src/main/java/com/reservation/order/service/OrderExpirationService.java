package com.reservation.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.reservation.order.event.OrderCancelledEvent;
import com.reservation.order.model.OrderEntity;
import com.reservation.order.repository.OrderRepository;
import com.reservation.order.repository.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
public class OrderExpirationService {

    private static final Logger log = LoggerFactory.getLogger(OrderExpirationService.class);
    private static final String ORDER_EVENTS_TOPIC = "order-events";
    private static final String HOLD_EXPIRED_REASON = "HOLD_EXPIRED";

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final int batchSize;

    public OrderExpirationService(
        OrderRepository orderRepository,
        OutboxRepository outboxRepository,
        ObjectMapper objectMapper,
        @Value("${reservation.expiration-batch-size:50}") int batchSize
    ) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${reservation.expiration-scan-interval-ms:30000}")
    public void scanAndExpire() {
        expireBatch();
    }

    @Transactional
    public int expireBatch() {
        Instant now = Instant.now();
        List<OrderEntity> expiredOrders = orderRepository.findExpiredOrdersForUpdate(now, batchSize);
        if (expiredOrders.isEmpty()) {
            return 0;
        }

        int expiredCount = 0;
        for (OrderEntity order : expiredOrders) {
            order.markExpired(now);
            orderRepository.save(order);

            UUID eventId = UUID.randomUUID();
            OrderCancelledEvent event = new OrderCancelledEvent(
                eventId,
                order.getAggregateVersion(),
                order.getId(),
                order.getSku(),
                HOLD_EXPIRED_REASON,
                now
            );

            String payload = writePayloadAsJson(event);
            outboxRepository.insertMessage(
                UUID.randomUUID(),
                eventId,
                order.getId(),
                order.getAggregateVersion(),
                ORDER_EVENTS_TOPIC,
                order.getSku(),
                OrderCancelledEvent.EVENT_TYPE,
                payload,
                now
            );

            expiredCount++;
            log.info(
                "Заказ переведен в статус EXPIRED по таймауту удержания и событие OrderCancelled сохранено в outbox: orderId={}, expiresAt={}, aggregateVersion={}",
                order.getId(),
                order.getExpiresAt(),
                order.getAggregateVersion()
            );
        }

        return expiredCount;
    }

    private String writePayloadAsJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Failed to serialize outbox event payload", exception);
        }
    }
}
