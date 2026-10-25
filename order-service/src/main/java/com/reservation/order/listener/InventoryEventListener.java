package com.reservation.order.listener;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.reservation.order.event.InventoryReservationFailedEvent;
import com.reservation.order.event.InventoryReservedEvent;
import com.reservation.order.service.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventListener.class);
    private static final String INVENTORY_EVENTS_TOPIC = "inventory-events";

    private final OrderService orderService;
    private final ObjectMapper objectMapper;

    public InventoryEventListener(OrderService orderService, ObjectMapper objectMapper) {
        this.orderService = orderService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = INVENTORY_EVENTS_TOPIC)
    public void handleInventoryEvent(String payload) {
        try {
            JsonNode root = objectMapper.readTree(payload);
            String eventType = root.path("eventType").asText();

            switch (eventType) {
                case InventoryReservedEvent.EVENT_TYPE -> {
                    InventoryReservedEvent event = objectMapper.treeToValue(root, InventoryReservedEvent.class);
                    log.info(
                        "Получено событие резервирования остатка: eventId={}, orderId={}, sku={}, quantity={}",
                        event.eventId(),
                        event.orderId(),
                        event.sku(),
                        event.quantity()
                    );
                    orderService.handleInventoryReserved(event);
                }
                case InventoryReservationFailedEvent.EVENT_TYPE -> {
                    InventoryReservationFailedEvent event = objectMapper.treeToValue(root, InventoryReservationFailedEvent.class);
                    log.info(
                        "Получено событие отказа в резервировании: eventId={}, orderId={}, reason={}, sku={}",
                        event.eventId(),
                        event.orderId(),
                        event.reason(),
                        event.sku()
                    );
                    orderService.handleInventoryReservationFailed(event);
                }
                default -> log.warn("Пропуск неизвестного типа события inventory: eventType={}", eventType);
            }
        } catch (Exception exception) {
            log.error("Ошибка при обработке события из топика {}: {}", INVENTORY_EVENTS_TOPIC, exception.getMessage(), exception);
            throw new IllegalStateException("Failed to process inventory event", exception);
        }
    }
}
