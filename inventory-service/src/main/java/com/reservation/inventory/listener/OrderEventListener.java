package com.reservation.inventory.listener;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.reservation.inventory.event.OrderCancelledEvent;
import com.reservation.inventory.event.OrderPlacedEvent;
import com.reservation.inventory.service.InventoryReservationService;
import com.reservation.inventory.service.ReservationOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);
    private static final String ORDER_EVENTS_TOPIC = "order-events";

    private final InventoryReservationService reservationService;
    private final ObjectMapper objectMapper;
    private final boolean simulateCrashAfterReservation;
    private final Set<UUID> crashedEventIds = ConcurrentHashMap.newKeySet();

    @Autowired
    public OrderEventListener(
        InventoryReservationService reservationService,
        ObjectMapper objectMapper,
        @Value("${reservation.simulate-crash-after-reservation:false}") boolean simulateCrashAfterReservation
    ) {
        this.reservationService = reservationService;
        this.objectMapper = objectMapper;
        this.simulateCrashAfterReservation = simulateCrashAfterReservation;
    }

    public OrderEventListener(
        InventoryReservationService reservationService,
        boolean simulateCrashAfterReservation
    ) {
        this(
            reservationService,
            new ObjectMapper().registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule()),
            simulateCrashAfterReservation
        );
    }

    @KafkaListener(topics = ORDER_EVENTS_TOPIC)
    public void handleOrderEvent(String payload) {
        try {
            JsonNode root = objectMapper.readTree(payload);
            String eventType = root.path("eventType").asText();

            switch (eventType) {
                case "OrderPlaced" -> {
                    OrderPlacedEvent event = objectMapper.treeToValue(root, OrderPlacedEvent.class);
                    handleOrderPlaced(event);
                }
                case OrderCancelledEvent.EVENT_TYPE -> {
                    OrderCancelledEvent event = objectMapper.treeToValue(root, OrderCancelledEvent.class);
                    handleOrderCancelled(event);
                }
                default -> {
                    log.warn("Неизвестный тип события из {}: eventType={}", ORDER_EVENTS_TOPIC, eventType);
                    throw new IllegalArgumentException("Unknown or unsupported eventType: " + eventType);
                }
            }
        } catch (Exception exception) {
            log.error("Ошибка при обработке сообщения из топика {}: {}", ORDER_EVENTS_TOPIC, exception.getMessage(), exception);
            throw new IllegalStateException("Failed to process order event", exception);
        }
    }

    public void handleOrderPlaced(OrderPlacedEvent event) {
        log.info(
            "Получено событие заказа: eventId={}, orderId={}, sku={}, quantity={}, occurredAt={}",
            event.eventId(),
            event.orderId(),
            event.sku(),
            event.quantity(),
            event.occurredAt()
        );

        ReservationOutcome outcome = reservationService.reserve(event);
        if (
            simulateCrashAfterReservation
                && outcome == ReservationOutcome.RESERVED
                && crashedEventIds.add(event.eventId())
        ) {
            log.warn(
                "Симуляция падения консьюмера после резервирования остатка и до коммита смещения Kafka: eventId={}",
                event.eventId()
            );
            throw new IllegalStateException("Simulated consumer crash after stock reservation");
        }

        switch (outcome) {
            case RESERVED -> log.info(
                "Остаток успешно зарезервирован: orderId={}, sku={}, quantity={}",
                event.orderId(),
                event.sku(),
                event.quantity()
            );
            case DUPLICATE -> log.info(
                "Пропуск уже обработанного события заказа (дубликат): eventId={}, orderId={}",
                event.eventId(),
                event.orderId()
            );
            case SKU_NOT_FOUND -> log.warn(
                "Невозможно зарезервировать неизвестный SKU: orderId={}, sku={}",
                event.orderId(),
                event.sku()
            );
            case INSUFFICIENT_STOCK -> log.info(
                "Недостаточно товара на складе: orderId={}, sku={}, quantity={}",
                event.orderId(),
                event.sku(),
                event.quantity()
            );
            case ORDER_ALREADY_CANCELLED -> log.info(
                "Заказ уже был отменен до размещения (tombstone CANCELLED): orderId={}, sku={}",
                event.orderId(),
                event.sku()
            );
        }
    }

    public void handleOrderCancelled(OrderCancelledEvent event) {
        log.info(
            "Получено событие отмены заказа: eventId={}, orderId={}, sku={}, reason={}",
            event.eventId(),
            event.orderId(),
            event.sku(),
            event.reason()
        );
        reservationService.cancel(event);
    }
}
