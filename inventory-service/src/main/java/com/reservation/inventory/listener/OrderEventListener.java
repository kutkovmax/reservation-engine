package com.reservation.inventory.listener;

import com.reservation.inventory.event.OrderPlacedEvent;
import com.reservation.inventory.service.InventoryReservationService;
import com.reservation.inventory.service.ReservationOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private final boolean simulateCrashAfterReservation;
    private final Set<UUID> crashedEventIds = ConcurrentHashMap.newKeySet();

    public OrderEventListener(
        InventoryReservationService reservationService,
        @Value("${reservation.simulate-crash-after-reservation:false}") boolean simulateCrashAfterReservation
    ) {
        this.reservationService = reservationService;
        this.simulateCrashAfterReservation = simulateCrashAfterReservation;
    }

    @KafkaListener(topics = ORDER_EVENTS_TOPIC)
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
        }
    }
}
