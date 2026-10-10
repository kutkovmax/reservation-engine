package com.reservation.inventory.listener;

import com.reservation.inventory.event.OrderPlacedEvent;
import com.reservation.inventory.service.InventoryReservationService;
import com.reservation.inventory.service.ReservationOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);
    private static final String ORDER_EVENTS_TOPIC = "order-events";

    private final InventoryReservationService reservationService;

    public OrderEventListener(InventoryReservationService reservationService) {
        this.reservationService = reservationService;
    }

    @KafkaListener(topics = ORDER_EVENTS_TOPIC)
    public void handleOrderPlaced(OrderPlacedEvent event) {
        log.info(
            "Received order event: eventId={}, orderId={}, sku={}, quantity={}, createdAt={}",
            event.eventId(),
            event.orderId(),
            event.sku(),
            event.quantity(),
            event.createdAt()
        );

        ReservationOutcome outcome = reservationService.reserve(event);
        switch (outcome) {
            case RESERVED -> log.info(
                "Reserved stock: orderId={}, sku={}, quantity={}",
                event.orderId(),
                event.sku(),
                event.quantity()
            );
            case SKU_NOT_FOUND -> log.warn(
                "Cannot reserve unknown SKU: orderId={}, sku={}",
                event.orderId(),
                event.sku()
            );
            case INSUFFICIENT_STOCK -> log.info(
                "Insufficient stock: orderId={}, sku={}, quantity={}",
                event.orderId(),
                event.sku(),
                event.quantity()
            );
        }
    }
}
