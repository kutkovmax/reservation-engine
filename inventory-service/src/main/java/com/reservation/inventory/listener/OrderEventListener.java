package com.reservation.inventory.listener;

import com.reservation.inventory.event.OrderPlacedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);
    private static final String ORDER_EVENTS_TOPIC = "order-events";

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
    }
}
