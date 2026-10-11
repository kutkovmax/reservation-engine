package com.reservation.order.controller;

import com.reservation.order.dto.CreateOrderRequest;
import com.reservation.order.event.OrderPlacedEvent;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);
    private static final String TOPIC = "order-events";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderController(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/test")
    public ResponseEntity<Map<String, Object>> sendTestOrder(@Valid @RequestBody CreateOrderRequest request) {
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();

        OrderPlacedEvent event = new OrderPlacedEvent(
            eventId,
            orderId,
            request.sku(),
            request.quantity(),
            Instant.now()
        );

        log.info("Sending test order event to Kafka topic {}: {}", TOPIC, event);
        kafkaTemplate.send(TOPIC, orderId.toString(), event);

        return ResponseEntity.ok(Map.of(
            "status", "SENT",
            "orderId", orderId,
            "eventId", eventId,
            "sku", request.sku(),
            "quantity", request.quantity()
        ));
    }
}
