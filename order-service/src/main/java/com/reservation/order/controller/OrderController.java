package com.reservation.order.controller;

import com.reservation.order.dto.CreateOrderRequest;
import com.reservation.order.dto.OrderResponse;
import com.reservation.order.event.OrderPlacedEvent;
import com.reservation.order.service.CreateOrderResult;
import com.reservation.order.service.OrderService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);
    private static final String ORDER_EVENTS_TOPIC = "order-events";
    private static final String ORDER_PLACED_EVENT_TYPE = "OrderPlaced";
    private static final int ORDER_PLACED_EVENT_VERSION = 1;

    private final OrderService orderService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderController(
        OrderService orderService,
        KafkaTemplate<String, Object> kafkaTemplate
    ) {
        this.orderService = orderService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public ResponseEntity<OrderResponse> create(
        @RequestHeader("Idempotency-Key") String idempotencyKey,
        @Valid @RequestBody CreateOrderRequest request
    ) {
        CreateOrderResult result = orderService.create(idempotencyKey, request);
        if (result.created()) {
            publishOrderPlaced(result.order());
        }

        HttpStatus status = result.created() ? HttpStatus.ACCEPTED : HttpStatus.OK;
        return ResponseEntity.status(status).body(result.order());
    }

    @GetMapping("/{orderId}")
    public OrderResponse get(@PathVariable UUID orderId) {
        return orderService.get(orderId);
    }

    private void publishOrderPlaced(OrderResponse order) {
        OrderPlacedEvent event = new OrderPlacedEvent(
            UUID.randomUUID(),
            ORDER_PLACED_EVENT_TYPE,
            ORDER_PLACED_EVENT_VERSION,
            1,
            order.orderId(),
            order.sku(),
            order.quantity(),
            order.createdAt()
        );

        log.info(
            "Publishing order event: eventId={}, orderId={}, sku={}, quantity={}",
            event.eventId(),
            event.orderId(),
            event.sku(),
            event.quantity()
        );
        kafkaTemplate.send(ORDER_EVENTS_TOPIC, order.sku(), event);
    }
}
