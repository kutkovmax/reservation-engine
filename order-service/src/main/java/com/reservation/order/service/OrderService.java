package com.reservation.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.reservation.order.dto.CreateOrderRequest;
import com.reservation.order.dto.OrderResponse;
import com.reservation.order.event.OrderPlacedEvent;
import com.reservation.order.exception.IdempotencyKeyReusedException;
import com.reservation.order.exception.OrderNotFoundException;
import com.reservation.order.exception.RequestValidationException;
import com.reservation.order.model.OrderStatus;
import com.reservation.order.repository.OrderRepository;
import com.reservation.order.repository.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);
    private static final String ORDER_EVENTS_TOPIC = "order-events";
    private static final String ORDER_PLACED_EVENT_TYPE = "OrderPlaced";
    private static final int ORDER_PLACED_EVENT_VERSION = 1;
    private static final Pattern SKU_PATTERN = Pattern.compile("[A-Z0-9._-]{1,64}");


    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OrderService(
        OrderRepository orderRepository,
        OutboxRepository outboxRepository,
        ObjectMapper objectMapper
    ) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public CreateOrderResult create(String idempotencyKey, CreateOrderRequest request) {
        validateIdempotencyKey(idempotencyKey);
        String sku = normalizeSku(request.sku());
        int quantity = validateQuantity(request.quantity());
        String requestHash = requestHash(sku, quantity);

        UUID orderId = UUID.randomUUID();
        Instant now = Instant.now();
        int insertedRows = orderRepository.insertPendingIfAbsent(
            orderId,
            idempotencyKey,
            requestHash,
            sku,
            quantity,
            now
        );

        if (insertedRows == 1) {
            UUID eventId = UUID.randomUUID();
            OrderPlacedEvent event = new OrderPlacedEvent(
                eventId,
                ORDER_PLACED_EVENT_TYPE,
                ORDER_PLACED_EVENT_VERSION,
                1L,
                orderId,
                sku,
                quantity,
                now
            );

            String payload = writePayloadAsJson(event);
            outboxRepository.insertMessage(
                UUID.randomUUID(),
                eventId,
                orderId,
                1L,
                ORDER_EVENTS_TOPIC,
                sku,
                ORDER_PLACED_EVENT_TYPE,
                payload,
                now
            );

            log.info(
                "Создан заказ в статусе PENDING и сохранено событие в outbox: orderId={}, sku={}, quantity={}",
                orderId,
                sku,
                quantity
            );

            return new CreateOrderResult(
                new OrderResponse(
                    orderId,
                    sku,
                    quantity,
                    OrderStatus.PENDING,
                    null,
                    now,
                    now,
                    null,
                    null
                ),
                true
            );
        }

        var existingOrder = orderRepository.findByIdempotencyKey(idempotencyKey)
            .orElseThrow(() -> new IllegalStateException("Conflicting order was not found"));
        if (!existingOrder.getRequestHash().equals(requestHash)) {
            throw new IdempotencyKeyReusedException();
        }

        log.info(
            "Повторный идемпотентный запрос для заказа: orderId={}, idempotencyKey={}",
            existingOrder.getId(),
            idempotencyKey
        );
        return new CreateOrderResult(OrderResponse.from(existingOrder), false);
    }

    @Transactional(readOnly = true)
    public OrderResponse get(UUID orderId) {
        log.info("Получен запрос на чтение заказа: orderId={}", orderId);
        return orderRepository.findById(orderId)
            .map(OrderResponse::from)
            .orElseThrow(() -> new OrderNotFoundException(orderId));
    }


    static String normalizeSku(String rawSku) {
        if (rawSku == null) {
            throw new RequestValidationException(Map.of("sku", "must not be blank"));
        }

        String normalizedSku = rawSku.trim().toUpperCase(Locale.ROOT);
        if (!SKU_PATTERN.matcher(normalizedSku).matches()) {
            throw new RequestValidationException(Map.of(
                "sku",
                "must contain 1 to 64 characters from A-Z, 0-9, '.', '_' or '-'"
            ));
        }
        return normalizedSku;
    }

    static int validateQuantity(Integer quantity) {
        if (quantity == null || quantity < 1 || quantity > 1_000_000) {
            throw new RequestValidationException(Map.of(
                "quantity",
                "must be between 1 and 1000000"
            ));
        }
        return quantity;
    }

    static String requestHash(String sku, int quantity) {
        String normalizedBody = "{\"quantity\":" + quantity + ",\"sku\":\"" + sku + "\"}";
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(normalizedBody.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is not available", exception);
        }
    }

    private static void validateIdempotencyKey(String idempotencyKey) {
        boolean valid = idempotencyKey != null
            && idempotencyKey.length() >= 1
            && idempotencyKey.length() <= 100
            && idempotencyKey.chars().allMatch(character -> character >= 0x20 && character <= 0x7E);
        if (!valid) {
            throw new RequestValidationException(Map.of(
                "Idempotency-Key",
                "must contain 1 to 100 printable ASCII characters"
            ));
        }
    }

    private String writePayloadAsJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Failed to serialize outbox event payload", exception);
        }
    }
}

