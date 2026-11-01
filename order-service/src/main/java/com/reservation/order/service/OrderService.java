package com.reservation.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.reservation.order.dto.CreateOrderRequest;
import com.reservation.order.dto.OrderResponse;
import com.reservation.order.event.OrderCancelledEvent;
import com.reservation.order.event.OrderPlacedEvent;
import com.reservation.order.event.InventoryReservationFailedEvent;
import com.reservation.order.event.InventoryReservedEvent;
import com.reservation.order.exception.IdempotencyKeyReusedException;
import com.reservation.order.exception.OrderNotFoundException;
import com.reservation.order.exception.RequestValidationException;
import com.reservation.order.model.OrderEntity;
import com.reservation.order.model.OrderStatus;
import com.reservation.order.repository.OrderRepository;
import com.reservation.order.repository.OutboxRepository;
import com.reservation.order.repository.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
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
    private final ProcessedEventRepository processedEventRepository;
    private final ObjectMapper objectMapper;
    private final Duration holdTtl;

    @org.springframework.beans.factory.annotation.Autowired
    public OrderService(
        OrderRepository orderRepository,
        OutboxRepository outboxRepository,
        ProcessedEventRepository processedEventRepository,
        ObjectMapper objectMapper,
        @Value("${reservation.hold-ttl:15m}") Duration holdTtl
    ) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.processedEventRepository = processedEventRepository;
        this.objectMapper = objectMapper;
        this.holdTtl = holdTtl;
    }

    public OrderService(
        OrderRepository orderRepository,
        OutboxRepository outboxRepository,
        ProcessedEventRepository processedEventRepository,
        ObjectMapper objectMapper
    ) {
        this(orderRepository, outboxRepository, processedEventRepository, objectMapper, Duration.ofMinutes(15));
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

    @Transactional
    public void handleInventoryReserved(InventoryReservedEvent event) {
        Instant now = Instant.now();
        int inserted = processedEventRepository.insertIfAbsent(
            event.eventId(),
            event.eventType(),
            now
        );
        if (inserted == 0) {
            log.info(
                "Пропуск уже обработанного события inventory (дубликат): eventId={}, orderId={}",
                event.eventId(),
                event.orderId()
            );
            return;
        }

        Optional<OrderEntity> orderOpt = orderRepository.findById(event.orderId());
        if (orderOpt.isEmpty()) {
            log.warn(
                "Заказ для события InventoryReserved не найден: orderId={}, eventId={}",
                event.orderId(),
                event.eventId()
            );
            return;
        }

        OrderEntity order = orderOpt.get();
        if (order.getStatus() != OrderStatus.PENDING) {
            log.info(
                "Поздний результат inventory для заказа не в статусе PENDING (проигнорировано): orderId={}, currentStatus={}, eventId={}",
                order.getId(),
                order.getStatus(),
                event.eventId()
            );
            return;
        }

        Instant expiresAt = now.plus(holdTtl);
        order.markReserved(now, expiresAt);
        orderRepository.save(order);
        log.info(
            "Заказ переведен в статус RESERVED: orderId={}, expiresAt={}",
            order.getId(),
            expiresAt
        );
    }

    @Transactional
    public void handleInventoryReservationFailed(InventoryReservationFailedEvent event) {
        Instant now = Instant.now();
        int inserted = processedEventRepository.insertIfAbsent(
            event.eventId(),
            event.eventType(),
            now
        );
        if (inserted == 0) {
            log.info(
                "Пропуск уже обработанного события inventory (дубликат): eventId={}, orderId={}",
                event.eventId(),
                event.orderId()
            );
            return;
        }

        Optional<OrderEntity> orderOpt = orderRepository.findById(event.orderId());
        if (orderOpt.isEmpty()) {
            log.warn(
                "Заказ для события InventoryReservationFailed не найден: orderId={}, eventId={}",
                event.orderId(),
                event.eventId()
            );
            return;
        }

        OrderEntity order = orderOpt.get();
        if (order.getStatus() != OrderStatus.PENDING) {
            log.info(
                "Поздний результат inventory для заказа не в статусе PENDING (проигнорировано): orderId={}, currentStatus={}, eventId={}",
                order.getId(),
                order.getStatus(),
                event.eventId()
            );
            return;
        }

        order.markRejected(event.reason(), now);
        orderRepository.save(order);
        log.info(
            "Заказ переведен в статус REJECTED: orderId={}, reason={}",
            order.getId(),
            event.reason()
        );
    }

    @Transactional
    public OrderResponse cancel(UUID orderId) {
        log.info("Получен запрос на отмену заказа: orderId={}", orderId);
        OrderEntity order = orderRepository.findById(orderId)
            .orElseThrow(() -> new OrderNotFoundException(orderId));

        if (order.getStatus() == OrderStatus.CANCELLED) {
            log.info("Повторный запрос отмены для уже отмененного заказа: orderId={}", orderId);
            return OrderResponse.from(order);
        }

        if (order.getStatus() != OrderStatus.PENDING && order.getStatus() != OrderStatus.RESERVED) {
            log.info(
                "Запрос отмены для терминального заказа (статус не изменен): orderId={}, currentStatus={}",
                orderId,
                order.getStatus()
            );
            return OrderResponse.from(order);
        }

        Instant now = Instant.now();
        order.markCancelled(now);
        orderRepository.save(order);

        UUID eventId = UUID.randomUUID();
        OrderCancelledEvent event = new OrderCancelledEvent(
            eventId,
            order.getAggregateVersion(),
            order.getId(),
            order.getSku(),
            "USER_REQUESTED",
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

        log.info(
            "Заказ успешно отменен и событие OrderCancelled сохранено в outbox: orderId={}, aggregateVersion={}",
            order.getId(),
            order.getAggregateVersion()
        );

        return OrderResponse.from(order);
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

