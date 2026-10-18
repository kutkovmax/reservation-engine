package com.reservation.order.service;

import com.reservation.order.dto.CreateOrderRequest;
import com.reservation.order.dto.OrderResponse;
import com.reservation.order.exception.IdempotencyKeyReusedException;
import com.reservation.order.exception.OrderNotFoundException;
import com.reservation.order.exception.RequestValidationException;
import com.reservation.order.model.OrderStatus;
import com.reservation.order.repository.OrderRepository;
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

    private static final Pattern SKU_PATTERN = Pattern.compile("[A-Z0-9._-]{1,64}");

    private final OrderRepository orderRepository;

    public OrderService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
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

        return new CreateOrderResult(OrderResponse.from(existingOrder), false);
    }

    @Transactional(readOnly = true)
    public OrderResponse get(UUID orderId) {
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
}
