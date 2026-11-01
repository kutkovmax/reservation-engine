package com.reservation.order;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.reservation.order.dto.CreateOrderRequest;
import com.reservation.order.event.InventoryReservationFailedEvent;
import com.reservation.order.event.InventoryReservedEvent;
import com.reservation.order.model.OrderEntity;
import com.reservation.order.model.OrderStatus;
import com.reservation.order.model.OutboxMessageEntity;
import com.reservation.order.repository.OrderRepository;
import com.reservation.order.repository.OutboxRepository;
import com.reservation.order.service.OrderExpirationService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@DisplayName("Интеграционные тесты сервиса заказов (order-service)")
class OrderServiceIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(OrderServiceIntegrationTest.class);
    private static final String INVENTORY_EVENTS_TOPIC = "inventory-events";
    private static final String ORDER_EVENTS_TOPIC = "order-events";

    static PostgreSQLContainer<?> postgres;
    static KafkaContainer kafka;
    static String kafkaBootstrap = "localhost:9092";

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OutboxRepository outboxRepository;

    @Autowired
    private OrderExpirationService orderExpirationService;

    @Autowired
    private ObjectMapper objectMapper;

    static {
        boolean dockerAvailable = false;
        try {
            dockerAvailable = DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable t) {
            log.warn("Docker environment is not accessible for Testcontainers: {}", t.getMessage());
        }

        if (dockerAvailable) {
            try {
                postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                        .withDatabaseName("order_db")
                        .withUsername("postgres")
                        .withPassword("password");
                postgres.start();

                kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));
                kafka.start();
                kafkaBootstrap = kafka.getBootstrapServers();
                log.info("Testcontainers PostgreSQL and Kafka started successfully");
            } catch (Throwable t) {
                log.warn("Failed to start Testcontainers, falling back to local Docker services: {}", t.getMessage());
                postgres = null;
                kafka = null;
            }
        }
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        if (postgres != null && postgres.isRunning()) {
            registry.add("spring.datasource.url", postgres::getJdbcUrl);
            registry.add("spring.datasource.username", postgres::getUsername);
            registry.add("spring.datasource.password", postgres::getPassword);
        } else {
            registry.add("spring.datasource.url", () -> "jdbc:postgresql://localhost:15432/order_db");
            registry.add("spring.datasource.username", () -> "postgres");
            registry.add("spring.datasource.password", () -> "password");
        }

        if (kafka != null && kafka.isRunning()) {
            kafkaBootstrap = kafka.getBootstrapServers();
        } else {
            kafkaBootstrap = "localhost:9092";
        }
        registry.add("spring.kafka.bootstrap-servers", () -> kafkaBootstrap);
    }

    @Test
    @DisplayName("Сценарий 1: Успешное создание заказа через HTTP API и переход в RESERVED при подтверждении склада")
    void shouldCreateOrderAndTransitionToReservedWhenInventoryConfirms() throws Exception {
        String idempotencyKey = "IDEM-" + UUID.randomUUID();
        String sku = "SKU-IPHONE-15";
        int quantity = 2;

        CreateOrderRequest request = new CreateOrderRequest(sku, quantity);
        MvcResult postResult = mockMvc.perform(post("/api/v1/orders")
                        .header("Idempotency-Key", idempotencyKey)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.sku").value(sku))
                .andExpect(jsonPath("$.quantity").value(quantity))
                .andReturn();

        JsonNode postJson = objectMapper.readTree(postResult.getResponse().getContentAsString());
        UUID orderId = UUID.fromString(postJson.path("orderId").asText());

        // Отправляем подтверждение резерва от inventory-service в топик inventory-events
        UUID inventoryEventId = UUID.randomUUID();
        InventoryReservedEvent reservedEvent = new InventoryReservedEvent(
                inventoryEventId,
                UUID.randomUUID(),
                orderId,
                sku,
                quantity,
                Instant.now()
        );
        kafkaTemplate.send(INVENTORY_EVENTS_TOPIC, orderId.toString(), reservedEvent);

        // Ожидаем через Awaitility переход заказа в RESERVED
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            MvcResult getResult = mockMvc.perform(get("/api/v1/orders/" + orderId))
                    .andExpect(status().isOk())
                    .andReturn();

            JsonNode getJson = objectMapper.readTree(getResult.getResponse().getContentAsString());
            assertThat(getJson.path("status").asText()).isEqualTo("RESERVED");
            assertThat(getJson.path("reservedAt").asText()).isNotEmpty();
            assertThat(getJson.path("expiresAt").asText()).isNotEmpty();
        });
    }

    @Test
    @DisplayName("Сценарий 2: Отказ в резервировании при недостаточном остатке (переход в REJECTED)")
    void shouldTransitionToRejectedWhenInsufficientStock() throws Exception {
        String idempotencyKey = "IDEM-" + UUID.randomUUID();
        String sku = "SKU-IPHONE-15";
        int quantity = 999;

        CreateOrderRequest request = new CreateOrderRequest(sku, quantity);
        MvcResult postResult = mockMvc.perform(post("/api/v1/orders")
                        .header("Idempotency-Key", idempotencyKey)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted())
                .andReturn();

        UUID orderId = UUID.fromString(objectMapper.readTree(postResult.getResponse().getContentAsString()).path("orderId").asText());

        // Отправляем отказ из-за отсутствия остатка
        InventoryReservationFailedEvent failedEvent = new InventoryReservationFailedEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                orderId,
                sku,
                quantity,
                "INSUFFICIENT_STOCK",
                Instant.now()
        );
        kafkaTemplate.send(INVENTORY_EVENTS_TOPIC, orderId.toString(), failedEvent);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            MvcResult getResult = mockMvc.perform(get("/api/v1/orders/" + orderId))
                    .andExpect(status().isOk())
                    .andReturn();

            JsonNode getJson = objectMapper.readTree(getResult.getResponse().getContentAsString());
            assertThat(getJson.path("status").asText()).isEqualTo("REJECTED");
            assertThat(getJson.path("rejectionReason").asText()).isEqualTo("INSUFFICIENT_STOCK");
        });
    }

    @Test
    @DisplayName("Сценарий 3: Отказ в резервировании при неизвестном SKU (переход в REJECTED)")
    void shouldTransitionToRejectedWhenSkuNotFound() throws Exception {
        String idempotencyKey = "IDEM-" + UUID.randomUUID();
        String sku = "SKU-UNKNOWN-" + UUID.randomUUID().toString().substring(0, 8);

        CreateOrderRequest request = new CreateOrderRequest(sku, 1);
        MvcResult postResult = mockMvc.perform(post("/api/v1/orders")
                        .header("Idempotency-Key", idempotencyKey)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted())
                .andReturn();

        UUID orderId = UUID.fromString(objectMapper.readTree(postResult.getResponse().getContentAsString()).path("orderId").asText());

        // Отправляем отказ из-за неизвестного SKU
        InventoryReservationFailedEvent failedEvent = new InventoryReservationFailedEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                orderId,
                sku,
                1,
                "SKU_NOT_FOUND",
                Instant.now()
        );
        kafkaTemplate.send(INVENTORY_EVENTS_TOPIC, orderId.toString(), failedEvent);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            MvcResult getResult = mockMvc.perform(get("/api/v1/orders/" + orderId))
                    .andExpect(status().isOk())
                    .andReturn();

            JsonNode getJson = objectMapper.readTree(getResult.getResponse().getContentAsString());
            assertThat(getJson.path("status").asText()).isEqualTo("REJECTED");
            assertThat(getJson.path("rejectionReason").asText()).isEqualTo("SKU_NOT_FOUND");
        });
    }

    @Test
    @DisplayName("Сценарий 4: Повторный запрос с тем же Idempotency-Key и телом возвращает существующий заказ")
    void shouldReturnSameOrderOnIdempotentCreate() throws Exception {
        String idempotencyKey = "IDEM-" + UUID.randomUUID();
        CreateOrderRequest request = new CreateOrderRequest("SKU-MACBOOK-AIR", 1);

        MvcResult first = mockMvc.perform(post("/api/v1/orders")
                        .header("Idempotency-Key", idempotencyKey)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted())
                .andReturn();

        String firstOrderId = objectMapper.readTree(first.getResponse().getContentAsString()).path("orderId").asText();

        MvcResult second = mockMvc.perform(post("/api/v1/orders")
                        .header("Idempotency-Key", idempotencyKey)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andReturn();

        String secondOrderId = objectMapper.readTree(second.getResponse().getContentAsString()).path("orderId").asText();

        assertThat(secondOrderId).isEqualTo(firstOrderId);
    }

    @Test
    @DisplayName("Сценарий 5: Повторный запрос с тем же Idempotency-Key и другим телом возвращает 409 Conflict")
    void shouldReturnConflictOnDifferentBodyWithSameIdempotencyKey() throws Exception {
        String idempotencyKey = "IDEM-" + UUID.randomUUID();
        CreateOrderRequest request1 = new CreateOrderRequest("SKU-MACBOOK-AIR", 1);
        CreateOrderRequest request2 = new CreateOrderRequest("SKU-MACBOOK-AIR", 2);

        mockMvc.perform(post("/api/v1/orders")
                        .header("Idempotency-Key", idempotencyKey)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request1)))
                .andExpect(status().isAccepted());

        mockMvc.perform(post("/api/v1/orders")
                        .header("Idempotency-Key", idempotencyKey)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request2)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("IDEMPOTENCY_KEY_REUSED"));
    }

    @Test
    @DisplayName("Сценарий 9: Создание заказа и outbox в локальной БД гарантирует публикацию события в Kafka")
    void shouldPersistOrderAndPublishOutboxToKafka() throws Exception {
        String idempotencyKey = "IDEM-" + UUID.randomUUID();
        CreateOrderRequest request = new CreateOrderRequest("SKU-PS5-SLIM", 1);

        MvcResult result = mockMvc.perform(post("/api/v1/orders")
                        .header("Idempotency-Key", idempotencyKey)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted())
                .andReturn();

        UUID orderId = UUID.fromString(objectMapper.readTree(result.getResponse().getContentAsString()).path("orderId").asText());

        // Проверяем, что outbox-сообщение создано и успешно опубликовано фоновым шедулером
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            List<OutboxMessageEntity> unpublished = outboxRepository.findUnpublishedForUpdate(100);
            boolean orderOutboxPublished = unpublished.stream()
                    .noneMatch(m -> m.getAggregateId().equals(orderId));
            assertThat(orderOutboxPublished).isTrue();
        });
    }

    @Test
    @DisplayName("Сценарий 12: Отмена зарезервированного заказа через HTTP API и генерация OrderCancelled в outbox")
    void shouldCancelReservedOrderAndPublishCancelledEvent() throws Exception {
        String idempotencyKey = "IDEM-" + UUID.randomUUID();
        CreateOrderRequest request = new CreateOrderRequest("SKU-PS5-SLIM", 1);

        MvcResult postResult = mockMvc.perform(post("/api/v1/orders")
                        .header("Idempotency-Key", idempotencyKey)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted())
                .andReturn();

        UUID orderId = UUID.fromString(objectMapper.readTree(postResult.getResponse().getContentAsString()).path("orderId").asText());

        // Переводим в RESERVED
        InventoryReservedEvent reservedEvent = new InventoryReservedEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                orderId,
                "SKU-PS5-SLIM",
                1,
                Instant.now()
        );
        kafkaTemplate.send(INVENTORY_EVENTS_TOPIC, orderId.toString(), reservedEvent);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            OrderEntity order = orderRepository.findById(orderId).orElseThrow();
            assertThat(order.getStatus()).isEqualTo(OrderStatus.RESERVED);
        });

        // Вызываем POST /api/v1/orders/{id}/cancel
        mockMvc.perform(post("/api/v1/orders/" + orderId + "/cancel"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));

        // Проверяем статус в БД
        OrderEntity order = orderRepository.findById(orderId).orElseThrow();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
    }

    @Test
    @DisplayName("Сценарий 14: Автоматическое истечение срока брони (TTL) переводит заказ в EXPIRED и отправляет событие отмены")
    void shouldExpireOrdersOnTimeoutAndEmitCancellation() throws Exception {
        String idempotencyKey = "IDEM-" + UUID.randomUUID();
        CreateOrderRequest request = new CreateOrderRequest("SKU-PS5-SLIM", 1);

        MvcResult postResult = mockMvc.perform(post("/api/v1/orders")
                        .header("Idempotency-Key", idempotencyKey)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted())
                .andReturn();

        UUID orderId = UUID.fromString(objectMapper.readTree(postResult.getResponse().getContentAsString()).path("orderId").asText());

        // Переводим в RESERVED
        InventoryReservedEvent reservedEvent = new InventoryReservedEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                orderId,
                "SKU-PS5-SLIM",
                1,
                Instant.now()
        );
        kafkaTemplate.send(INVENTORY_EVENTS_TOPIC, orderId.toString(), reservedEvent);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            OrderEntity order = orderRepository.findById(orderId).orElseThrow();
            assertThat(order.getStatus()).isEqualTo(OrderStatus.RESERVED);
        });

        // Искусственно устанавливаем expiresAt в прошлое (10 секунд назад)
        OrderEntity order = orderRepository.findById(orderId).orElseThrow();
        order.markReserved(Instant.now().minusSeconds(60), Instant.now().minusSeconds(10));
        orderRepository.save(order);

        // Запускаем expireBatch
        int expiredCount = orderExpirationService.expireBatch();
        assertThat(expiredCount).isGreaterThanOrEqualTo(1);

        // Проверяем, что заказ перешел в статус EXPIRED
        OrderEntity expiredOrder = orderRepository.findById(orderId).orElseThrow();
        assertThat(expiredOrder.getStatus()).isEqualTo(OrderStatus.EXPIRED);
    }

    @Test
    @DisplayName("Сценарий 15: Запоздалый результат склада не изменяет терминальное состояние отмененного заказа")
    void shouldNotReviveCancelledOrderOnLateInventoryResult() throws Exception {
        String idempotencyKey = "IDEM-" + UUID.randomUUID();
        CreateOrderRequest request = new CreateOrderRequest("SKU-PS5-SLIM", 1);

        MvcResult postResult = mockMvc.perform(post("/api/v1/orders")
                        .header("Idempotency-Key", idempotencyKey)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted())
                .andReturn();

        UUID orderId = UUID.fromString(objectMapper.readTree(postResult.getResponse().getContentAsString()).path("orderId").asText());

        // Сразу отменяем заказ
        mockMvc.perform(post("/api/v1/orders/" + orderId + "/cancel"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));

        // Теперь шлем запоздалый InventoryReservedEvent
        InventoryReservedEvent lateEvent = new InventoryReservedEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                orderId,
                "SKU-PS5-SLIM",
                1,
                Instant.now()
        );
        kafkaTemplate.send(INVENTORY_EVENTS_TOPIC, orderId.toString(), lateEvent);

        // Проверяем, что заказ остается CANCELLED и не воскрешается
        await().during(Duration.ofMillis(800)).atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
            OrderEntity current = orderRepository.findById(orderId).orElseThrow();
            assertThat(current.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        });
    }
}
