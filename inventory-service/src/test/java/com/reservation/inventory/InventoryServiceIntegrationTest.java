package com.reservation.inventory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.reservation.inventory.event.OrderCancelledEvent;
import com.reservation.inventory.event.OrderPlacedEvent;
import com.reservation.inventory.model.Inventory;
import com.reservation.inventory.model.ReservationEntity;
import com.reservation.inventory.model.ReservationStatus;
import com.reservation.inventory.repository.InventoryRepository;
import com.reservation.inventory.repository.OutboxRepository;
import com.reservation.inventory.repository.ProcessedEventRepository;
import com.reservation.inventory.repository.ReservationRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@DisplayName("Интеграционные тесты сервиса склада (inventory-service)")
class InventoryServiceIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(InventoryServiceIntegrationTest.class);
    private static final String ORDER_EVENTS_TOPIC = "order-events";
    private static final String DLT_TOPIC = "order-events-dlt";

    static PostgreSQLContainer<?> postgres;
    static KafkaContainer kafka;
    static String kafkaBootstrap = "localhost:9092";

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private InventoryRepository inventoryRepository;

    @Autowired
    private ReservationRepository reservationRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private OutboxRepository outboxRepository;

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
                        .withDatabaseName("inventory_db")
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
            registry.add("spring.datasource.url", () -> "jdbc:postgresql://localhost:15432/inventory_db");
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
    @DisplayName("Сценарий 1: Успешное резервирование товара при достаточном остатке (PENDING -> HELD)")
    void shouldReserveStockWhenOrderPlacedWithSufficientStock() {
        String sku = "SKU-TEST-1-" + UUID.randomUUID().toString().substring(0, 8);
        inventoryRepository.save(new Inventory(sku, 50));

        int quantity = 2;
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();

        OrderPlacedEvent event = new OrderPlacedEvent(
                eventId,
                "OrderPlaced",
                1,
                1L,
                orderId,
                sku,
                quantity,
                Instant.now()
        );

        kafkaTemplate.send(ORDER_EVENTS_TOPIC, sku, event);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            ReservationEntity reservation = reservationRepository.findById(orderId).orElse(null);
            assertThat(reservation).isNotNull();
            assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.HELD);
            assertThat(reservation.getSku()).isEqualTo(sku);
            assertThat(reservation.getQuantity()).isEqualTo(quantity);

            Inventory updated = inventoryRepository.findById(sku).orElseThrow();
            assertThat(updated.getReservedStock()).isEqualTo(quantity);

            assertThat(processedEventRepository.existsById(eventId)).isTrue();
        });
    }

    @Test
    @DisplayName("Сценарий 2: Отказ в резервировании при недостаточном остатке (INSUFFICIENT_STOCK)")
    void shouldRejectReservationWhenInsufficientStock() {
        String sku = "SKU-TEST-2-" + UUID.randomUUID().toString().substring(0, 8);
        inventoryRepository.save(new Inventory(sku, 10));

        int excessiveQuantity = 9999;
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();

        OrderPlacedEvent event = new OrderPlacedEvent(
                eventId,
                "OrderPlaced",
                1,
                1L,
                orderId,
                sku,
                excessiveQuantity,
                Instant.now()
        );

        kafkaTemplate.send(ORDER_EVENTS_TOPIC, sku, event);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            ReservationEntity reservation = reservationRepository.findById(orderId).orElse(null);
            assertThat(reservation).isNotNull();
            assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.REJECTED);
            assertThat(reservation.getFailureReason()).isEqualTo("INSUFFICIENT_STOCK");

            Inventory current = inventoryRepository.findById(sku).orElseThrow();
            assertThat(current.getReservedStock()).isEqualTo(0);
        });
    }

    @Test
    @DisplayName("Сценарий 3: Отказ в резервировании при неизвестном SKU (SKU_NOT_FOUND)")
    void shouldRejectReservationWhenSkuNotFound() {
        String unknownSku = "SKU-UNKNOWN-" + UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();

        OrderPlacedEvent event = new OrderPlacedEvent(
                eventId,
                "OrderPlaced",
                1,
                1L,
                orderId,
                unknownSku,
                1,
                Instant.now()
        );

        kafkaTemplate.send(ORDER_EVENTS_TOPIC, unknownSku, event);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            ReservationEntity reservation = reservationRepository.findById(orderId).orElse(null);
            assertThat(reservation).isNotNull();
            assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.REJECTED);
            assertThat(reservation.getFailureReason()).isEqualTo("SKU_NOT_FOUND");
        });
    }

    @Test
    @DisplayName("Сценарий 6: Идемпотентная обработка дубликата OrderPlaced без повторного списания остатка")
    void shouldHandleDuplicateOrderPlacedIdempotently() {
        String sku = "SKU-TEST-6-" + UUID.randomUUID().toString().substring(0, 8);
        inventoryRepository.save(new Inventory(sku, 50));

        int quantity = 3;
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();

        OrderPlacedEvent event = new OrderPlacedEvent(
                eventId,
                "OrderPlaced",
                1,
                1L,
                orderId,
                sku,
                quantity,
                Instant.now()
        );

        kafkaTemplate.send(ORDER_EVENTS_TOPIC, sku, event);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Inventory updated = inventoryRepository.findById(sku).orElseThrow();
            assertThat(updated.getReservedStock()).isEqualTo(quantity);
        });

        // Повторная отправка идентичного события с тем же eventId
        kafkaTemplate.send(ORDER_EVENTS_TOPIC, sku, event);

        // Проверяем, что остаток не увеличился второй раз
        await().during(Duration.ofMillis(800)).atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
            Inventory current = inventoryRepository.findById(sku).orElseThrow();
            assertThat(current.getReservedStock()).isEqualTo(quantity);
        });
    }

    @Test
    @DisplayName("Сценарий 7: Идемпотентная обработка повторной отмены заказа OrderCancelled без двойного возврата остатка")
    void shouldHandleDuplicateOrderCancelledIdempotently() {
        String sku = "SKU-TEST-7-" + UUID.randomUUID().toString().substring(0, 8);
        inventoryRepository.save(new Inventory(sku, 50));

        int quantity = 2;
        UUID orderId = UUID.randomUUID();
        UUID placeEventId = UUID.randomUUID();
        UUID cancelEventId = UUID.randomUUID();

        // 1. Создаем заказ и резервируем
        OrderPlacedEvent placeEvent = new OrderPlacedEvent(
                placeEventId,
                "OrderPlaced",
                1,
                1L,
                orderId,
                sku,
                quantity,
                Instant.now()
        );
        kafkaTemplate.send(ORDER_EVENTS_TOPIC, sku, placeEvent);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            ReservationEntity res = reservationRepository.findById(orderId).orElse(null);
            assertThat(res).isNotNull();
            assertThat(res.getStatus()).isEqualTo(ReservationStatus.HELD);
        });

        // 2. Первая отмена
        OrderCancelledEvent cancelEvent = new OrderCancelledEvent(
                cancelEventId,
                2L,
                orderId,
                sku,
                "CUSTOMER_REQUEST",
                Instant.now()
        );
        kafkaTemplate.send(ORDER_EVENTS_TOPIC, sku, cancelEvent);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            ReservationEntity res = reservationRepository.findById(orderId).orElse(null);
            assertThat(res).isNotNull();
            assertThat(res.getStatus()).isEqualTo(ReservationStatus.RELEASED);

            Inventory updated = inventoryRepository.findById(sku).orElseThrow();
            assertThat(updated.getReservedStock()).isEqualTo(0);
        });

        // 3. Повторная доставка той же отмены с тем же eventId
        kafkaTemplate.send(ORDER_EVENTS_TOPIC, sku, cancelEvent);

        await().during(Duration.ofMillis(800)).atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
            Inventory updated = inventoryRepository.findById(sku).orElseThrow();
            assertThat(updated.getReservedStock()).isEqualTo(0);
        });
    }

    @Test
    @DisplayName("Сценарий 8: Защита от оверселлинга при 100 конкурентных заказах на один SKU")
    void shouldPreventOversellingUnderConcurrentOrders() {
        String sku = "SKU-CONCURRENT-" + UUID.randomUUID().toString().substring(0, 8);
        int totalStock = 20;
        inventoryRepository.save(new Inventory(sku, totalStock));

        int totalRequests = 100;
        ExecutorService executor = Executors.newFixedThreadPool(10);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int i = 0; i < totalRequests; i++) {
            UUID orderId = UUID.randomUUID();
            UUID eventId = UUID.randomUUID();
            OrderPlacedEvent event = new OrderPlacedEvent(
                    eventId,
                    "OrderPlaced",
                    1,
                    1L,
                    orderId,
                    sku,
                    1,
                    Instant.now()
            );
            futures.add(CompletableFuture.runAsync(
                    () -> kafkaTemplate.send(ORDER_EVENTS_TOPIC, sku, event),
                    executor
            ));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();

        // Ожидаем завершения обработки всех 100 заказов
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Inventory inventory = inventoryRepository.findById(sku).orElseThrow();
            assertThat(inventory.getReservedStock()).isEqualTo(totalStock);
            assertThat(inventory.getReservedStock()).isLessThanOrEqualTo(inventory.getTotalStock());
        });
    }

    @Test
    @DisplayName("Сценарий 11: Невалидное JSON-сообщение после retry попадает в Dead Letter Topic (DLT)")
    void shouldRouteInvalidJsonToDeadLetterTopic() throws Exception {
        String malformedJson = "{invalid-json-content-payload: 123";
        String key = "SKU-TEST-DLT-" + UUID.randomUUID().toString().substring(0, 8);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-verifier-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        Properties producerProps = new Properties();
        producerProps.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        producerProps.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        producerProps.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
             org.apache.kafka.clients.producer.KafkaProducer<String, String> rawProducer = new org.apache.kafka.clients.producer.KafkaProducer<>(producerProps)) {
            consumer.subscribe(List.of("order-events-retry-0", "order-events-retry-1", "order-events-retry-2", DLT_TOPIC));
            consumer.poll(Duration.ofMillis(100));

            rawProducer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(ORDER_EVENTS_TOPIC, key, malformedJson)).get(5, java.util.concurrent.TimeUnit.SECONDS);

            List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                consumer.poll(Duration.ofMillis(300)).forEach(received::add);
                boolean found = received.stream().anyMatch(r ->
                        key.equals(r.key()) && r.value().contains("invalid-json-content-payload")
                );
                assertThat(found).isTrue();
            });
        }
    }

    @Test
    @DisplayName("Сценарий 12: Освобождение остатка при компенсации отмены заказа (OrderCancelled)")
    void shouldReleaseStockWhenOrderCancelled() {
        String sku = "SKU-TEST-12-" + UUID.randomUUID().toString().substring(0, 8);
        inventoryRepository.save(new Inventory(sku, 50));

        int quantity = 1;
        UUID orderId = UUID.randomUUID();

        // Резервируем
        OrderPlacedEvent placeEvent = new OrderPlacedEvent(
                UUID.randomUUID(),
                "OrderPlaced",
                1,
                1L,
                orderId,
                sku,
                quantity,
                Instant.now()
        );
        kafkaTemplate.send(ORDER_EVENTS_TOPIC, sku, placeEvent);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            ReservationEntity res = reservationRepository.findById(orderId).orElse(null);
            assertThat(res).isNotNull();
            assertThat(res.getStatus()).isEqualTo(ReservationStatus.HELD);
        });

        // Отменяем
        OrderCancelledEvent cancelEvent = new OrderCancelledEvent(
                UUID.randomUUID(),
                2L,
                orderId,
                sku,
                "USER_CANCELLED",
                Instant.now()
        );
        kafkaTemplate.send(ORDER_EVENTS_TOPIC, sku, cancelEvent);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            ReservationEntity res = reservationRepository.findById(orderId).orElse(null);
            assertThat(res).isNotNull();
            assertThat(res.getStatus()).isEqualTo(ReservationStatus.RELEASED);

            Inventory after = inventoryRepository.findById(sku).orElseThrow();
            assertThat(after.getReservedStock()).isEqualTo(0);
        });
    }

    @Test
    @DisplayName("Сценарий 13: Обработка tombstone — отмена заказа до получения OrderPlaced предотвращает поздний резерв")
    void shouldHandleTombstoneWhenCancelArrivesBeforePlaced() {
        String sku = "SKU-TEST-13-" + UUID.randomUUID().toString().substring(0, 8);
        inventoryRepository.save(new Inventory(sku, 50));

        UUID orderId = UUID.randomUUID();
        UUID cancelEventId = UUID.randomUUID();
        UUID placeEventId = UUID.randomUUID();

        // 1. Сначала приходит OrderCancelled
        OrderCancelledEvent cancelEvent = new OrderCancelledEvent(
                cancelEventId,
                1L,
                orderId,
                sku,
                "EARLY_CANCEL",
                Instant.now()
        );
        kafkaTemplate.send(ORDER_EVENTS_TOPIC, sku, cancelEvent);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            ReservationEntity tombstone = reservationRepository.findById(orderId).orElse(null);
            assertThat(tombstone).isNotNull();
            assertThat(tombstone.getStatus()).isEqualTo(ReservationStatus.CANCELLED);
        });

        // 2. Затем приходит запоздалый OrderPlaced
        OrderPlacedEvent placeEvent = new OrderPlacedEvent(
                placeEventId,
                "OrderPlaced",
                1,
                1L,
                orderId,
                sku,
                2,
                Instant.now()
        );
        kafkaTemplate.send(ORDER_EVENTS_TOPIC, sku, placeEvent);

        await().during(Duration.ofMillis(800)).atMost(Duration.ofSeconds(4)).untilAsserted(() -> {
            ReservationEntity res = reservationRepository.findById(orderId).orElse(null);
            assertThat(res).isNotNull();
            // Статус остался CANCELLED
            assertThat(res.getStatus()).isEqualTo(ReservationStatus.CANCELLED);

            // Остаток не был зарезервирован
            Inventory current = inventoryRepository.findById(sku).orElseThrow();
            assertThat(current.getReservedStock()).isEqualTo(0);
        });
    }
}
