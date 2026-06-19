package com.order.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.order.domain.entity.Inventory;
import com.order.domain.entity.InventoryReservation;
import com.order.domain.entity.OutboxEvent;
import com.order.domain.entity.Product;
import com.order.domain.enums.OutboxStatus;
import com.order.infrastructure.repository.InventoryRepository;
import com.order.infrastructure.repository.InventoryReservationRepository;
import com.order.infrastructure.repository.OutboxEventRepository;
import com.order.infrastructure.repository.ProductRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("orderflow-inventory-kafka-integration")
@TestPropertySource(properties = {
        "spring.docker.compose.enabled=false",

        "orderflow.scheduler.outbox.enabled=false",
        "orderflow.scheduler.unpaid-payments.enabled=false",

        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.listener.missing-topics-fatal=false",

        "orderflow.kafka.topics.order-created=order.created.it",
        "orderflow.kafka.topics.order-cancelled=order.cancelled.it",
        "orderflow.kafka.topics.inventory-reserved=inventory.reserved.it",
        "orderflow.kafka.topics.inventory-failed=inventory.failed.it",
        "orderflow.kafka.topics.inventory-released=inventory.released.it"
})
class InventoryOrderEventConsumerKafkaIntegrationTest
        extends AbstractPostgresTestcontainersTest {

    private static final String ORDER_CREATED_TOPIC = "order.created.it";
    private static final String ORDER_CANCELLED_TOPIC = "order.cancelled.it";
    private static final String INVENTORY_RESERVED_TOPIC = "inventory.reserved.it";
    private static final String INVENTORY_FAILED_TOPIC = "inventory.failed.it";
    private static final String INVENTORY_RELEASED_TOPIC = "inventory.released.it";

    private static final KafkaContainer KAFKA =
            new KafkaContainer(
                    DockerImageName.parse("apache/kafka-native:3.8.0")
            );

    private static final String INVENTORY_CONSUMER_GROUP =
            "inventory-consumer-it-" + UUID.randomUUID();

    static {
        KAFKA.start();
    }

    @DynamicPropertySource
    static void registerKafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("orderflow.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add(
                "orderflow.kafka.consumer-groups.inventory",
                () -> INVENTORY_CONSUMER_GROUP
        );
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private InventoryRepository inventoryRepository;

    @Autowired
    private InventoryReservationRepository inventoryReservationRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @BeforeEach
    void waitForKafkaListeners() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;

        while (System.currentTimeMillis() < deadline) {
            boolean allContainersRunning =
                    kafkaListenerEndpointRegistry.getListenerContainers()
                            .stream()
                            .allMatch(MessageListenerContainer::isRunning);

            if (allContainersRunning) {
                Thread.sleep(1_000);
                return;
            }

            Thread.sleep(250);
        }

        throw new AssertionError("Kafka listener containers were not ready");
    }

    @Test
    void shouldConsumeOrderCreatedAndReserveInventoryWithOutboxEvent()
            throws Exception {
        TestData testData = createProductWithInventory(10, 0);
        Product product = testData.product();

        Long orderId = Math.abs(UUID.randomUUID().getMostSignificantBits());
        Long userId = 1001L;

        Map<String, Object> event = Map.of(
                "orderId", orderId,
                "userId", userId,
                "totalAmount", new BigDecimal("149.90"),
                "items", List.of(
                        Map.of(
                                "productId", product.getId(),
                                "quantity", 3,
                                "price", new BigDecimal("49.97")
                        )
                ),
                "createdAt", OffsetDateTime.now()
        );

        sendKafkaMessage(
                ORDER_CREATED_TOPIC,
                orderId,
                objectMapper.writeValueAsString(event)
        );

        Inventory updatedInventory = awaitInventory(product.getId(), 7, 3);

        assertThat(updatedInventory.getProductId()).isEqualTo(product.getId());
        assertThat(updatedInventory.getAvailableQuantity()).isEqualTo(7);
        assertThat(updatedInventory.getReservedQuantity()).isEqualTo(3);

        InventoryReservation reservation =
                awaitInventoryReservation(orderId, product.getId(), "RESERVED");

        assertThat(reservation.getOrderId()).isEqualTo(orderId);
        assertThat(reservation.getProductId()).isEqualTo(product.getId());
        assertThat(reservation.getQuantity()).isEqualTo(3);
        assertThat(String.valueOf(reservation.getStatus())).isEqualTo("RESERVED");

        OutboxEvent outboxEvent =
                awaitOutboxEvent("INVENTORY", orderId, "INVENTORY_RESERVED");

        assertThat(outboxEvent.getAggregateType()).isEqualTo("INVENTORY");
        assertThat(outboxEvent.getAggregateId()).isEqualTo(orderId);
        assertThat(outboxEvent.getEventType()).isEqualTo("INVENTORY_RESERVED");
        assertThat(outboxEvent.getTopic()).isEqualTo(INVENTORY_RESERVED_TOPIC);
        assertThat(outboxEvent.getEventKey()).isEqualTo(orderId.toString());
        assertThat(outboxEvent.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outboxEvent.getRetryCount()).isZero();
        assertThat(outboxEvent.getPayload()).contains("\"orderId\":" + orderId);
        assertThat(outboxEvent.getPayload()).contains("\"productId\":" + product.getId());
        assertThat(outboxEvent.getPayload()).contains("\"quantity\":3");
    }

    @Test
    void shouldConsumeOrderCreatedAndCreateInventoryFailedOutboxEventWhenStockIsInsufficient()
            throws Exception {
        TestData testData = createProductWithInventory(2, 0);
        Product product = testData.product();

        Long orderId = Math.abs(UUID.randomUUID().getMostSignificantBits());
        Long userId = 1002L;

        Map<String, Object> event = Map.of(
                "orderId", orderId,
                "userId", userId,
                "totalAmount", new BigDecimal("249.95"),
                "items", List.of(
                        Map.of(
                                "productId", product.getId(),
                                "quantity", 5,
                                "price", new BigDecimal("49.99")
                        )
                ),
                "createdAt", OffsetDateTime.now()
        );

        sendKafkaMessage(
                ORDER_CREATED_TOPIC,
                orderId,
                objectMapper.writeValueAsString(event)
        );

        OutboxEvent outboxEvent =
                awaitInventoryFailedOutboxEventWithDiagnostics(orderId);

        assertThat(outboxEvent.getAggregateType()).isEqualTo("INVENTORY");
        assertThat(outboxEvent.getAggregateId()).isEqualTo(orderId);
        assertThat(outboxEvent.getEventType()).isEqualTo("INVENTORY_FAILED");
        assertThat(outboxEvent.getTopic()).isEqualTo(INVENTORY_FAILED_TOPIC);
        assertThat(outboxEvent.getEventKey()).isEqualTo(orderId.toString());
        assertThat(outboxEvent.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outboxEvent.getRetryCount()).isZero();

        assertThat(outboxEvent.getPayload()).contains("\"orderId\":" + orderId);
        assertThat(outboxEvent.getPayload()).contains("product id: " + product.getId());
        assertThat(outboxEvent.getPayload()).contains("Requested: 5");

        Inventory updatedInventory =
                inventoryRepository.findByProductId(product.getId())
                        .block(Duration.ofSeconds(1));

        assertThat(updatedInventory).isNotNull();
        assertThat(updatedInventory.getAvailableQuantity()).isEqualTo(2);
        assertThat(updatedInventory.getReservedQuantity()).isZero();

        Optional<InventoryReservation> reservation =
                findInventoryReservation(orderId, product.getId());

        assertThat(reservation).isEmpty();
    }

    @Test
    void shouldConsumeOrderCancelledAndReleaseReservedInventory()
            throws Exception {
        TestData testData = createProductWithInventory(10, 0);
        Product product = testData.product();

        Long orderId = Math.abs(UUID.randomUUID().getMostSignificantBits());

        inventoryRepository.reserveStock(product.getId(), 3)
                .then(inventoryReservationRepository.createReservation(
                        orderId,
                        product.getId(),
                        3
                ))
                .block(Duration.ofSeconds(5));

        Inventory reservedInventory =
                inventoryRepository.findByProductId(product.getId())
                        .block(Duration.ofSeconds(1));

        assertThat(reservedInventory).isNotNull();
        assertThat(reservedInventory.getAvailableQuantity()).isEqualTo(7);
        assertThat(reservedInventory.getReservedQuantity()).isEqualTo(3);

        InventoryReservation reservedReservation =
                awaitInventoryReservation(orderId, product.getId(), "RESERVED");

        assertThat(reservedReservation.getOrderId()).isEqualTo(orderId);
        assertThat(reservedReservation.getProductId()).isEqualTo(product.getId());
        assertThat(reservedReservation.getQuantity()).isEqualTo(3);
        assertThat(String.valueOf(reservedReservation.getStatus())).isEqualTo("RESERVED");

        Map<String, Object> event = Map.of(
                "orderId", orderId,
                "items", List.of(
                        Map.of(
                                "productId", product.getId(),
                                "quantity", 3,
                                "price", new BigDecimal("49.99")
                        )
                ),
                "cancelledAt", OffsetDateTime.now()
        );

        sendKafkaMessage(
                ORDER_CANCELLED_TOPIC,
                orderId,
                objectMapper.writeValueAsString(event)
        );

        Inventory updatedInventory = awaitInventory(product.getId(), 10, 0);

        assertThat(updatedInventory.getProductId()).isEqualTo(product.getId());
        assertThat(updatedInventory.getAvailableQuantity()).isEqualTo(10);
        assertThat(updatedInventory.getReservedQuantity()).isZero();

        InventoryReservation releasedReservation =
                awaitInventoryReservation(orderId, product.getId(), "RELEASED");

        assertThat(releasedReservation.getOrderId()).isEqualTo(orderId);
        assertThat(releasedReservation.getProductId()).isEqualTo(product.getId());
        assertThat(releasedReservation.getQuantity()).isEqualTo(3);
        assertThat(String.valueOf(releasedReservation.getStatus())).isEqualTo("RELEASED");

        OutboxEvent outboxEvent =
                awaitOutboxEvent("INVENTORY", orderId, "INVENTORY_RELEASED");

        assertThat(outboxEvent.getAggregateType()).isEqualTo("INVENTORY");
        assertThat(outboxEvent.getAggregateId()).isEqualTo(orderId);
        assertThat(outboxEvent.getEventType()).isEqualTo("INVENTORY_RELEASED");
        assertThat(outboxEvent.getTopic()).isEqualTo(INVENTORY_RELEASED_TOPIC);
        assertThat(outboxEvent.getEventKey()).isEqualTo(orderId.toString());
        assertThat(outboxEvent.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outboxEvent.getRetryCount()).isZero();
        assertThat(outboxEvent.getPayload()).contains("\"orderId\":" + orderId);
        assertThat(outboxEvent.getPayload()).contains("\"productId\":" + product.getId());
        assertThat(outboxEvent.getPayload()).contains("\"quantity\":3");
    }

    private Mono<Void> cleanDatabase() {
        return outboxEventRepository.deleteAll()
                .then(inventoryReservationRepository.deleteAll())
                .then(inventoryRepository.deleteAll())
                .then(productRepository.deleteAll());
    }

    private TestData createProductWithInventory(
            int availableQuantity,
            int reservedQuantity
    ) {
        cleanDatabase().block(Duration.ofSeconds(5));

        Product product = Product.builder()
                .name("Kafka Inventory Test Product " + UUID.randomUUID())
                .price(new BigDecimal("49.99"))
                .stock(100)
                .build();

        Product savedProduct =
                productRepository.save(product)
                        .block(Duration.ofSeconds(5));

        assertThat(savedProduct).isNotNull();
        assertThat(savedProduct.getId()).isNotNull();

        Inventory inventory = Inventory.builder()
                .productId(savedProduct.getId())
                .availableQuantity(availableQuantity)
                .reservedQuantity(reservedQuantity)
                .build();

        Inventory savedInventory =
                inventoryRepository.save(inventory)
                        .block(Duration.ofSeconds(5));

        assertThat(savedInventory).isNotNull();
        assertThat(savedInventory.getId()).isNotNull();
        assertThat(savedInventory.getProductId()).isEqualTo(savedProduct.getId());

        return new TestData(savedProduct, savedInventory);
    }

    private void sendKafkaMessage(
            String topic,
            Long key,
            String payload
    ) throws Exception {
        kafkaTemplate.send(
                        topic,
                        key.toString(),
                        payload
                )
                .get(10, TimeUnit.SECONDS);

        kafkaTemplate.flush();
    }

    private Inventory awaitInventory(
            Long productId,
            int expectedAvailableQuantity,
            int expectedReservedQuantity
    ) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;

        while (System.currentTimeMillis() < deadline) {
            Inventory inventory =
                    inventoryRepository.findByProductId(productId)
                            .block(Duration.ofSeconds(1));

            if (inventory != null
                    && Integer.valueOf(expectedAvailableQuantity)
                    .equals(inventory.getAvailableQuantity())
                    && Integer.valueOf(expectedReservedQuantity)
                    .equals(inventory.getReservedQuantity())) {
                return inventory;
            }

            Thread.sleep(250);
        }

        throw new AssertionError(
                "Inventory was not updated for productId=" + productId
                        + ", expected availableQuantity=" + expectedAvailableQuantity
                        + ", expected reservedQuantity=" + expectedReservedQuantity
        );
    }

    private InventoryReservation awaitInventoryReservation(
            Long orderId,
            Long productId,
            String expectedStatus
    ) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;

        while (System.currentTimeMillis() < deadline) {
            Optional<InventoryReservation> reservation =
                    findInventoryReservation(orderId, productId);

            if (reservation.isPresent()
                    && expectedStatus.equals(String.valueOf(reservation.get().getStatus()))) {
                return reservation.get();
            }

            Thread.sleep(250);
        }

        throw new AssertionError(
                "Inventory reservation was not found for orderId=" + orderId
                        + ", productId=" + productId
                        + ", expectedStatus=" + expectedStatus
        );
    }

    private Optional<InventoryReservation> findInventoryReservation(
            Long orderId,
            Long productId
    ) {
        List<InventoryReservation> reservations =
                inventoryReservationRepository.findAll()
                        .collectList()
                        .block(Duration.ofSeconds(1));

        if (reservations == null) {
            return Optional.empty();
        }

        return reservations.stream()
                .filter(reservation -> orderId.equals(reservation.getOrderId()))
                .filter(reservation -> productId.equals(reservation.getProductId()))
                .findFirst();
    }

    private OutboxEvent awaitOutboxEvent(
            String aggregateType,
            Long aggregateId,
            String eventType
    ) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;

        while (System.currentTimeMillis() < deadline) {
            List<OutboxEvent> events =
                    outboxEventRepository.findAll()
                            .collectList()
                            .block(Duration.ofSeconds(1));

            if (events != null) {
                Optional<OutboxEvent> event =
                        events.stream()
                                .filter(outbox ->
                                        aggregateType.equals(outbox.getAggregateType())
                                                && aggregateId.equals(outbox.getAggregateId())
                                                && eventType.equals(outbox.getEventType())
                                )
                                .findFirst();

                if (event.isPresent()) {
                    return event.get();
                }
            }

            Thread.sleep(250);
        }

        throw new AssertionError(
                eventType + " outbox event was not created for aggregateType="
                        + aggregateType + ", aggregateId=" + aggregateId
        );
    }

    private OutboxEvent awaitInventoryFailedOutboxEventWithDiagnostics(Long orderId)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        List<OutboxEvent> latestEvents = List.of();

        while (System.currentTimeMillis() < deadline) {
            List<OutboxEvent> events =
                    outboxEventRepository.findAll()
                            .collectList()
                            .block(Duration.ofSeconds(1));

            if (events != null) {
                latestEvents = events;

                Optional<OutboxEvent> failedEvent =
                        events.stream()
                                .filter(outbox -> "INVENTORY".equals(outbox.getAggregateType()))
                                .filter(outbox -> orderId.equals(outbox.getAggregateId()))
                                .filter(outbox -> "INVENTORY_FAILED".equals(outbox.getEventType()))
                                .findFirst();

                if (failedEvent.isPresent()) {
                    return failedEvent.get();
                }
            }

            Thread.sleep(250);
        }

        throw new AssertionError(
                "INVENTORY_FAILED outbox event was not created for orderId="
                        + orderId
                        + ". Existing outbox events: "
                        + latestEvents.stream()
                        .map(event ->
                                "{aggregateType=" + event.getAggregateType()
                                        + ", aggregateId=" + event.getAggregateId()
                                        + ", eventType=" + event.getEventType()
                                        + ", topic=" + event.getTopic()
                                        + ", eventKey=" + event.getEventKey()
                                        + ", status=" + event.getStatus()
                                        + ", payload=" + event.getPayload()
                                        + "}"
                        )
                        .toList()
        );
    }

    private record TestData(
            Product product,
            Inventory inventory
    ) {
    }
}