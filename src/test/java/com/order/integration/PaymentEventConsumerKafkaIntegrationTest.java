package com.order.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.order.domain.entity.Order;
import com.order.domain.entity.OrderItem;
import com.order.domain.entity.OutboxEvent;
import com.order.domain.entity.Shipment;
import com.order.domain.entity.User;
import com.order.domain.enums.OrderStatus;
import com.order.domain.enums.OutboxStatus;
import com.order.domain.enums.ShipmentStatus;
import com.order.domain.enums.UserRole;
import com.order.domain.enums.UserStatus;
import com.order.domain.event.PaymentCompletedEvent;
import com.order.domain.event.PaymentExpiredEvent;
import com.order.domain.event.PaymentFailedEvent;
import com.order.infrastructure.repository.OrderItemRepository;
import com.order.infrastructure.repository.OrderRepository;
import com.order.infrastructure.repository.OutboxEventRepository;
import com.order.infrastructure.repository.ShipmentRepository;
import com.order.infrastructure.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
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
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@TestPropertySource(properties = {
        "spring.docker.compose.enabled=false",

        "orderflow.scheduler.outbox.enabled=false",
        "orderflow.scheduler.unpaid-payments.enabled=false",

        "orderflow.kafka.topics.payment-completed=payment.completed.it",
        "orderflow.kafka.topics.payment-failed=payment.failed.it",
        "orderflow.kafka.topics.payment-expired=payment.expired.it",
        "orderflow.kafka.topics.shipment-created=shipment.created.it",
        "orderflow.kafka.topics.order-cancelled=order.cancelled.it",

        "orderflow.kafka.consumer-groups.shipment=shipment-consumer-it",
        "orderflow.kafka.consumer-groups.order=order-consumer-it"
})
class PaymentEventConsumerKafkaIntegrationTest
        extends AbstractPostgresTestcontainersTest {

    private static final KafkaContainer KAFKA =
            new KafkaContainer(
                    DockerImageName.parse("apache/kafka-native:3.8.0")
            );

    static {
        KAFKA.start();
    }

    @DynamicPropertySource
    static void registerKafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("orderflow.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ShipmentRepository shipmentRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OrderItemRepository orderItemRepository;

    @Test
    void shouldConsumePaymentCompletedAndCreateShipmentWithOutboxEvent()
            throws Exception {
        Long paymentId = randomPositiveLong();
        Long orderId = randomPositiveLong();

        PaymentCompletedEvent event = new PaymentCompletedEvent(
                paymentId,
                orderId,
                new BigDecimal("149.90"),
                "TXN-" + orderId,
                OffsetDateTime.now()
        );

        String payload = objectMapper.writeValueAsString(event);

        cleanDatabase().block(Duration.ofSeconds(5));

        kafkaTemplate.send(
                        "payment.completed.it",
                        orderId.toString(),
                        payload
                )
                .get();

        Shipment shipment = awaitShipment(orderId);

        assertThat(shipment.getOrderId()).isEqualTo(orderId);
        assertThat(shipment.getStatus()).isEqualTo(ShipmentStatus.CREATED);
        assertThat(shipment.getCarrier()).isEqualTo("DHL");
        assertThat(shipment.getTrackingNumber()).isNotNull();
        assertThat(shipment.getTrackingNumber()).startsWith("TRK-");
        assertThat(shipment.getShippedAt()).isNull();
        assertThat(shipment.getDeliveredAt()).isNull();

        OutboxEvent outboxEvent = awaitShipmentCreatedOutboxEvent(shipment.getId());

        assertThat(outboxEvent.getAggregateType()).isEqualTo("SHIPMENT");
        assertThat(outboxEvent.getAggregateId()).isEqualTo(shipment.getId());
        assertThat(outboxEvent.getEventType()).isEqualTo("SHIPMENT_CREATED");
        assertThat(outboxEvent.getTopic()).isEqualTo("shipment.created.it");
        assertThat(outboxEvent.getEventKey()).isEqualTo(orderId.toString());
        assertThat(outboxEvent.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outboxEvent.getRetryCount()).isZero();
        assertThat(outboxEvent.getPayload()).contains("\"orderId\":" + orderId);
        assertThat(outboxEvent.getPayload()).contains("\"trackingNumber\"");
        assertThat(outboxEvent.getPayload()).contains("\"carrier\":\"DHL\"");
    }

    @Test
    void shouldConsumePaymentFailedAndCancelOrderWithOutboxEvent()
            throws Exception {
        Long paymentId = randomPositiveLong();

        cleanDatabase().block(Duration.ofSeconds(5));

        Order order = createConfirmedOrderWithOneItem();

        PaymentFailedEvent event = new PaymentFailedEvent(
                paymentId,
                order.getId(),
                new BigDecimal("20.00"),
                "Card declined",
                OffsetDateTime.now()
        );

        String payload = objectMapper.writeValueAsString(event);

        kafkaTemplate.send(
                        "payment.failed.it",
                        order.getId().toString(),
                        payload
                )
                .get();

        Order cancelledOrder = awaitOrderStatus(
                order.getId(),
                OrderStatus.CANCELLED
        );

        assertThat(cancelledOrder.getStatus()).isEqualTo(OrderStatus.CANCELLED);

        OutboxEvent outboxEvent =
                awaitOrderCancelledOutboxEvent(cancelledOrder.getId());

        assertThat(outboxEvent.getAggregateType()).isEqualTo("ORDER");
        assertThat(outboxEvent.getAggregateId()).isEqualTo(cancelledOrder.getId());
        assertThat(outboxEvent.getEventType()).isEqualTo("ORDER_CANCELLED");
        assertThat(outboxEvent.getTopic()).isEqualTo("order.cancelled.it");
        assertThat(outboxEvent.getEventKey()).isEqualTo(cancelledOrder.getId().toString());
        assertThat(outboxEvent.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outboxEvent.getRetryCount()).isZero();
        assertThat(outboxEvent.getPayload()).contains("\"orderId\":" + cancelledOrder.getId());
        assertThat(outboxEvent.getPayload()).contains("\"items\"");
    }

    @Test
    void shouldConsumePaymentExpiredAndCancelOrderWithOutboxEvent()
            throws Exception {
        Long paymentId = randomPositiveLong();

        cleanDatabase().block(Duration.ofSeconds(5));

        Order order = createConfirmedOrderWithOneItem();

        PaymentExpiredEvent event = new PaymentExpiredEvent(
                paymentId,
                order.getId(),
                new BigDecimal("20.00"),
                "Payment expired before completion",
                OffsetDateTime.now()
        );

        String payload = objectMapper.writeValueAsString(event);

        kafkaTemplate.send(
                        "payment.expired.it",
                        order.getId().toString(),
                        payload
                )
                .get();

        Order cancelledOrder = awaitOrderStatus(
                order.getId(),
                OrderStatus.CANCELLED
        );

        assertThat(cancelledOrder.getStatus()).isEqualTo(OrderStatus.CANCELLED);

        OutboxEvent outboxEvent =
                awaitOrderCancelledOutboxEvent(cancelledOrder.getId());

        assertThat(outboxEvent.getAggregateType()).isEqualTo("ORDER");
        assertThat(outboxEvent.getAggregateId()).isEqualTo(cancelledOrder.getId());
        assertThat(outboxEvent.getEventType()).isEqualTo("ORDER_CANCELLED");
        assertThat(outboxEvent.getTopic()).isEqualTo("order.cancelled.it");
        assertThat(outboxEvent.getEventKey()).isEqualTo(cancelledOrder.getId().toString());
        assertThat(outboxEvent.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outboxEvent.getRetryCount()).isZero();
        assertThat(outboxEvent.getPayload()).contains("\"orderId\":" + cancelledOrder.getId());
        assertThat(outboxEvent.getPayload()).contains("\"items\"");
    }

    private Mono<Void> cleanDatabase() {
        return outboxEventRepository.deleteAll()
                .then(shipmentRepository.deleteAll())
                .then(orderItemRepository.deleteAll())
                .then(orderRepository.deleteAll())
                .then(userRepository.deleteAll());
    }

    private Order createConfirmedOrderWithOneItem() {
        Long userSeed = randomPositiveLong();

        User user = userRepository.save(newUser(userSeed))
                .block(Duration.ofSeconds(5));

        Order order = orderRepository.save(newOrder(
                        user.getId(),
                        OrderStatus.CONFIRMED
                ))
                .block(Duration.ofSeconds(5));

        orderItemRepository.save(newOrderItem(
                        order.getId(),
                        1001L,
                        2,
                        new BigDecimal("10.00")
                ))
                .block(Duration.ofSeconds(5));

        return order;
    }

    private Shipment awaitShipment(Long orderId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;

        while (System.currentTimeMillis() < deadline) {
            Optional<Shipment> shipment =
                    shipmentRepository.findByOrderId(orderId)
                            .blockOptional(Duration.ofSeconds(1));

            if (shipment.isPresent()) {
                return shipment.get();
            }

            Thread.sleep(250);
        }

        throw new AssertionError(
                "Shipment was not created for orderId=" + orderId
        );
    }

    private OutboxEvent awaitShipmentCreatedOutboxEvent(Long shipmentId)
            throws InterruptedException {
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
                                        "SHIPMENT".equals(outbox.getAggregateType())
                                                && shipmentId.equals(outbox.getAggregateId())
                                                && "SHIPMENT_CREATED".equals(outbox.getEventType())
                                )
                                .findFirst();

                if (event.isPresent()) {
                    return event.get();
                }
            }

            Thread.sleep(250);
        }

        throw new AssertionError(
                "SHIPMENT_CREATED outbox event was not created for shipmentId=" + shipmentId
        );
    }

    private Order awaitOrderStatus(
            Long orderId,
            OrderStatus expectedStatus
    ) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;

        while (System.currentTimeMillis() < deadline) {
            Optional<Order> order =
                    orderRepository.findById(orderId)
                            .blockOptional(Duration.ofSeconds(1));

            if (order.isPresent() && order.get().getStatus() == expectedStatus) {
                return order.get();
            }

            Thread.sleep(250);
        }

        throw new AssertionError(
                "Order " + orderId + " did not reach status " + expectedStatus
        );
    }

    private OutboxEvent awaitOrderCancelledOutboxEvent(Long orderId)
            throws InterruptedException {
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
                                        "ORDER".equals(outbox.getAggregateType())
                                                && orderId.equals(outbox.getAggregateId())
                                                && "ORDER_CANCELLED".equals(outbox.getEventType())
                                )
                                .findFirst();

                if (event.isPresent()) {
                    return event.get();
                }
            }

            Thread.sleep(250);
        }

        throw new AssertionError(
                "ORDER_CANCELLED outbox event was not created for orderId=" + orderId
        );
    }

    private static User newUser(Long seed) {
        OffsetDateTime now = OffsetDateTime.now();

        return User.builder()
                .email("payment.consumer.user." + seed + "@example.com")
                .name("Payment Consumer User " + seed)
                .role(UserRole.CUSTOMER)
                .status(UserStatus.ACTIVE)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private static Order newOrder(
            Long userId,
            OrderStatus status
    ) {
        OffsetDateTime now = OffsetDateTime.now();

        return Order.builder()
                .userId(userId)
                .status(status)
                .totalAmount(new BigDecimal("20.00"))
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private static OrderItem newOrderItem(
            Long orderId,
            Long productId,
            int quantity,
            BigDecimal price
    ) {
        return OrderItem.builder()
                .orderId(orderId)
                .productId(productId)
                .quantity(quantity)
                .price(price)
                .build();
    }

    private static Long randomPositiveLong() {
        return Math.abs(UUID.randomUUID().getMostSignificantBits());
    }
}