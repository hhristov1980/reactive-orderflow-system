package com.order.integration;

import com.order.domain.entity.Order;
import com.order.domain.entity.Payment;
import com.order.domain.entity.Shipment;
import com.order.domain.entity.User;
import com.order.domain.enums.OrderStatus;
import com.order.domain.enums.PaymentStatus;
import com.order.domain.enums.ShipmentStatus;
import com.order.domain.enums.UserRole;
import com.order.domain.enums.UserStatus;
import com.order.infrastructure.repository.OrderRepository;
import com.order.infrastructure.repository.PaymentRepository;
import com.order.infrastructure.repository.ShipmentRepository;
import com.order.infrastructure.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.r2dbc.test.autoconfigure.DataR2dbcTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@DataR2dbcTest
class ShipmentRepositoryIntegrationTest extends AbstractPostgresTestcontainersTest{

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private ShipmentRepository shipmentRepository;

    @Test
    void shouldSaveAndFindCreatedShipment() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newActiveCustomer()))
                                .flatMap(savedUser ->
                                        orderRepository.save(newConfirmedOrder(savedUser.getId()))
                                )
                                .flatMap(savedOrder ->
                                        paymentRepository.save(newCompletedPayment(savedOrder.getId()))
                                                .thenReturn(savedOrder)
                                )
                                .flatMap(savedOrder ->
                                        shipmentRepository.save(newCreatedShipment(savedOrder.getId()))
                                )
                                .flatMap(savedShipment ->
                                        shipmentRepository.findById(savedShipment.getId())
                                )
                )
                .expectNextMatches(saved ->
                        saved.getOrderId() != null
                                && saved.getStatus() == ShipmentStatus.CREATED
                                && saved.getTrackingNumber() != null
                                && saved.getCarrier().equals("TEST_CARRIER")
                                && saved.getCreatedAt() != null
                                && saved.getUpdatedAt() != null
                                && saved.getShippedAt() == null
                                && saved.getDeliveredAt() == null
                )
                .verifyComplete();
    }

    private Mono<Void> cleanDatabase() {
        return shipmentRepository.deleteAll()
                .then(paymentRepository.deleteAll())
                .then(orderRepository.deleteAll())
                .then(userRepository.deleteAll());
    }

    private static User newActiveCustomer() {
        OffsetDateTime now = OffsetDateTime.now();

        return User.builder()
                .email("shipment.test.user." + UUID.randomUUID() + "@example.com")
                .name("Shipment Test User " + UUID.randomUUID())
                .role(UserRole.CUSTOMER)
                .status(UserStatus.ACTIVE)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private static Order newConfirmedOrder(Long userId) {
        OffsetDateTime now = OffsetDateTime.now();

        return Order.builder()
                .userId(userId)
                .status(OrderStatus.CONFIRMED)
                .totalAmount(new BigDecimal("49.80"))
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private static Payment newCompletedPayment(Long orderId) {
        OffsetDateTime now = OffsetDateTime.now();

        return Payment.builder()
                .orderId(orderId)
                .status(PaymentStatus.COMPLETED)
                .amount(new BigDecimal("49.80"))
                .provider("TEST_PROVIDER")
                .transactionId("tx-" + UUID.randomUUID())
                .failureReason(null)
                .createdAt(now.minusMinutes(5))
                .updatedAt(now)
                .paidAt(now)
                .failedAt(null)
                .expiredAt(null)
                .build();
    }

    private static Shipment newCreatedShipment(Long orderId) {
        OffsetDateTime now = OffsetDateTime.now();

        return Shipment.builder()
                .orderId(orderId)
                .status(ShipmentStatus.CREATED)
                .trackingNumber("TRK-" + UUID.randomUUID())
                .carrier("TEST_CARRIER")
                .createdAt(now)
                .updatedAt(now)
                .shippedAt(null)
                .deliveredAt(null)
                .build();
    }
}