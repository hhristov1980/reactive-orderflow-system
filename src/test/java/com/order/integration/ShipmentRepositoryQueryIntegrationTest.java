package com.order.integration;

import com.order.domain.entity.Order;
import com.order.domain.entity.Shipment;
import com.order.domain.entity.User;
import com.order.domain.enums.OrderStatus;
import com.order.domain.enums.ShipmentStatus;
import com.order.domain.enums.UserRole;
import com.order.domain.enums.UserStatus;
import com.order.infrastructure.repository.OrderRepository;
import com.order.infrastructure.repository.ShipmentRepository;
import com.order.infrastructure.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.r2dbc.test.autoconfigure.DataR2dbcTest;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@DataR2dbcTest
class ShipmentRepositoryQueryIntegrationTest extends AbstractPostgresTestcontainersTest {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private ShipmentRepository shipmentRepository;

    @Test
    void shouldFindShipmentByOrderId() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newActiveCustomer()))
                                .flatMap(savedUser -> orderRepository.save(newConfirmedOrder(savedUser.getId())))
                                .flatMap(savedOrder ->
                                        shipmentRepository.save(newCreatedShipment(savedOrder.getId()))
                                                .then(shipmentRepository.findByOrderId(savedOrder.getId()))
                                )
                )
                .expectNextMatches(shipment ->
                        shipment.getOrderId() != null
                                && shipment.getStatus() == ShipmentStatus.CREATED
                                && shipment.getTrackingNumber().startsWith("TRK-")
                                && shipment.getCarrier().equals("TEST_CARRIER")
                                && shipment.getShippedAt() == null
                                && shipment.getDeliveredAt() == null
                )
                .verifyComplete();
    }

    @Test
    void shouldReturnTrueWhenShipmentExistsByOrderId() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newActiveCustomer()))
                                .flatMap(savedUser -> orderRepository.save(newConfirmedOrder(savedUser.getId())))
                                .flatMap(savedOrder ->
                                        shipmentRepository.save(newCreatedShipment(savedOrder.getId()))
                                                .then(shipmentRepository.existsByOrderId(savedOrder.getId()))
                                )
                )
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    void shouldReturnFalseWhenShipmentDoesNotExistByOrderId() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(shipmentRepository.existsByOrderId(999_999L))
                )
                .expectNext(false)
                .verifyComplete();
    }

    private Mono<Void> cleanDatabase() {
        return shipmentRepository.deleteAll()
                .then(orderRepository.deleteAll())
                .then(userRepository.deleteAll());
    }

    private static User newActiveCustomer() {
        OffsetDateTime now = OffsetDateTime.now();

        return User.builder()
                .email("shipment.query.user." + UUID.randomUUID() + "@example.com")
                .name("Shipment Query Test User " + UUID.randomUUID())
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