package com.order.integration;

import com.order.domain.entity.Order;
import com.order.domain.entity.Payment;
import com.order.domain.entity.User;
import com.order.domain.enums.OrderStatus;
import com.order.domain.enums.PaymentStatus;
import com.order.domain.enums.UserRole;
import com.order.domain.enums.UserStatus;
import com.order.infrastructure.repository.OrderRepository;
import com.order.infrastructure.repository.PaymentRepository;
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
class PaymentRepositoryIntegrationTest extends AbstractPostgresTestcontainersTest{

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private PaymentRepository paymentRepository;

    @Test
    void shouldSaveAndFindPendingPayment() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newActiveCustomer()))
                                .flatMap(savedUser ->
                                        orderRepository.save(newConfirmedOrder(savedUser.getId()))
                                )
                                .flatMap(savedOrder ->
                                        paymentRepository.save(newPendingPayment(savedOrder.getId()))
                                )
                                .flatMap(savedPayment ->
                                        paymentRepository.findById(savedPayment.getId())
                                )
                )
                .expectNextMatches(saved ->
                        saved.getOrderId() != null
                                && saved.getStatus() == PaymentStatus.PENDING
                                && saved.getAmount().compareTo(new BigDecimal("49.80")) == 0
                                && saved.getProvider().equals("TEST_PROVIDER")
                                && saved.getTransactionId() == null
                                && saved.getFailureReason() == null
                                && saved.getPaidAt() == null
                                && saved.getFailedAt() == null
                                && saved.getExpiredAt() == null
                                && saved.getCreatedAt() != null
                                && saved.getUpdatedAt() != null
                )
                .verifyComplete();
    }

    @Test
    void shouldSaveAndFindExpiredPayment() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newActiveCustomer()))
                                .flatMap(savedUser ->
                                        orderRepository.save(newConfirmedOrder(savedUser.getId()))
                                )
                                .flatMap(savedOrder ->
                                        paymentRepository.save(newExpiredPayment(savedOrder.getId()))
                                )
                                .flatMap(savedPayment ->
                                        paymentRepository.findById(savedPayment.getId())
                                )
                )
                .expectNextMatches(saved ->
                        saved.getOrderId() != null
                                && saved.getStatus() == PaymentStatus.EXPIRED
                                && saved.getAmount().compareTo(new BigDecimal("49.80")) == 0
                                && saved.getExpiredAt() != null
                                && saved.getPaidAt() == null
                                && saved.getFailedAt() == null
                )
                .verifyComplete();
    }

    private Mono<Void> cleanDatabase() {
        return paymentRepository.deleteAll()
                .then(orderRepository.deleteAll())
                .then(userRepository.deleteAll());
    }

    private static User newActiveCustomer() {
        OffsetDateTime now = OffsetDateTime.now();

        return User.builder()
                .email("payment.test.user." + UUID.randomUUID() + "@example.com")
                .name("Payment Test User " + UUID.randomUUID())
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

    private static Payment newPendingPayment(Long orderId) {
        OffsetDateTime now = OffsetDateTime.now();

        return Payment.builder()
                .orderId(orderId)
                .status(PaymentStatus.PENDING)
                .amount(new BigDecimal("49.80"))
                .provider("TEST_PROVIDER")
                .transactionId(null)
                .failureReason(null)
                .createdAt(now)
                .updatedAt(now)
                .paidAt(null)
                .failedAt(null)
                .expiredAt(null)
                .build();
    }

    private static Payment newExpiredPayment(Long orderId) {
        OffsetDateTime now = OffsetDateTime.now();

        return Payment.builder()
                .orderId(orderId)
                .status(PaymentStatus.EXPIRED)
                .amount(new BigDecimal("49.80"))
                .provider("TEST_PROVIDER")
                .transactionId(null)
                .failureReason(null)
                .createdAt(now.minusDays(4))
                .updatedAt(now)
                .paidAt(null)
                .failedAt(null)
                .expiredAt(now)
                .build();
    }
}