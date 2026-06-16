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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@DataR2dbcTest
class PaymentRepositoryQueryIntegrationTest extends AbstractPostgresTestcontainersTest {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private PaymentRepository paymentRepository;

    @Test
    void shouldFindOnlyPendingPaymentsCreatedBeforeCutoff() {
        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime cutoff = now.minusDays(3);

        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newActiveCustomer()))
                                .flatMapMany(savedUser ->
                                        createOrdersAndPayments(savedUser.getId(), now)
                                )
                                .thenMany(paymentRepository.findByStatusAndCreatedAtBefore(
                                        PaymentStatus.PENDING,
                                        cutoff
                                ))
                                .collectList()
                )
                .expectNextMatches(payments ->
                        payments.size() == 1
                                && payments.getFirst().getStatus() == PaymentStatus.PENDING
                                && payments.getFirst().getCreatedAt().isBefore(cutoff)
                                && payments.getFirst().getExpiredAt() == null
                                && payments.getFirst().getPaidAt() == null
                                && payments.getFirst().getFailedAt() == null
                )
                .verifyComplete();
    }

    @Test
    void shouldFindPaymentByOrderId() {
        OffsetDateTime now = OffsetDateTime.now();

        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newActiveCustomer()))
                                .flatMap(savedUser ->
                                        orderRepository.save(newConfirmedOrder(savedUser.getId(), now))
                                )
                                .flatMap(savedOrder ->
                                        paymentRepository.save(newPayment(
                                                        savedOrder.getId(),
                                                        PaymentStatus.PENDING,
                                                        now.minusDays(4)
                                                ))
                                                .then(paymentRepository.findByOrderId(savedOrder.getId()))
                                )
                )
                .expectNextMatches(payment ->
                        payment.getOrderId() != null
                                && payment.getStatus() == PaymentStatus.PENDING
                                && payment.getAmount().compareTo(new BigDecimal("49.80")) == 0
                )
                .verifyComplete();
    }

    @Test
    void shouldCheckIfPaymentExistsByOrderId() {
        OffsetDateTime now = OffsetDateTime.now();

        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newActiveCustomer()))
                                .flatMap(savedUser ->
                                        orderRepository.save(newConfirmedOrder(savedUser.getId(), now))
                                )
                                .flatMap(savedOrder ->
                                        paymentRepository.save(newPayment(
                                                        savedOrder.getId(),
                                                        PaymentStatus.PENDING,
                                                        now.minusDays(4)
                                                ))
                                                .then(paymentRepository.existsByOrderId(savedOrder.getId()))
                                )
                )
                .expectNext(true)
                .verifyComplete();
    }

    private Flux<Payment> createOrdersAndPayments(Long userId, OffsetDateTime now) {
        Order oldPendingOrder = newConfirmedOrder(userId, now.minusDays(5));
        Order recentPendingOrder = newConfirmedOrder(userId, now.minusDays(1));
        Order oldCompletedOrder = newConfirmedOrder(userId, now.minusDays(5));
        Order oldFailedOrder = newConfirmedOrder(userId, now.minusDays(5));
        Order oldExpiredOrder = newConfirmedOrder(userId, now.minusDays(5));

        return orderRepository.saveAll(Flux.just(
                        oldPendingOrder,
                        recentPendingOrder,
                        oldCompletedOrder,
                        oldFailedOrder,
                        oldExpiredOrder
                ))
                .collectList()
                .flatMapMany(savedOrders -> paymentRepository.saveAll(Flux.just(
                        newPayment(savedOrders.get(0).getId(), PaymentStatus.PENDING, now.minusDays(5)),
                        newPayment(savedOrders.get(1).getId(), PaymentStatus.PENDING, now.minusDays(1)),
                        newCompletedPayment(savedOrders.get(2).getId(), now.minusDays(5)),
                        newFailedPayment(savedOrders.get(3).getId(), now.minusDays(5)),
                        newExpiredPayment(savedOrders.get(4).getId(), now.minusDays(5), now)
                )));
    }

    private Mono<Void> cleanDatabase() {
        return paymentRepository.deleteAll()
                .then(orderRepository.deleteAll())
                .then(userRepository.deleteAll());
    }

    private static User newActiveCustomer() {
        OffsetDateTime now = OffsetDateTime.now();

        return User.builder()
                .email("payment.query.user." + UUID.randomUUID() + "@example.com")
                .name("Payment Query Test User " + UUID.randomUUID())
                .role(UserRole.CUSTOMER)
                .status(UserStatus.ACTIVE)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private static Order newConfirmedOrder(Long userId, OffsetDateTime createdAt) {
        return Order.builder()
                .userId(userId)
                .status(OrderStatus.CONFIRMED)
                .totalAmount(new BigDecimal("49.80"))
                .createdAt(createdAt)
                .updatedAt(createdAt)
                .build();
    }

    private static Payment newPayment(
            Long orderId,
            PaymentStatus status,
            OffsetDateTime createdAt
    ) {
        return Payment.builder()
                .orderId(orderId)
                .status(status)
                .amount(new BigDecimal("49.80"))
                .provider("TEST_PROVIDER")
                .transactionId(null)
                .failureReason(null)
                .createdAt(createdAt)
                .updatedAt(createdAt)
                .paidAt(null)
                .failedAt(null)
                .expiredAt(null)
                .build();
    }

    private static Payment newCompletedPayment(Long orderId, OffsetDateTime createdAt) {
        OffsetDateTime paidAt = createdAt.plusMinutes(30);

        return Payment.builder()
                .orderId(orderId)
                .status(PaymentStatus.COMPLETED)
                .amount(new BigDecimal("49.80"))
                .provider("TEST_PROVIDER")
                .transactionId("tx-" + UUID.randomUUID())
                .failureReason(null)
                .createdAt(createdAt)
                .updatedAt(paidAt)
                .paidAt(paidAt)
                .failedAt(null)
                .expiredAt(null)
                .build();
    }

    private static Payment newFailedPayment(Long orderId, OffsetDateTime createdAt) {
        OffsetDateTime failedAt = createdAt.plusMinutes(30);

        return Payment.builder()
                .orderId(orderId)
                .status(PaymentStatus.FAILED)
                .amount(new BigDecimal("49.80"))
                .provider("TEST_PROVIDER")
                .transactionId(null)
                .failureReason("Test payment failure")
                .createdAt(createdAt)
                .updatedAt(failedAt)
                .paidAt(null)
                .failedAt(failedAt)
                .expiredAt(null)
                .build();
    }

    private static Payment newExpiredPayment(
            Long orderId,
            OffsetDateTime createdAt,
            OffsetDateTime expiredAt
    ) {
        return Payment.builder()
                .orderId(orderId)
                .status(PaymentStatus.EXPIRED)
                .amount(new BigDecimal("49.80"))
                .provider("TEST_PROVIDER")
                .transactionId(null)
                .failureReason(null)
                .createdAt(createdAt)
                .updatedAt(expiredAt)
                .paidAt(null)
                .failedAt(null)
                .expiredAt(expiredAt)
                .build();
    }
}