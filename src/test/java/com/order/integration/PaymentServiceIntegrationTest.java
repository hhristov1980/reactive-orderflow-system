package com.order.integration;

import com.order.application.mapper.PaymentMapper;
import com.order.application.service.OutboxService;
import com.order.application.service.impl.PaymentServiceImpl;
import com.order.domain.dto.response.PaymentResponse;
import com.order.domain.entity.Payment;
import com.order.domain.enums.PaymentStatus;
import com.order.domain.event.OrderConfirmedEvent;
import com.order.exception.PaymentAlreadyExistsException;
import com.order.exception.PaymentInvalidStatusTransitionException;
import com.order.infrastructure.config.properties.OrderKafkaProperties;
import com.order.infrastructure.repository.PaymentRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.data.r2dbc.test.autoconfigure.DataR2dbcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@DataR2dbcTest
@Import(PaymentServiceImpl.class)
@EnableConfigurationProperties(OrderKafkaProperties.class)
@TestPropertySource(properties = {
        "orderflow.kafka.topics.payment-created=payment.created",
        "orderflow.kafka.topics.payment-completed=payment.completed",
        "orderflow.kafka.topics.payment-failed=payment.failed",
        "orderflow.kafka.topics.payment-expired=payment.expired"
})
class PaymentServiceIntegrationTest extends AbstractPostgresTestcontainersTest {

    @Autowired
    private PaymentServiceImpl paymentService;

    @Autowired
    private PaymentRepository paymentRepository;

    @MockitoBean
    private PaymentMapper paymentMapper;

    @MockitoBean
    private OutboxService outboxService;

    @Test
    void shouldExpireOnlyOverduePendingPaymentsAndSaveOutboxEvents() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime cutoff = now.minusDays(2);

        AtomicReference<Long> overduePendingPaymentId = new AtomicReference<>();
        AtomicReference<Long> recentPendingPaymentId = new AtomicReference<>();
        AtomicReference<Long> completedPaymentId = new AtomicReference<>();
        AtomicReference<Long> failedPaymentId = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(paymentRepository.save(newPayment(
                                        101L,
                                        PaymentStatus.PENDING,
                                        new BigDecimal("100.00"),
                                        now.minusDays(5)
                                )))
                                .doOnNext(payment -> overduePendingPaymentId.set(payment.getId()))
                                .then(paymentRepository.save(newPayment(
                                        102L,
                                        PaymentStatus.PENDING,
                                        new BigDecimal("50.00"),
                                        now.minusHours(6)
                                )))
                                .doOnNext(payment -> recentPendingPaymentId.set(payment.getId()))
                                .then(paymentRepository.save(newCompletedPayment(
                                        103L,
                                        new BigDecimal("70.00"),
                                        now.minusDays(5)
                                )))
                                .doOnNext(payment -> completedPaymentId.set(payment.getId()))
                                .then(paymentRepository.save(newFailedPayment(
                                        104L,
                                        new BigDecimal("80.00"),
                                        now.minusDays(5)
                                )))
                                .doOnNext(payment -> failedPaymentId.set(payment.getId()))
                                .then(paymentService.expireOverduePayments(cutoff))
                )
                .expectNext(1)
                .verifyComplete();

        StepVerifier.create(paymentRepository.findById(overduePendingPaymentId.get()))
                .expectNextMatches(payment ->
                        payment.getStatus() == PaymentStatus.EXPIRED
                                && "Payment expired before completion".equals(payment.getFailureReason())
                                && payment.getExpiredAt() != null
                )
                .verifyComplete();

        StepVerifier.create(paymentRepository.findById(recentPendingPaymentId.get()))
                .expectNextMatches(payment ->
                        payment.getStatus() == PaymentStatus.PENDING
                                && payment.getExpiredAt() == null
                                && payment.getFailureReason() == null
                )
                .verifyComplete();

        StepVerifier.create(paymentRepository.findById(completedPaymentId.get()))
                .expectNextMatches(payment ->
                        payment.getStatus() == PaymentStatus.COMPLETED
                                && payment.getPaidAt() != null
                                && payment.getExpiredAt() == null
                )
                .verifyComplete();

        StepVerifier.create(paymentRepository.findById(failedPaymentId.get()))
                .expectNextMatches(payment ->
                        payment.getStatus() == PaymentStatus.FAILED
                                && payment.getFailedAt() != null
                                && payment.getExpiredAt() == null
                )
                .verifyComplete();

        verify(outboxService, times(1)).saveEvent(
                eq("PAYMENT"),
                eq(overduePendingPaymentId.get()),
                eq("PAYMENT_EXPIRED"),
                eq("payment.expired"),
                eq("101"),
                any()
        );
    }

    @Test
    void shouldReturnZeroWhenNoOverduePendingPaymentsExist() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime cutoff = now.minusDays(2);

        StepVerifier.create(
                        cleanDatabase()
                                .then(paymentRepository.save(newPayment(
                                        201L,
                                        PaymentStatus.PENDING,
                                        new BigDecimal("40.00"),
                                        now.minusHours(3)
                                )))
                                .then(paymentRepository.save(newCompletedPayment(
                                        202L,
                                        new BigDecimal("90.00"),
                                        now.minusDays(5)
                                )))
                                .then(paymentService.expireOverduePayments(cutoff))
                )
                .expectNext(0)
                .verifyComplete();

        verify(outboxService, times(0)).saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        );
    }

    @Test
    void shouldRollbackExpiredPaymentWhenOutboxSaveFails() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.error(new RuntimeException("Outbox failure")));

        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime cutoff = now.minusDays(2);

        AtomicReference<Long> paymentId = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(paymentRepository.save(newPayment(
                                        301L,
                                        PaymentStatus.PENDING,
                                        new BigDecimal("120.00"),
                                        now.minusDays(5)
                                )))
                                .doOnNext(payment -> paymentId.set(payment.getId()))
                                .then(paymentService.expireOverduePayments(cutoff))
                )
                .expectError(RuntimeException.class)
                .verify();

        StepVerifier.create(paymentRepository.findById(paymentId.get()))
                .expectNextMatches(payment ->
                        payment.getStatus() == PaymentStatus.PENDING
                                && payment.getFailureReason() == null
                                && payment.getExpiredAt() == null
                )
                .verifyComplete();

        verify(outboxService, times(1)).saveEvent(
                eq("PAYMENT"),
                eq(paymentId.get()),
                eq("PAYMENT_EXPIRED"),
                eq("payment.expired"),
                eq("301"),
                any()
        );
    }

    @Test
    void shouldCompletePendingPaymentAndSaveOutboxEvent() {
        PaymentResponse response = mock(PaymentResponse.class);

        when(paymentMapper.toResponse(any(Payment.class)))
                .thenReturn(response);

        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        OffsetDateTime now = OffsetDateTime.now();
        AtomicReference<Long> paymentId = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(paymentRepository.save(newPayment(
                                        401L,
                                        PaymentStatus.PENDING,
                                        new BigDecimal("99.90"),
                                        now.minusHours(1)
                                )))
                                .doOnNext(payment -> paymentId.set(payment.getId()))
                                .flatMap(savedPayment -> paymentService.complete(savedPayment.getId()))
                )
                .expectNext(response)
                .verifyComplete();

        StepVerifier.create(paymentRepository.findById(paymentId.get()))
                .expectNextMatches(payment ->
                        payment.getStatus() == PaymentStatus.COMPLETED
                                && payment.getTransactionId() != null
                                && payment.getTransactionId().startsWith("TXN-")
                                && payment.getPaidAt() != null
                                && payment.getFailureReason() == null
                                && payment.getFailedAt() == null
                                && payment.getExpiredAt() == null
                )
                .verifyComplete();

        verify(outboxService).saveEvent(
                eq("PAYMENT"),
                eq(paymentId.get()),
                eq("PAYMENT_COMPLETED"),
                eq("payment.completed"),
                eq("401"),
                any()
        );
    }

    @Test
    void shouldFailPendingPaymentAndSaveOutboxEvent() {
        PaymentResponse response = mock(PaymentResponse.class);

        when(paymentMapper.toResponse(any(Payment.class)))
                .thenReturn(response);

        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        OffsetDateTime now = OffsetDateTime.now();
        AtomicReference<Long> paymentId = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(paymentRepository.save(newPayment(
                                        501L,
                                        PaymentStatus.PENDING,
                                        new BigDecimal("49.90"),
                                        now.minusHours(1)
                                )))
                                .doOnNext(payment -> paymentId.set(payment.getId()))
                                .flatMap(savedPayment ->
                                        paymentService.fail(
                                                savedPayment.getId(),
                                                "Card declined"
                                        )
                                )
                )
                .expectNext(response)
                .verifyComplete();

        StepVerifier.create(paymentRepository.findById(paymentId.get()))
                .expectNextMatches(payment ->
                        payment.getStatus() == PaymentStatus.FAILED
                                && "Card declined".equals(payment.getFailureReason())
                                && payment.getFailedAt() != null
                                && payment.getTransactionId() == null
                                && payment.getPaidAt() == null
                                && payment.getExpiredAt() == null
                )
                .verifyComplete();

        verify(outboxService).saveEvent(
                eq("PAYMENT"),
                eq(paymentId.get()),
                eq("PAYMENT_FAILED"),
                eq("payment.failed"),
                eq("501"),
                any()
        );
    }

    @Test
    void shouldRejectCompletingNonPendingPayment() {
        PaymentResponse response = mock(PaymentResponse.class);

        when(paymentMapper.toResponse(any(Payment.class)))
                .thenReturn(response);

        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        OffsetDateTime now = OffsetDateTime.now();
        AtomicReference<Long> paymentId = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(paymentRepository.save(newCompletedPayment(
                                        601L,
                                        new BigDecimal("79.90"),
                                        now.minusHours(2)
                                )))
                                .doOnNext(payment -> paymentId.set(payment.getId()))
                                .flatMap(savedPayment -> paymentService.complete(savedPayment.getId()))
                )
                .expectError(PaymentInvalidStatusTransitionException.class)
                .verify();

        StepVerifier.create(paymentRepository.findById(paymentId.get()))
                .expectNextMatches(payment ->
                        payment.getStatus() == PaymentStatus.COMPLETED
                                && payment.getPaidAt() != null
                                && payment.getTransactionId() != null
                )
                .verifyComplete();

        verify(outboxService, never()).saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        );
    }

    @Test
    void shouldCreatePendingPaymentFromOrderConfirmedEventAndSaveOutboxEvent() {
        PaymentResponse response = mock(PaymentResponse.class);

        when(paymentMapper.toResponse(any(Payment.class)))
                .thenReturn(response);

        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> paymentId = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(paymentService.createFromOrderConfirmed(new OrderConfirmedEvent(
                                        701L,
                                        1001L,
                                        new BigDecimal("149.90"),
                                        OffsetDateTime.now()
                                )))
                )
                .expectNext(response)
                .verifyComplete();

        StepVerifier.create(paymentRepository.findByOrderId(701L))
                .expectNextMatches(payment -> {
                    paymentId.set(payment.getId());

                    return payment.getOrderId().equals(701L)
                            && payment.getAmount().compareTo(new BigDecimal("149.90")) == 0
                            && payment.getStatus() == PaymentStatus.PENDING
                            && "MOCK_PAYMENT_PROVIDER".equals(payment.getProvider())
                            && payment.getTransactionId() == null
                            && payment.getFailureReason() == null
                            && payment.getPaidAt() == null
                            && payment.getFailedAt() == null
                            && payment.getExpiredAt() == null;
                })
                .verifyComplete();

        verify(outboxService).saveEvent(
                eq("PAYMENT"),
                eq(paymentId.get()),
                eq("PAYMENT_CREATED"),
                eq("payment.created"),
                eq("701"),
                any()
        );
    }

    @Test
    void shouldRejectCreatingPaymentWhenPaymentAlreadyExistsForOrder() {
        PaymentResponse response = mock(PaymentResponse.class);

        when(paymentMapper.toResponse(any(Payment.class)))
                .thenReturn(response);

        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        OffsetDateTime now = OffsetDateTime.now();

        StepVerifier.create(
                        cleanDatabase()
                                .then(paymentRepository.save(newPayment(
                                        801L,
                                        PaymentStatus.PENDING,
                                        new BigDecimal("59.90"),
                                        now.minusMinutes(5)
                                )))
                                .then(paymentService.createFromOrderConfirmed(new OrderConfirmedEvent(
                                        801L,
                                        1001L,
                                        new BigDecimal("59.90"),
                                        OffsetDateTime.now()
                                )))
                )
                .expectError(PaymentAlreadyExistsException.class)
                .verify();

        StepVerifier.create(paymentRepository.findByOrderId(801L))
                .expectNextMatches(payment ->
                        payment.getOrderId().equals(801L)
                                && payment.getStatus() == PaymentStatus.PENDING
                                && payment.getAmount().compareTo(new BigDecimal("59.90")) == 0
                )
                .verifyComplete();

        verify(outboxService, never()).saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        );
    }

    private Mono<Void> cleanDatabase() {
        return paymentRepository.deleteAll();
    }

    private static Payment newPayment(
            Long orderId,
            PaymentStatus status,
            BigDecimal amount,
            OffsetDateTime createdAt
    ) {
        return Payment.builder()
                .orderId(orderId)
                .status(status)
                .amount(amount)
                .provider("TEST_PROVIDER")
                .createdAt(createdAt)
                .updatedAt(createdAt)
                .build();
    }

    private static Payment newCompletedPayment(
            Long orderId,
            BigDecimal amount,
            OffsetDateTime createdAt
    ) {
        OffsetDateTime paidAt = createdAt.plusMinutes(10);

        return Payment.builder()
                .orderId(orderId)
                .status(PaymentStatus.COMPLETED)
                .amount(amount)
                .provider("TEST_PROVIDER")
                .transactionId("TXN-" + orderId)
                .createdAt(createdAt)
                .updatedAt(paidAt)
                .paidAt(paidAt)
                .build();
    }

    private static Payment newFailedPayment(
            Long orderId,
            BigDecimal amount,
            OffsetDateTime createdAt
    ) {
        OffsetDateTime failedAt = createdAt.plusMinutes(10);

        return Payment.builder()
                .orderId(orderId)
                .status(PaymentStatus.FAILED)
                .amount(amount)
                .provider("TEST_PROVIDER")
                .failureReason("Test failure")
                .createdAt(createdAt)
                .updatedAt(failedAt)
                .failedAt(failedAt)
                .build();
    }
}