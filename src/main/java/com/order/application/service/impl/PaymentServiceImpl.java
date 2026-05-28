package com.order.application.service.impl;

import com.order.application.mapper.PaymentMapper;
import com.order.application.service.OutboxService;
import com.order.application.service.PaymentService;
import com.order.domain.dto.response.PaymentResponse;
import com.order.domain.entity.Payment;
import com.order.domain.enums.PaymentStatus;
import com.order.domain.event.*;
import com.order.exception.PaymentAlreadyExistsException;
import com.order.exception.PaymentForOrderNotFoundException;
import com.order.exception.PaymentInvalidStatusTransitionException;
import com.order.exception.PaymentNotFoundException;
import com.order.infrastructure.config.properties.OrderKafkaProperties;
import com.order.infrastructure.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentServiceImpl implements PaymentService {

    private static final String DEFAULT_PROVIDER = "MOCK_PAYMENT_PROVIDER";
    private static final String AGGREGATE_TYPE_PAYMENT = "PAYMENT";
    private static final String EVENT_TYPE_PAYMENT_CREATED = "PAYMENT_CREATED";
    private static final String EVENT_TYPE_PAYMENT_COMPLETED = "PAYMENT_COMPLETED";
    private static final String EVENT_TYPE_PAYMENT_FAILED = "PAYMENT_FAILED";
    private static final String EVENT_TYPE_PAYMENT_EXPIRED = "PAYMENT_EXPIRED";

    private final PaymentRepository repository;
    private final PaymentMapper mapper;
    private final OutboxService outboxService;
    private final OrderKafkaProperties kafkaProperties;
    private final TransactionalOperator transactionalOperator;

    @Override
    public Mono<PaymentResponse> createFromOrderConfirmed(OrderConfirmedEvent event) {
        log.info("Creating payment for confirmed orderId={}", event.orderId());

        if (event.totalAmount() == null) {
            return Mono.error(
                    new IllegalArgumentException(
                            "Order confirmed event does not contain total amount for order id: "
                                    + event.orderId()
                    )
            );
        }

        Mono<PaymentResponse> flow =
                repository.existsByOrderId(event.orderId())
                        .flatMap(exists -> {
                            if (exists) {
                                return Mono.error(
                                        new PaymentAlreadyExistsException(event.orderId())
                                );
                            }

                            Payment payment = buildPayment(event);

                            return repository.save(payment);
                        })
                        .flatMap(savedPayment ->
                                outboxService.saveEvent(
                                                AGGREGATE_TYPE_PAYMENT,
                                                savedPayment.getId(),
                                                EVENT_TYPE_PAYMENT_CREATED,
                                                kafkaProperties.getTopics().getPaymentCreated(),
                                                savedPayment.getOrderId().toString(),
                                                toPaymentCreatedEvent(savedPayment)
                                        )
                                        .thenReturn(savedPayment)
                        )
                        .map(mapper::toResponse);

        return transactionalOperator.transactional(flow);
    }

    @Override
    public Mono<PaymentResponse> getById(Long id) {
        return repository.findById(id)
                .switchIfEmpty(Mono.error(new PaymentNotFoundException(id)))
                .map(mapper::toResponse);
    }

    @Override
    public Mono<PaymentResponse> getByOrderId(Long orderId) {
        return repository.findByOrderId(orderId)
                .switchIfEmpty(
                        Mono.error(
                                new PaymentForOrderNotFoundException(orderId)
                        )
                )
                .map(mapper::toResponse);
    }

    @Override
    public Mono<PaymentResponse> complete(Long id) {
        log.info("Completing payment with id={}", id);

        Mono<PaymentResponse> flow =
                repository.findById(id)
                        .switchIfEmpty(Mono.error(new PaymentNotFoundException(id)))
                        .flatMap(payment -> {
                            if (payment.getStatus() != PaymentStatus.PENDING) {
                                return Mono.error(
                                        new PaymentInvalidStatusTransitionException(
                                                id,
                                                payment.getStatus().name(),
                                                PaymentStatus.COMPLETED.name()
                                        )
                                );
                            }

                            payment.setStatus(PaymentStatus.COMPLETED);
                            payment.setTransactionId(generateTransactionId());
                            payment.setPaidAt(OffsetDateTime.now());
                            payment.setUpdatedAt(OffsetDateTime.now());

                            return repository.save(payment);
                        })
                        .flatMap(savedPayment ->
                                outboxService.saveEvent(
                                                AGGREGATE_TYPE_PAYMENT,
                                                savedPayment.getId(),
                                                EVENT_TYPE_PAYMENT_COMPLETED,
                                                kafkaProperties.getTopics().getPaymentCompleted(),
                                                savedPayment.getOrderId().toString(),
                                                toPaymentCompletedEvent(savedPayment)
                                        )
                                        .thenReturn(savedPayment)
                        )
                        .map(mapper::toResponse);

        return transactionalOperator.transactional(flow);
    }

    @Override
    public Mono<PaymentResponse> fail(Long id, String reason) {
        log.info("Failing payment with id={}, reason={}", id, reason);

        Mono<PaymentResponse> flow =
                repository.findById(id)
                        .switchIfEmpty(Mono.error(new PaymentNotFoundException(id)))
                        .flatMap(payment -> {
                            if (payment.getStatus() != PaymentStatus.PENDING) {
                                return Mono.error(
                                        new PaymentInvalidStatusTransitionException(
                                                id,
                                                payment.getStatus().name(),
                                                PaymentStatus.FAILED.name()
                                        )
                                );
                            }

                            payment.setStatus(PaymentStatus.FAILED);
                            payment.setFailureReason(reason);
                            payment.setFailedAt(OffsetDateTime.now());
                            payment.setUpdatedAt(OffsetDateTime.now());

                            return repository.save(payment);
                        })
                        .flatMap(savedPayment ->
                                outboxService.saveEvent(
                                                AGGREGATE_TYPE_PAYMENT,
                                                savedPayment.getId(),
                                                EVENT_TYPE_PAYMENT_FAILED,
                                                kafkaProperties.getTopics().getPaymentFailed(),
                                                savedPayment.getOrderId().toString(),
                                                toPaymentFailedEvent(savedPayment)
                                        )
                                        .thenReturn(savedPayment)
                        )
                        .map(mapper::toResponse);

        return transactionalOperator.transactional(flow);
    }

    @Override
    public Mono<Integer> expireOverduePayments(OffsetDateTime cutoff) {
        log.info("Expiring overdue pending payments created before {}", cutoff);

        return repository.findByStatusAndCreatedAtBefore(
                        PaymentStatus.PENDING,
                        cutoff
                )
                .flatMap(this::expirePayment)
                .count()
                .map(Long::intValue)
                .doOnNext(count ->
                        log.info("Expired overdue payments count={}", count)
                );
    }

    private Mono<Payment> expirePayment(Payment payment) {
        Mono<Payment> flow = expirePaymentInternal(payment);

        return transactionalOperator.transactional(flow);
    }

    private Mono<Payment> expirePaymentInternal(Payment payment) {
        log.info(
                "Expiring payment. paymentId={}, orderId={}",
                payment.getId(),
                payment.getOrderId()
        );

        OffsetDateTime now = OffsetDateTime.now();
        String reason = "Payment expired before completion";

        payment.setStatus(PaymentStatus.EXPIRED);
        payment.setFailureReason(reason);
        payment.setExpiredAt(now);
        payment.setUpdatedAt(now);

        return repository.save(payment)
                .flatMap(savedPayment ->
                        outboxService.saveEvent(
                                        AGGREGATE_TYPE_PAYMENT,
                                        savedPayment.getId(),
                                        EVENT_TYPE_PAYMENT_EXPIRED,
                                        kafkaProperties.getTopics().getPaymentExpired(),
                                        savedPayment.getOrderId().toString(),
                                        toPaymentExpiredEvent(savedPayment)
                                )
                                .thenReturn(savedPayment)
                );
    }

    private PaymentExpiredEvent toPaymentExpiredEvent(Payment payment) {
        return new PaymentExpiredEvent(
                payment.getId(),
                payment.getOrderId(),
                payment.getAmount(),
                payment.getFailureReason(),
                payment.getExpiredAt()
        );
    }

    private Payment buildPayment(OrderConfirmedEvent event) {
        OffsetDateTime now = OffsetDateTime.now();

        return Payment.builder()
                .orderId(event.orderId())
                .amount(event.totalAmount())
                .status(PaymentStatus.PENDING)
                .provider(DEFAULT_PROVIDER)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private String generateTransactionId() {
        return "TXN-" + UUID.randomUUID()
                .toString()
                .replace("-", "")
                .substring(0, 12)
                .toUpperCase();
    }

    private PaymentCreatedEvent toPaymentCreatedEvent(Payment payment) {
        return new PaymentCreatedEvent(
                payment.getId(),
                payment.getOrderId(),
                payment.getAmount(),
                payment.getProvider(),
                payment.getCreatedAt()
        );
    }

    private PaymentCompletedEvent toPaymentCompletedEvent(Payment payment) {
        return new PaymentCompletedEvent(
                payment.getId(),
                payment.getOrderId(),
                payment.getAmount(),
                payment.getTransactionId(),
                payment.getPaidAt()
        );
    }

    private PaymentFailedEvent toPaymentFailedEvent(Payment payment) {
        return new PaymentFailedEvent(
                payment.getId(),
                payment.getOrderId(),
                payment.getAmount(),
                payment.getFailureReason(),
                payment.getFailedAt()
        );
    }
}