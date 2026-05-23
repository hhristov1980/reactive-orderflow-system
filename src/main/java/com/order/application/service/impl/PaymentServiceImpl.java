package com.order.application.service.impl;

import com.order.application.mapper.PaymentMapper;
import com.order.application.service.PaymentService;
import com.order.domain.dto.response.PaymentResponse;
import com.order.domain.entity.Payment;
import com.order.domain.enums.PaymentStatus;
import com.order.domain.event.*;
import com.order.exception.PaymentAlreadyExistsException;
import com.order.exception.PaymentForOrderNotFoundException;
import com.order.exception.PaymentInvalidStatusTransitionException;
import com.order.exception.PaymentNotFoundException;
import com.order.infrastructure.messaging.kafka.PaymentEventProducer;
import com.order.infrastructure.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentServiceImpl implements PaymentService {

    private static final String DEFAULT_PROVIDER = "MOCK_PAYMENT_PROVIDER";

    private final PaymentRepository repository;
    private final PaymentMapper mapper;
    private final PaymentEventProducer producer;

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

        return repository.existsByOrderId(event.orderId())
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
                        producer.publishPaymentCreated(
                                        toPaymentCreatedEvent(savedPayment)
                                )
                                .thenReturn(savedPayment)
                )
                .map(mapper::toResponse);
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

        return repository.findById(id)
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
                        producer.publishPaymentCompleted(
                                        toPaymentCompletedEvent(savedPayment)
                                )
                                .thenReturn(savedPayment)
                )
                .map(mapper::toResponse);
    }

    @Override
    public Mono<PaymentResponse> fail(Long id, String reason) {
        log.info("Failing payment with id={}, reason={}", id, reason);

        return repository.findById(id)
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
                        producer.publishPaymentFailed(
                                        toPaymentFailedEvent(savedPayment)
                                )
                                .thenReturn(savedPayment)
                )
                .map(mapper::toResponse);
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
                        producer.publishPaymentExpired(
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