package com.order.infrastructure.repository;

import com.order.domain.entity.Payment;
import com.order.domain.enums.PaymentStatus;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;

public interface PaymentRepository extends ReactiveCrudRepository<Payment, Long> {

    Mono<Payment> findByOrderId(Long orderId);

    Mono<Boolean> existsByOrderId(Long orderId);

    Flux<Payment> findByStatusAndCreatedAtBefore(
            PaymentStatus status,
            OffsetDateTime createdAt
    );
}