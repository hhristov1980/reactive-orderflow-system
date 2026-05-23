package com.order.application.service;

import com.order.domain.dto.response.PaymentResponse;
import com.order.domain.event.OrderConfirmedEvent;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;

public interface PaymentService {

    Mono<PaymentResponse> createFromOrderConfirmed(OrderConfirmedEvent event);

    Mono<PaymentResponse> getById(Long id);

    Mono<PaymentResponse> getByOrderId(Long orderId);

    Mono<PaymentResponse> complete(Long id);

    Mono<PaymentResponse> fail(Long id, String reason);

    Mono<Integer> expireOverduePayments(OffsetDateTime cutoff);
}