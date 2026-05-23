package com.order.application.service;

import com.order.domain.dto.response.ShipmentResponse;
import com.order.domain.event.OrderConfirmedEvent;
import com.order.domain.event.PaymentCompletedEvent;
import reactor.core.publisher.Mono;

public interface ShipmentService {

    Mono<ShipmentResponse> createFromPaymentCompleted(PaymentCompletedEvent event);

    Mono<ShipmentResponse> getById(Long id);

    Mono<ShipmentResponse> getByOrderId(Long orderId);

    Mono<ShipmentResponse> markAsShipped(Long id);

    Mono<ShipmentResponse> markAsDelivered(Long id);
}