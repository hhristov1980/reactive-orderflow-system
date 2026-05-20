package com.order.application.service;

import com.order.domain.dto.response.ShipmentResponse;
import com.order.domain.event.OrderConfirmedEvent;
import reactor.core.publisher.Mono;

public interface ShipmentService {

    Mono<ShipmentResponse> createFromOrderConfirmed(OrderConfirmedEvent event);

    Mono<ShipmentResponse> getById(Long id);

    Mono<ShipmentResponse> getByOrderId(Long orderId);

    Mono<ShipmentResponse> markAsShipped(Long id);

    Mono<ShipmentResponse> markAsDelivered(Long id);
}