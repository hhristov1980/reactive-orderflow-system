package com.order.infrastructure.repository;

import com.order.domain.entity.Shipment;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Mono;

public interface ShipmentRepository extends ReactiveCrudRepository<Shipment, Long> {

    Mono<Shipment> findByOrderId(Long orderId);

    Mono<Boolean> existsByOrderId(Long orderId);
}