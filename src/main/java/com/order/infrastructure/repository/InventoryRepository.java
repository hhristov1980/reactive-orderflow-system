package com.order.infrastructure.repository;

import com.order.domain.entity.Inventory;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Mono;

public interface InventoryRepository extends ReactiveCrudRepository<Inventory, Long> {

    Mono<Inventory> findByProductId(Long productId);

    Mono<Boolean> existsByProductId(Long productId);
}