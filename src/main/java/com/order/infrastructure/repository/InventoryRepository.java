package com.order.infrastructure.repository;

import com.order.domain.entity.Inventory;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Mono;

public interface InventoryRepository extends ReactiveCrudRepository<Inventory, Long> {

    Mono<Inventory> findByProductId(Long productId);

    Mono<Boolean> existsByProductId(Long productId);

    @Modifying
    @Query("""
            UPDATE inventory
            SET available_quantity = available_quantity - :quantity,
                reserved_quantity = reserved_quantity + :quantity,
                updated_at = NOW()
            WHERE product_id = :productId
              AND available_quantity >= :quantity
            """)
    Mono<Integer> reserveStock(
            Long productId,
            int quantity
    );

    @Modifying
    @Query("""
            UPDATE inventory
            SET available_quantity = available_quantity + :quantity,
                reserved_quantity = reserved_quantity - :quantity,
                updated_at = NOW()
            WHERE product_id = :productId
              AND reserved_quantity >= :quantity
            """)
    Mono<Integer> releaseStock(
            Long productId,
            int quantity
    );
}
