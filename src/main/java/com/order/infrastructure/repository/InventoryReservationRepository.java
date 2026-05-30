package com.order.infrastructure.repository;

import com.order.domain.entity.InventoryReservation;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Mono;

public interface InventoryReservationRepository extends ReactiveCrudRepository<InventoryReservation, Long> {

    @Modifying
    @Query("""
            INSERT INTO inventory_reservations (
                order_id,
                product_id,
                quantity,
                status,
                created_at,
                updated_at
            )
            VALUES (
                :orderId,
                :productId,
                :quantity,
                'RESERVED',
                NOW(),
                NOW()
            )
            ON CONFLICT (order_id, product_id) DO NOTHING
            """)
    Mono<Integer> createReservation(
            Long orderId,
            Long productId,
            int quantity
    );

    @Modifying
    @Query("""
            UPDATE inventory_reservations
            SET status = 'RELEASED',
                updated_at = NOW()
            WHERE order_id = :orderId
              AND product_id = :productId
              AND quantity = :quantity
              AND status = 'RESERVED'
            """)
    Mono<Integer> markReleased(
            Long orderId,
            Long productId,
            int quantity
    );

    @Query("""
            SELECT EXISTS (
                SELECT 1
                FROM inventory_reservations
                WHERE order_id = :orderId
                  AND product_id = :productId
                  AND quantity = :quantity
                  AND status = 'RELEASED'
            )
            """)
    Mono<Boolean> existsReleasedReservation(
            Long orderId,
            Long productId,
            int quantity
    );
}
