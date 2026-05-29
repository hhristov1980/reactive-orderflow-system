package com.order.infrastructure.repository;

import com.order.domain.entity.OutboxEvent;
import com.order.domain.enums.OutboxStatus;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface OutboxEventRepository extends ReactiveCrudRepository<OutboxEvent, Long> {

    @Query("""
            SELECT *
            FROM outbox_events
            WHERE status = 'PENDING'
               OR (status = 'FAILED' AND retry_count < :maxRetries)
            ORDER BY created_at ASC
            LIMIT :limit
            """)
    Flux<OutboxEvent> findPublishableEvents(
            int maxRetries,
            int limit
    );

    @Query("""
            SELECT *
            FROM outbox_events
            ORDER BY created_at DESC
            LIMIT :limit
            OFFSET :offset
            """)
    Flux<OutboxEvent> findAllPaged(
            int limit,
            long offset
    );

    @Query("""
            SELECT COUNT(*)
            FROM outbox_events
            """)
    Mono<Long> countAll();

    @Query("""
            SELECT *
            FROM outbox_events
            WHERE status = :status
            ORDER BY created_at DESC
            LIMIT :limit
            OFFSET :offset
            """)
    Flux<OutboxEvent> findByStatusPaged(
            OutboxStatus status,
            int limit,
            long offset
    );

    @Query("""
            SELECT COUNT(*)
            FROM outbox_events
            WHERE status = :status
            """)
    Mono<Long> countByStatus(
            OutboxStatus status
    );
}