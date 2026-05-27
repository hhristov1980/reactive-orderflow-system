package com.order.infrastructure.repository;

import com.order.domain.entity.OutboxEvent;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

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
}