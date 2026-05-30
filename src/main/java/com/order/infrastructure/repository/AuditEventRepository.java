package com.order.infrastructure.repository;

import com.order.domain.entity.AuditEvent;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface AuditEventRepository extends ReactiveCrudRepository<AuditEvent, Long> {

    @Query("""
            SELECT *
            FROM audit_events
            ORDER BY created_at DESC
            LIMIT :limit
            OFFSET :offset
            """)
    Flux<AuditEvent> findAllPaged(
            int limit,
            long offset
    );

    @Query("""
            SELECT COUNT(*)
            FROM audit_events
            """)
    Mono<Long> countAll();

    @Query("""
            SELECT *
            FROM audit_events
            WHERE aggregate_type = :aggregateType
              AND aggregate_id = :aggregateId
            ORDER BY created_at DESC
            LIMIT :limit
            OFFSET :offset
            """)
    Flux<AuditEvent> findByAggregatePaged(
            String aggregateType,
            Long aggregateId,
            int limit,
            long offset
    );

    @Query("""
            SELECT COUNT(*)
            FROM audit_events
            WHERE aggregate_type = :aggregateType
              AND aggregate_id = :aggregateId
            """)
    Mono<Long> countByAggregate(
            String aggregateType,
            Long aggregateId
    );
}