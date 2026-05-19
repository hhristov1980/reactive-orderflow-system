package com.order.application.service;

import reactor.core.publisher.Mono;

public interface AuditService {

    Mono<Void> saveEvent(
            String eventType,
            String aggregateType,
            Long aggregateId,
            String payload
    );
}