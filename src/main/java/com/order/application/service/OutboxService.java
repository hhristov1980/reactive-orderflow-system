package com.order.application.service;

import reactor.core.publisher.Mono;

public interface OutboxService {

    Mono<Void> saveEvent(
            String aggregateType,
            Long aggregateId,
            String eventType,
            String topic,
            String eventKey,
            Object payload
    );

    Mono<Integer> publishPublishableEvents(int maxRetries);
}