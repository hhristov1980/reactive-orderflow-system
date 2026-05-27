package com.order.application.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.order.application.service.OutboxService;
import com.order.domain.entity.OutboxEvent;
import com.order.domain.enums.OutboxStatus;
import com.order.infrastructure.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;

@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxServiceImpl implements OutboxService {

    private final OutboxEventRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Override
    public Mono<Void> saveEvent(
            String aggregateType,
            Long aggregateId,
            String eventType,
            String topic,
            String eventKey,
            Object payload
    ) {
        OutboxEvent event = OutboxEvent.builder()
                .aggregateType(aggregateType)
                .aggregateId(aggregateId)
                .eventType(eventType)
                .topic(topic)
                .eventKey(eventKey)
                .payload(toJson(payload))
                .status(OutboxStatus.PENDING)
                .retryCount(0)
                .createdAt(OffsetDateTime.now())
                .updatedAt(OffsetDateTime.now())
                .build();

        return repository.save(event)
                .doOnSuccess(saved ->
                        log.info(
                                "Outbox event saved. id={}, eventType={}, aggregateType={}, aggregateId={}",
                                saved.getId(),
                                saved.getEventType(),
                                saved.getAggregateType(),
                                saved.getAggregateId()
                        )
                )
                .then();
    }

    @Override
    public Mono<Integer> publishPublishableEvents(int maxRetries) {
        int limit = 50;

        return repository.findPublishableEvents(maxRetries, limit)
                .flatMap(this::publishSingleEvent)
                .count()
                .map(Long::intValue)
                .doOnNext(count ->
                        log.info(
                                "Outbox publishing completed. processedCount={}, maxRetries={}",
                                count,
                                maxRetries
                        )
                );
    }

    private Mono<OutboxEvent> publishSingleEvent(OutboxEvent event) {
        log.info(
                "Publishing outbox event. id={}, topic={}, key={}",
                event.getId(),
                event.getTopic(),
                event.getEventKey()
        );

        return Mono.fromFuture(
                        kafkaTemplate.send(
                                event.getTopic(),
                                event.getEventKey(),
                                event.getPayload()
                        )
                )
                .then(markAsPublished(event))
                .onErrorResume(error -> markAsFailed(event, error));
    }

    private Mono<OutboxEvent> markAsPublished(OutboxEvent event) {
        OffsetDateTime now = OffsetDateTime.now();

        event.setStatus(OutboxStatus.PUBLISHED);
        event.setPublishedAt(now);
        event.setUpdatedAt(now);
        event.setLastError(null);

        return repository.save(event)
                .doOnSuccess(saved ->
                        log.info(
                                "Outbox event published. id={}, topic={}, key={}",
                                saved.getId(),
                                saved.getTopic(),
                                saved.getEventKey()
                        )
                );
    }

    private Mono<OutboxEvent> markAsFailed(
            OutboxEvent event,
            Throwable error
    ) {
        OffsetDateTime now = OffsetDateTime.now();

        int currentRetryCount =
                event.getRetryCount() == null
                        ? 0
                        : event.getRetryCount();

        event.setStatus(OutboxStatus.FAILED);
        event.setRetryCount(currentRetryCount + 1);
        event.setLastError(error.getMessage());
        event.setUpdatedAt(now);

        return repository.save(event)
                .doOnSuccess(saved ->
                        log.error(
                                "Outbox event failed. id={}, topic={}, key={}, retryCount={}, error={}",
                                saved.getId(),
                                saved.getTopic(),
                                saved.getEventKey(),
                                saved.getRetryCount(),
                                error.getMessage()
                        )
                );
    }

    private String toJson(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException(
                    "Failed to serialize outbox payload",
                    ex
            );
        }
    }
}