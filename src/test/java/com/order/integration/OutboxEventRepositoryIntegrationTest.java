package com.order.integration;

import com.order.domain.entity.OutboxEvent;
import com.order.domain.enums.OutboxStatus;
import com.order.infrastructure.repository.OutboxEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.r2dbc.test.autoconfigure.DataR2dbcTest;
import reactor.test.StepVerifier;

import java.time.OffsetDateTime;
import java.util.UUID;

@DataR2dbcTest
class OutboxEventRepositoryIntegrationTest extends AbstractPostgresTestcontainersTest{

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Test
    void shouldSaveAndFindOutboxEvent() {
        OutboxEvent outboxEvent = newPendingOrderCreatedOutboxEvent();

        Long expectedAggregateId = outboxEvent.getAggregateId();
        String expectedEventKey = outboxEvent.getEventKey();

        StepVerifier.create(
                        outboxEventRepository.deleteAll()
                                .then(outboxEventRepository.save(outboxEvent))
                                .flatMap(saved -> outboxEventRepository.findById(saved.getId()))
                )
                .expectNextMatches(saved ->
                        saved.getAggregateType().equals("ORDER")
                                && saved.getAggregateId().equals(expectedAggregateId)
                                && saved.getEventType().equals("ORDER_CREATED")
                                && saved.getTopic().equals("order.created")
                                && saved.getEventKey().equals(expectedEventKey)
                                && saved.getPayload().contains("ORDER_CREATED")
                                && saved.getStatus() == OutboxStatus.PENDING
                                && saved.getRetryCount() == 0
                                && saved.getLastError() == null
                                && saved.getPublishedAt() == null
                )
                .verifyComplete();
    }

    private static OutboxEvent newPendingOrderCreatedOutboxEvent() {
        OffsetDateTime now = OffsetDateTime.now();
        Long aggregateId = System.currentTimeMillis();
        String eventKey = UUID.randomUUID().toString();

        return OutboxEvent.builder()
                .aggregateType("ORDER")
                .aggregateId(aggregateId)
                .eventType("ORDER_CREATED")
                .topic("order.created")
                .eventKey(eventKey)
                .payload("""
                        {
                          "eventType": "ORDER_CREATED",
                          "aggregateType": "ORDER",
                          "aggregateId": %d
                        }
                        """.formatted(aggregateId))
                .status(OutboxStatus.PENDING)
                .retryCount(0)
                .lastError(null)
                .createdAt(now)
                .updatedAt(now)
                .publishedAt(null)
                .build();
    }
}