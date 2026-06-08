package com.order.integration;

import com.order.domain.entity.OutboxEvent;
import com.order.domain.enums.OutboxStatus;
import com.order.infrastructure.repository.OutboxEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.r2dbc.test.autoconfigure.DataR2dbcTest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.OffsetDateTime;

@DataR2dbcTest
class OutboxEventRepositoryQueryIntegrationTest extends AbstractPostgresTestcontainersTest {

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Test
    void shouldFindOnlyPublishableEventsOrderedByCreatedAtAscending() {
        StepVerifier.create(
                        outboxEventRepository.deleteAll()
                                .thenMany(outboxEventRepository.saveAll(Flux.just(
                                        newOutboxEvent(OutboxStatus.PUBLISHED, 0, OffsetDateTime.now().minusMinutes(5), 1L),
                                        newOutboxEvent(OutboxStatus.FAILED, 5, OffsetDateTime.now().minusMinutes(4), 2L),
                                        newOutboxEvent(OutboxStatus.PENDING, 0, OffsetDateTime.now().minusMinutes(3), 3L),
                                        newOutboxEvent(OutboxStatus.FAILED, 2, OffsetDateTime.now().minusMinutes(2), 4L),
                                        newOutboxEvent(OutboxStatus.PENDING, 0, OffsetDateTime.now().minusMinutes(1), 5L)
                                )))
                                .thenMany(outboxEventRepository.findPublishableEvents(3, 10))
                                .collectList()
                )
                .expectNextMatches(events ->
                        events.size() == 3
                                && events.get(0).getStatus() == OutboxStatus.PENDING
                                && events.get(0).getAggregateId().equals(3L)
                                && events.get(1).getStatus() == OutboxStatus.FAILED
                                && events.get(1).getAggregateId().equals(4L)
                                && events.get(2).getStatus() == OutboxStatus.PENDING
                                && events.get(2).getAggregateId().equals(5L)
                )
                .verifyComplete();
    }

    @Test
    void shouldRespectPublishableEventsLimit() {
        StepVerifier.create(
                        outboxEventRepository.deleteAll()
                                .thenMany(outboxEventRepository.saveAll(Flux.just(
                                        newOutboxEvent(OutboxStatus.PENDING, 0, OffsetDateTime.now().minusMinutes(3), 11L),
                                        newOutboxEvent(OutboxStatus.PENDING, 0, OffsetDateTime.now().minusMinutes(2), 12L),
                                        newOutboxEvent(OutboxStatus.PENDING, 0, OffsetDateTime.now().minusMinutes(1), 13L)
                                )))
                                .thenMany(outboxEventRepository.findPublishableEvents(3, 2))
                                .collectList()
                )
                .expectNextMatches(events ->
                        events.size() == 2
                                && events.get(0).getAggregateId().equals(11L)
                                && events.get(1).getAggregateId().equals(12L)
                )
                .verifyComplete();
    }

    @Test
    void shouldCountAllOutboxEvents() {
        StepVerifier.create(
                        outboxEventRepository.deleteAll()
                                .thenMany(outboxEventRepository.saveAll(Flux.just(
                                        newOutboxEvent(OutboxStatus.PENDING, 0, OffsetDateTime.now().minusMinutes(3), 21L),
                                        newOutboxEvent(OutboxStatus.PUBLISHED, 0, OffsetDateTime.now().minusMinutes(2), 22L),
                                        newOutboxEvent(OutboxStatus.FAILED, 1, OffsetDateTime.now().minusMinutes(1), 23L)
                                )))
                                .then(outboxEventRepository.countAll())
                )
                .expectNext(3L)
                .verifyComplete();
    }

    @Test
    void shouldCountOutboxEventsByStatus() {
        StepVerifier.create(
                        outboxEventRepository.deleteAll()
                                .thenMany(outboxEventRepository.saveAll(Flux.just(
                                        newOutboxEvent(OutboxStatus.PENDING, 0, OffsetDateTime.now().minusMinutes(4), 31L),
                                        newOutboxEvent(OutboxStatus.PENDING, 0, OffsetDateTime.now().minusMinutes(3), 32L),
                                        newOutboxEvent(OutboxStatus.PUBLISHED, 0, OffsetDateTime.now().minusMinutes(2), 33L),
                                        newOutboxEvent(OutboxStatus.FAILED, 1, OffsetDateTime.now().minusMinutes(1), 34L)
                                )))
                                .then(outboxEventRepository.countByStatus(OutboxStatus.PENDING))
                )
                .expectNext(2L)
                .verifyComplete();
    }

    @Test
    void shouldFindOutboxEventsByStatusWithPaging() {
        StepVerifier.create(
                        outboxEventRepository.deleteAll()
                                .thenMany(outboxEventRepository.saveAll(Flux.just(
                                        newOutboxEvent(OutboxStatus.PENDING, 0, OffsetDateTime.now().minusMinutes(5), 41L),
                                        newOutboxEvent(OutboxStatus.FAILED, 1, OffsetDateTime.now().minusMinutes(4), 42L),
                                        newOutboxEvent(OutboxStatus.PENDING, 0, OffsetDateTime.now().minusMinutes(3), 43L),
                                        newOutboxEvent(OutboxStatus.PENDING, 0, OffsetDateTime.now().minusMinutes(2), 44L),
                                        newOutboxEvent(OutboxStatus.PUBLISHED, 0, OffsetDateTime.now().minusMinutes(1), 45L)
                                )))
                                .thenMany(outboxEventRepository.findByStatusPaged(OutboxStatus.PENDING, 2, 0))
                                .collectList()
                )
                .expectNextMatches(events ->
                        events.size() == 2
                                && events.stream().allMatch(event -> event.getStatus() == OutboxStatus.PENDING)
                                && events.get(0).getAggregateId().equals(44L)
                                && events.get(1).getAggregateId().equals(43L)
                )
                .verifyComplete();
    }

    @Test
    void shouldFindAllPagedOrderedByCreatedAtDescending() {
        StepVerifier.create(
                        outboxEventRepository.deleteAll()
                                .thenMany(outboxEventRepository.saveAll(Flux.just(
                                        newOutboxEvent(OutboxStatus.PENDING, 0, OffsetDateTime.now().minusMinutes(3), 51L),
                                        newOutboxEvent(OutboxStatus.PUBLISHED, 0, OffsetDateTime.now().minusMinutes(2), 52L),
                                        newOutboxEvent(OutboxStatus.FAILED, 1, OffsetDateTime.now().minusMinutes(1), 53L)
                                )))
                                .thenMany(outboxEventRepository.findAllPaged(2, 0))
                                .collectList()
                )
                .expectNextMatches(events ->
                        events.size() == 2
                                && events.get(0).getAggregateId().equals(53L)
                                && events.get(1).getAggregateId().equals(52L)
                )
                .verifyComplete();
    }

    private static OutboxEvent newOutboxEvent(
            OutboxStatus status,
            int retryCount,
            OffsetDateTime createdAt,
            Long aggregateId
    ) {
        return OutboxEvent.builder()
                .aggregateType("ORDER")
                .aggregateId(aggregateId)
                .eventType("ORDER_CREATED")
                .topic("order.created")
                .eventKey("order-" + aggregateId)
                .payload("""
                        {
                          "eventType": "ORDER_CREATED",
                          "aggregateType": "ORDER",
                          "aggregateId": %d
                        }
                        """.formatted(aggregateId))
                .status(status)
                .retryCount(retryCount)
                .lastError(status == OutboxStatus.FAILED ? "Test failure" : null)
                .createdAt(createdAt)
                .updatedAt(createdAt)
                .publishedAt(status == OutboxStatus.PUBLISHED ? createdAt.plusSeconds(10) : null)
                .build();
    }
}