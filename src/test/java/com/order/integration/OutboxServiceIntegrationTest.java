package com.order.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.order.application.service.impl.OutboxServiceImpl;
import com.order.domain.entity.OutboxEvent;
import com.order.domain.enums.OutboxStatus;
import com.order.infrastructure.repository.OutboxEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.r2dbc.test.autoconfigure.DataR2dbcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.OffsetDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DataR2dbcTest
@Import({
        OutboxServiceImpl.class,
        OutboxServiceIntegrationTest.TestConfig.class
})
class OutboxServiceIntegrationTest extends AbstractPostgresTestcontainersTest {

    @Autowired
    private OutboxServiceImpl outboxService;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @MockitoBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void shouldSavePendingOutboxEventWithSerializedPayload() {
        TestPayload payload = new TestPayload(
                31L,
                "ORDER_CREATED",
                OffsetDateTime.parse("2026-05-30T16:52:14Z")
        );

        StepVerifier.create(
                        cleanDatabase()
                                .then(outboxService.saveEvent(
                                        "ORDER",
                                        31L,
                                        "ORDER_CREATED",
                                        "order.created",
                                        "31",
                                        payload
                                ))
                                .thenMany(outboxEventRepository.findAll())
                                .collectList()
                )
                .expectNextMatches(events ->
                        events.size() == 1
                                && events.get(0).getAggregateType().equals("ORDER")
                                && events.get(0).getAggregateId().equals(31L)
                                && events.get(0).getEventType().equals("ORDER_CREATED")
                                && events.get(0).getTopic().equals("order.created")
                                && events.get(0).getEventKey().equals("31")
                                && events.get(0).getStatus() == OutboxStatus.PENDING
                                && events.get(0).getRetryCount() == 0
                                && events.get(0).getPayload() != null
                                && events.get(0).getPayload().contains("\"orderId\":31")
                                && events.get(0).getPayload().contains("\"eventType\":\"ORDER_CREATED\"")
                                && events.get(0).getCreatedAt() != null
                                && events.get(0).getUpdatedAt() != null
                                && events.get(0).getPublishedAt() == null
                                && events.get(0).getLastError() == null
                )
                .verifyComplete();
    }

    @Test
    void shouldPublishPendingEventAndMarkItAsPublished() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(successfulSend());

        AtomicReference<Long> eventIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(outboxEventRepository.save(newOutboxEvent(
                                        OutboxStatus.PENDING,
                                        0,
                                        null
                                )))
                                .doOnNext(savedEvent -> eventIdRef.set(savedEvent.getId()))
                                .then(outboxService.publishPublishableEvents(3))
                )
                .expectNext(1)
                .verifyComplete();

        StepVerifier.create(outboxEventRepository.findById(eventIdRef.get()))
                .expectNextMatches(event ->
                        event.getStatus() == OutboxStatus.PUBLISHED
                                && event.getRetryCount() == 0
                                && event.getPublishedAt() != null
                                && event.getLastError() == null
                )
                .verifyComplete();

        verify(kafkaTemplate).send(
                "order.created",
                "31",
                "{\"eventType\":\"ORDER_CREATED\"}"
        );
    }

    @Test
    void shouldMarkPendingEventAsFailedAndIncrementRetryCountWhenKafkaPublishFails() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(failedSend("Kafka unavailable"));

        AtomicReference<Long> eventIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(outboxEventRepository.save(newOutboxEvent(
                                        OutboxStatus.PENDING,
                                        0,
                                        null
                                )))
                                .doOnNext(savedEvent -> eventIdRef.set(savedEvent.getId()))
                                .then(outboxService.publishPublishableEvents(3))
                )
                .expectNext(1)
                .verifyComplete();

        StepVerifier.create(outboxEventRepository.findById(eventIdRef.get()))
                .expectNextMatches(event ->
                        event.getStatus() == OutboxStatus.FAILED
                                && event.getRetryCount() == 1
                                && event.getPublishedAt() == null
                                && event.getLastError() != null
                                && event.getLastError().contains("Kafka unavailable")
                )
                .verifyComplete();

        verify(kafkaTemplate).send(
                "order.created",
                "31",
                "{\"eventType\":\"ORDER_CREATED\"}"
        );
    }

    @Test
    void shouldRetryFailedEventWhenRetryCountIsBelowMaxRetries() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(successfulSend());

        AtomicReference<Long> eventIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(outboxEventRepository.save(newOutboxEvent(
                                        OutboxStatus.FAILED,
                                        2,
                                        "Previous failure"
                                )))
                                .doOnNext(savedEvent -> eventIdRef.set(savedEvent.getId()))
                                .then(outboxService.publishPublishableEvents(3))
                )
                .expectNext(1)
                .verifyComplete();

        StepVerifier.create(outboxEventRepository.findById(eventIdRef.get()))
                .expectNextMatches(event ->
                        event.getStatus() == OutboxStatus.PUBLISHED
                                && event.getRetryCount() == 2
                                && event.getPublishedAt() != null
                                && event.getLastError() == null
                )
                .verifyComplete();

        verify(kafkaTemplate).send(
                "order.created",
                "31",
                "{\"eventType\":\"ORDER_CREATED\"}"
        );
    }

    @Test
    void shouldSkipFailedEventWhenRetryCountReachedMaxRetries() {
        AtomicReference<Long> eventIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(outboxEventRepository.save(newOutboxEvent(
                                        OutboxStatus.FAILED,
                                        3,
                                        "Previous failure"
                                )))
                                .doOnNext(savedEvent -> eventIdRef.set(savedEvent.getId()))
                                .then(outboxService.publishPublishableEvents(3))
                )
                .expectNext(0)
                .verifyComplete();

        StepVerifier.create(outboxEventRepository.findById(eventIdRef.get()))
                .expectNextMatches(event ->
                        event.getStatus() == OutboxStatus.FAILED
                                && event.getRetryCount() == 3
                                && event.getPublishedAt() == null
                                && "Previous failure".equals(event.getLastError())
                )
                .verifyComplete();

        verify(kafkaTemplate, never()).send(
                anyString(),
                anyString(),
                anyString()
        );
    }

    @Test
    void shouldProcessMultiplePublishableEvents() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(successfulSend());

        StepVerifier.create(
                        cleanDatabase()
                                .then(outboxEventRepository.save(newOutboxEvent(
                                        OutboxStatus.PENDING,
                                        0,
                                        null
                                )))
                                .then(outboxEventRepository.save(newOutboxEvent(
                                        OutboxStatus.FAILED,
                                        1,
                                        "Previous failure"
                                )))
                                .then(outboxService.publishPublishableEvents(3))
                )
                .expectNext(2)
                .verifyComplete();

        StepVerifier.create(outboxEventRepository.findAll().collectList())
                .expectNextMatches(events ->
                        events.size() == 2
                                && events.stream().allMatch(event ->
                                event.getStatus() == OutboxStatus.PUBLISHED
                                        && event.getPublishedAt() != null
                                        && event.getLastError() == null
                        )
                )
                .verifyComplete();
    }

    private Mono<Void> cleanDatabase() {
        return outboxEventRepository.deleteAll();
    }

    private static OutboxEvent newOutboxEvent(
            OutboxStatus status,
            int retryCount,
            String lastError
    ) {
        OffsetDateTime now = OffsetDateTime.now();

        return OutboxEvent.builder()
                .aggregateType("ORDER")
                .aggregateId(31L)
                .eventType("ORDER_CREATED")
                .topic("order.created")
                .eventKey("31")
                .payload("{\"eventType\":\"ORDER_CREATED\"}")
                .status(status)
                .retryCount(retryCount)
                .lastError(lastError)
                .createdAt(now)
                .updatedAt(now)
                .publishedAt(null)
                .build();
    }

    private static CompletableFuture<SendResult<String, String>> successfulSend() {
        return CompletableFuture.completedFuture(null);
    }

    private static CompletableFuture<SendResult<String, String>> failedSend(String message) {
        CompletableFuture<SendResult<String, String>> future =
                new CompletableFuture<>();

        future.completeExceptionally(new RuntimeException(message));

        return future;
    }

    private record TestPayload(
            Long orderId,
            String eventType,
            OffsetDateTime createdAt
    ) {
    }

    @TestConfiguration
    static class TestConfig {

        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapper()
                    .registerModule(new JavaTimeModule());
        }
    }
}