package com.order.infrastructure.scheduler;

import com.order.application.service.OutboxService;
import com.order.infrastructure.config.properties.OutboxSchedulerProperties;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxPublisherSchedulerTest {

    private final OutboxService outboxService = mock(OutboxService.class);
    private final OutboxSchedulerProperties properties =
            new OutboxSchedulerProperties();

    private final OutboxPublisherScheduler scheduler =
            new OutboxPublisherScheduler(outboxService, properties);

    @Test
    void shouldNotRunWhenSchedulerIsDisabled() {
        properties.setEnabled(false);
        properties.setMaxRetries(3);
        properties.setFixedDelayMs(60_000L);

        scheduler.publishPendingOutboxEvents();

        verify(outboxService, never()).publishPublishableEvents(anyInt());
    }

    @Test
    void shouldPublishEventsWhenSchedulerIsEnabled() {
        properties.setEnabled(true);
        properties.setMaxRetries(3);
        properties.setFixedDelayMs(60_000L);

        when(outboxService.publishPublishableEvents(3))
                .thenReturn(Mono.just(5));

        scheduler.publishPendingOutboxEvents();

        verify(outboxService).publishPublishableEvents(3);
    }

    @Test
    void shouldUseConfiguredMaxRetries() {
        properties.setEnabled(true);
        properties.setMaxRetries(7);
        properties.setFixedDelayMs(60_000L);

        when(outboxService.publishPublishableEvents(7))
                .thenReturn(Mono.just(2));

        scheduler.publishPendingOutboxEvents();

        verify(outboxService).publishPublishableEvents(7);
    }

    @Test
    void shouldSubscribeAndHandleServiceErrorWithoutThrowing() {
        properties.setEnabled(true);
        properties.setMaxRetries(3);
        properties.setFixedDelayMs(60_000L);

        when(outboxService.publishPublishableEvents(3))
                .thenReturn(Mono.error(new RuntimeException("Outbox publishing failed")));

        scheduler.publishPendingOutboxEvents();

        verify(outboxService).publishPublishableEvents(3);
    }

    @Test
    void shouldHandleZeroProcessedEvents() {
        properties.setEnabled(true);
        properties.setMaxRetries(3);
        properties.setFixedDelayMs(60_000L);

        when(outboxService.publishPublishableEvents(3))
                .thenReturn(Mono.just(0));

        scheduler.publishPendingOutboxEvents();

        verify(outboxService).publishPublishableEvents(3);
    }
}