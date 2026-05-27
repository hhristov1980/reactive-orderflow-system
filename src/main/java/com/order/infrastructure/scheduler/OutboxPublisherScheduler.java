package com.order.infrastructure.scheduler;

import com.order.application.service.OutboxService;
import com.order.infrastructure.config.properties.OutboxSchedulerProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxPublisherScheduler {

    private final OutboxService outboxService;
    private final OutboxSchedulerProperties properties;

    @Scheduled(
            fixedDelayString = "${orderflow.scheduler.outbox.fixed-delay-ms}"
    )
    public void publishPendingOutboxEvents() {
        if (!properties.isEnabled()) {
            log.debug("Outbox publisher scheduler is disabled");
            return;
        }

        outboxService.publishPublishableEvents(properties.getMaxRetries())
                .doOnNext(count -> {
                    if (count > 0) {
                        log.info(
                                "Outbox publisher processed {} events",
                                count
                        );
                    }
                })
                .doOnError(error ->
                        log.error(
                                "Outbox publisher failed",
                                error
                        )
                )
                .subscribe();
    }
}