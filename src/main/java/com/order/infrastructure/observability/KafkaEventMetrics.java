package com.order.infrastructure.observability;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KafkaEventMetrics {

    private static final String CONSUMER_EVENTS = "orderflow.kafka.consumer.events";
    private static final String DLT_EVENTS = "orderflow.kafka.dlt.events";
    private static final String INVENTORY_RESERVATION_FAILURES =
            "orderflow.inventory.reservation.failures";

    private final MeterRegistry meterRegistry;

    public void recordConsumerSuccess(String topic) {
        recordConsumerEvent(topic, "success", "none");
    }

    public void recordConsumerDuplicate(String topic) {
        recordConsumerEvent(topic, "duplicate", "none");
    }

    public void recordConsumerFailure(String topic, Throwable error) {
        recordConsumerEvent(topic, "failure", exceptionName(error));
    }

    public void recordInventoryReservationFailure(Throwable error) {
        meterRegistry.counter(
                INVENTORY_RESERVATION_FAILURES,
                "exception",
                exceptionName(error)
        ).increment();
    }

    public void recordDeadLetterPublished(
            ConsumerRecord<?, ?> record,
            String deadLetterTopic,
            Exception exception
    ) {
        meterRegistry.counter(
                DLT_EVENTS,
                "source.topic",
                record.topic(),
                "dlt.topic",
                deadLetterTopic,
                "exception",
                exceptionName(exception)
        ).increment();
    }

    private void recordConsumerEvent(
            String topic,
            String outcome,
            String exception
    ) {
        meterRegistry.counter(
                CONSUMER_EVENTS,
                "topic",
                topic,
                "outcome",
                outcome,
                "exception",
                exception
        ).increment();
    }

    private String exceptionName(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null) {
            current = current.getCause();
        }

        return current.getClass().getSimpleName();
    }
}
