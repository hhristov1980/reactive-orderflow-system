package com.order.infrastructure.messaging.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.order.domain.event.PaymentCompletedEvent;
import com.order.domain.event.PaymentCreatedEvent;
import com.order.domain.event.PaymentExpiredEvent;
import com.order.domain.event.PaymentFailedEvent;
import com.order.infrastructure.config.properties.OrderKafkaProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final OrderKafkaProperties kafkaProperties;

    public Mono<Void> publishPaymentCreated(PaymentCreatedEvent event) {
        return publish(
                kafkaProperties.getTopics().getPaymentCreated(),
                event.orderId().toString(),
                event
        );
    }

    public Mono<Void> publishPaymentCompleted(PaymentCompletedEvent event) {
        return publish(
                kafkaProperties.getTopics().getPaymentCompleted(),
                event.orderId().toString(),
                event
        );
    }

    public Mono<Void> publishPaymentFailed(PaymentFailedEvent event) {
        return publish(
                kafkaProperties.getTopics().getPaymentFailed(),
                event.orderId().toString(),
                event
        );
    }

    public Mono<Void> publishPaymentExpired(PaymentExpiredEvent event) {
        return publish(
                kafkaProperties.getTopics().getPaymentExpired(),
                event.orderId().toString(),
                event
        );
    }

    private Mono<Void> publish(
            String topic,
            String key,
            Object event
    ) {
        return Mono.fromCallable(() -> toJson(event))
                .flatMap(payload ->
                        Mono.fromFuture(
                                kafkaTemplate.send(topic, key, payload)
                        )
                )
                .doOnSuccess(result ->
                        log.info(
                                "Published payment event to topic={}, key={}",
                                topic,
                                key
                        )
                )
                .doOnError(error ->
                        log.error(
                                "Failed to publish payment event to topic={}, key={}",
                                topic,
                                key,
                                error
                        )
                )
                .then();
    }

    private String toJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException(
                    "Failed to serialize payment Kafka event",
                    ex
            );
        }
    }
}