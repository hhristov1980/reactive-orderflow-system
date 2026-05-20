package com.order.infrastructure.messaging.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.order.domain.event.InventoryFailedEvent;
import com.order.domain.event.InventoryReleasedEvent;
import com.order.domain.event.InventoryReservedEvent;
import com.order.infrastructure.config.properties.OrderKafkaProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final OrderKafkaProperties kafkaProperties;

    public Mono<Void> publishInventoryReserved(InventoryReservedEvent event) {
        return publish(
                kafkaProperties.getTopics().getInventoryReserved(),
                event.orderId().toString(),
                event
        );
    }

    public Mono<Void> publishInventoryFailed(InventoryFailedEvent event) {
        return publish(
                kafkaProperties.getTopics().getInventoryFailed(),
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
                                "Published inventory event to topic={}, key={}",
                                topic,
                                key
                        )
                )
                .doOnError(error ->
                        log.error(
                                "Failed to publish inventory event to topic={}, key={}",
                                topic,
                                key,
                                error
                        )
                )
                .then();
    }

    public Mono<Void> publishInventoryReleased(InventoryReleasedEvent event) {
        return publish(
                kafkaProperties.getTopics().getInventoryReleased(),
                event.orderId().toString(),
                event
        );
    }

    private String toJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException(
                    "Failed to serialize inventory Kafka event",
                    ex
            );
        }
    }
}