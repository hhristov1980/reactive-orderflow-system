package com.order.infrastructure.messaging.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.order.domain.event.ShipmentCreatedEvent;
import com.order.domain.event.ShipmentDeliveredEvent;
import com.order.domain.event.ShipmentShippedEvent;
import com.order.infrastructure.config.properties.OrderKafkaProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
@Slf4j
public class ShipmentEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final OrderKafkaProperties kafkaProperties;

    public Mono<Void> publishShipmentCreated(ShipmentCreatedEvent event) {
        return publish(
                kafkaProperties.getTopics().getShipmentCreated(),
                event.orderId().toString(),
                event
        );
    }

    public Mono<Void> publishShipmentShipped(ShipmentShippedEvent event) {
        return publish(
                kafkaProperties.getTopics().getShipmentShipped(),
                event.orderId().toString(),
                event
        );
    }

    public Mono<Void> publishShipmentDelivered(ShipmentDeliveredEvent event) {
        return publish(
                kafkaProperties.getTopics().getShipmentDelivered(),
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
                                "Published shipment event to topic={}, key={}",
                                topic,
                                key
                        )
                )
                .doOnError(error ->
                        log.error(
                                "Failed to publish shipment event to topic={}, key={}",
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
                    "Failed to serialize shipment Kafka event",
                    ex
            );
        }
    }
}