package com.order.infrastructure.messaging.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.order.application.service.InventoryService;
import com.order.application.service.OutboxService;
import com.order.domain.event.InventoryFailedEvent;
import com.order.domain.event.OrderCancelledEvent;
import com.order.domain.event.OrderCreatedEvent;
import com.order.infrastructure.config.properties.OrderKafkaProperties;
import com.order.infrastructure.observability.KafkaEventMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryOrderEventConsumer {

    private static final String AGGREGATE_TYPE_INVENTORY = "INVENTORY";
    private static final String EVENT_TYPE_INVENTORY_FAILED = "INVENTORY_FAILED";

    private final ObjectMapper objectMapper;
    private final InventoryService inventoryService;
    private final OutboxService outboxService;
    private final OrderKafkaProperties kafkaProperties;
    private final KafkaEventMetrics kafkaEventMetrics;

    @KafkaListener(
            topics = "#{@orderKafkaProperties.topics.orderCreated}",
            groupId = "#{@orderKafkaProperties.consumerGroups.inventory}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderCreated(String payload) {
        OrderCreatedEvent event =
                readEvent(payload, OrderCreatedEvent.class);
        String topic = kafkaProperties.getTopics().getOrderCreated();

        log.info(
                "INVENTORY: Received order.created for orderId={}",
                event.orderId()
        );

        inventoryService.reserve(event)
                .doOnSuccess(reservedEvent ->
                        log.info(
                                "INVENTORY: Inventory reserved and outbox event saved for orderId={}",
                                reservedEvent.orderId()
                        )
                )
                .then()
                .doOnSuccess(ignored ->
                        kafkaEventMetrics.recordConsumerSuccess(topic)
                )
                .onErrorResume(error -> {
                    kafkaEventMetrics.recordInventoryReservationFailure(error);

                    return saveInventoryFailedEvent(event, error)
                            .doOnSuccess(ignored ->
                                    kafkaEventMetrics.recordConsumerSuccess(topic)
                            );
                })
                .doOnError(error ->
                        kafkaEventMetrics.recordConsumerFailure(topic, error)
                )
                .block();
    }

    @KafkaListener(
            topics = "#{@orderKafkaProperties.topics.orderCancelled}",
            groupId = "#{@orderKafkaProperties.consumerGroups.inventory}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderCancelled(String payload) {
        OrderCancelledEvent event =
                readEvent(payload, OrderCancelledEvent.class);
        String topic = kafkaProperties.getTopics().getOrderCancelled();

        log.info(
                "INVENTORY: Received order.cancelled for orderId={}",
                event.orderId()
        );

        inventoryService.release(event)
                .doOnSuccess(releasedEvent ->
                        log.info(
                                "INVENTORY: Inventory released and outbox event saved for orderId={}",
                                releasedEvent.orderId()
                        )
                )
                .then()
                .doOnSuccess(ignored ->
                        kafkaEventMetrics.recordConsumerSuccess(topic)
                )
                .doOnError(error -> {
                    kafkaEventMetrics.recordConsumerFailure(topic, error);

                    log.error(
                            "INVENTORY: Failed to release inventory for cancelled orderId={}",
                            event.orderId(),
                            error
                    );
                })
                .block();
    }

    private Mono<Void> saveInventoryFailedEvent(
            OrderCreatedEvent event,
            Throwable error
    ) {
        log.error(
                "INVENTORY: Failed to reserve inventory for orderId={}",
                event.orderId(),
                error
        );

        InventoryFailedEvent failedEvent =
                new InventoryFailedEvent(
                        event.orderId(),
                        error.getMessage(),
                        OffsetDateTime.now()
                );

        return outboxService.saveEvent(
                AGGREGATE_TYPE_INVENTORY,
                event.orderId(),
                EVENT_TYPE_INVENTORY_FAILED,
                kafkaProperties.getTopics().getInventoryFailed(),
                event.orderId().toString(),
                failedEvent
        );
    }

    private <T> T readEvent(
            String payload,
            Class<T> eventType
    ) {
        try {
            return objectMapper.readValue(payload, eventType);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException(
                    "Failed to deserialize inventory order event payload: " + payload,
                    ex
            );
        }
    }
}
