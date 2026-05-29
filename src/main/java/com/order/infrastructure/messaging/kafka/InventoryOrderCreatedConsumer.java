package com.order.infrastructure.messaging.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.order.application.service.InventoryService;
import com.order.domain.event.InventoryFailedEvent;
import com.order.domain.event.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryOrderCreatedConsumer {

    private final ObjectMapper objectMapper;
    private final InventoryService inventoryService;
    private final InventoryEventProducer inventoryEventProducer;

    @KafkaListener(
            topics = "#{@orderKafkaProperties.topics.orderCreated}",
            groupId = "#{@orderKafkaProperties.consumerGroups.inventory}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderCreated(String payload) {
        OrderCreatedEvent event =
                readEvent(payload, OrderCreatedEvent.class);

        log.info(
                "INVENTORY: Received order.created for orderId={}",
                event.orderId()
        );

        inventoryService.reserve(event)
                .flatMap(inventoryEventProducer::publishInventoryReserved)
                .onErrorResume(error -> {
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

                    return inventoryEventProducer.publishInventoryFailed(failedEvent);
                })
                .subscribe();
    }

    private <T> T readEvent(
            String payload,
            Class<T> eventType
    ) {
        try {
            return objectMapper.readValue(payload, eventType);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException(
                    "Failed to deserialize order.created payload: " + payload,
                    ex
            );
        }
    }
}