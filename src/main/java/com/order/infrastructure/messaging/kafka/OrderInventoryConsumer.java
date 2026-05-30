package com.order.infrastructure.messaging.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.order.application.service.OrderService;
import com.order.domain.event.InventoryFailedEvent;
import com.order.domain.event.InventoryReservedEvent;
import com.order.infrastructure.config.properties.OrderKafkaProperties;
import com.order.infrastructure.observability.KafkaEventMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderInventoryConsumer {

    private final ObjectMapper objectMapper;
    private final OrderService orderService;
    private final OrderKafkaProperties kafkaProperties;
    private final KafkaEventMetrics kafkaEventMetrics;

    @KafkaListener(
            topics = "#{@orderKafkaProperties.topics.inventoryReserved}",
            groupId = "#{@orderKafkaProperties.consumerGroups.order}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeInventoryReserved(String payload) {
        InventoryReservedEvent event =
                readEvent(payload, InventoryReservedEvent.class);
        String topic = kafkaProperties.getTopics().getInventoryReserved();

        log.info(
                "ORDER: Received inventory.reserved for orderId={}",
                event.orderId()
        );

        orderService.confirmFromInventory(event.orderId())
                .doOnSuccess(ignored ->
                        kafkaEventMetrics.recordConsumerSuccess(topic)
                )
                .doOnError(error -> {
                    kafkaEventMetrics.recordConsumerFailure(topic, error);

                    log.error(
                            "ORDER: Failed to confirm order from inventory.reserved. orderId={}",
                            event.orderId(),
                            error
                    );
                })
                .block();
    }

    @KafkaListener(
            topics = "#{@orderKafkaProperties.topics.inventoryFailed}",
            groupId = "#{@orderKafkaProperties.consumerGroups.order}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeInventoryFailed(String payload) {
        InventoryFailedEvent event =
                readEvent(payload, InventoryFailedEvent.class);
        String topic = kafkaProperties.getTopics().getInventoryFailed();

        log.info(
                "ORDER: Received inventory.failed for orderId={}, reason={}",
                event.orderId(),
                event.reason()
        );

        orderService.failFromInventory(
                        event.orderId(),
                        event.reason()
                )
                .doOnSuccess(ignored ->
                        kafkaEventMetrics.recordConsumerSuccess(topic)
                )
                .doOnError(error -> {
                    kafkaEventMetrics.recordConsumerFailure(topic, error);

                    log.error(
                            "ORDER: Failed to mark order as failed from inventory.failed. orderId={}",
                            event.orderId(),
                            error
                    );
                })
                .block();
    }

    private <T> T readEvent(
            String payload,
            Class<T> eventType
    ) {
        try {
            return objectMapper.readValue(payload, eventType);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException(
                    "Failed to deserialize inventory event payload: " + payload,
                    ex
            );
        }
    }
}
