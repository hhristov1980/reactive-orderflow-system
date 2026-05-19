package com.order.infrastructure.messaging.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.order.domain.event.OrderCancelledEvent;
import com.order.domain.event.OrderConfirmedEvent;
import com.order.domain.event.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderAuditConsumer {

    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "#{@orderKafkaProperties.topics.orderCreated}",
            groupId = "#{@orderKafkaProperties.consumerGroupId}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderCreated(String payload) {

        OrderCreatedEvent event =
                readEvent(payload, OrderCreatedEvent.class);

        log.info(
                "AUDIT: Order created. orderId={}, userId={}, totalAmount={}, items={}",
                event.orderId(),
                event.userId(),
                event.totalAmount(),
                event.items()
        );
    }

    @KafkaListener(
            topics = "#{@orderKafkaProperties.topics.orderConfirmed}",
            groupId = "#{@orderKafkaProperties.consumerGroupId}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderConfirmed(String payload) {

        OrderConfirmedEvent event =
                readEvent(payload, OrderConfirmedEvent.class);

        log.info(
                "AUDIT: Order confirmed. orderId={}, userId={}, confirmedAt={}",
                event.orderId(),
                event.userId(),
                event.confirmedAt()
        );
    }

    @KafkaListener(
            topics = "#{@orderKafkaProperties.topics.orderCancelled}",
            groupId = "#{@orderKafkaProperties.consumerGroupId}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderCancelled(String payload) {

        OrderCancelledEvent event =
                readEvent(payload, OrderCancelledEvent.class);

        log.info(
                "AUDIT: Order cancelled. orderId={}, userId={}, cancelledAt={}",
                event.orderId(),
                event.userId(),
                event.cancelledAt()
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
                    "Failed to deserialize Kafka event payload: " + payload,
                    ex
            );
        }
    }
}