package com.order.infrastructure.messaging.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.order.application.service.PaymentService;
import com.order.domain.event.OrderConfirmedEvent;
import com.order.exception.PaymentAlreadyExistsException;
import com.order.infrastructure.config.properties.OrderKafkaProperties;
import com.order.infrastructure.observability.KafkaEventMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentOrderConfirmedConsumer {

    private final ObjectMapper objectMapper;
    private final PaymentService paymentService;
    private final OrderKafkaProperties kafkaProperties;
    private final KafkaEventMetrics kafkaEventMetrics;

    @KafkaListener(
            topics = "#{@orderKafkaProperties.topics.orderConfirmed}",
            groupId = "#{@orderKafkaProperties.consumerGroups.payment}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderConfirmed(String payload) {
        OrderConfirmedEvent event =
                readEvent(payload, OrderConfirmedEvent.class);
        String topic = kafkaProperties.getTopics().getOrderConfirmed();

        log.info(
                "PAYMENT: Received order.confirmed for orderId={}",
                event.orderId()
        );

        paymentService.createFromOrderConfirmed(event)
                .doOnSuccess(payment ->
                        kafkaEventMetrics.recordConsumerSuccess(topic)
                )
                .onErrorResume(PaymentAlreadyExistsException.class, error -> {
                    kafkaEventMetrics.recordConsumerDuplicate(topic);

                    log.info(
                            "PAYMENT: Payment already exists for orderId={}; treating duplicate order.confirmed as processed",
                            event.orderId()
                    );
                    return Mono.empty();
                })
                .doOnError(error -> {
                    kafkaEventMetrics.recordConsumerFailure(topic, error);

                    log.error(
                            "PAYMENT: Failed to process payment for orderId={}",
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
                    "Failed to deserialize order.confirmed payload: " + payload,
                    ex
            );
        }
    }
}
