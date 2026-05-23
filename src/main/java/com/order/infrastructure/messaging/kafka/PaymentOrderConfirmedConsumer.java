package com.order.infrastructure.messaging.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.order.application.service.PaymentService;
import com.order.domain.entity.Payment;
import com.order.domain.enums.PaymentStatus;
import com.order.domain.event.OrderConfirmedEvent;
import com.order.domain.event.PaymentExpiredEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentOrderConfirmedConsumer {

    private final ObjectMapper objectMapper;
    private final PaymentService paymentService;

    @KafkaListener(
            topics = "#{@orderKafkaProperties.topics.orderConfirmed}",
            groupId = "#{@orderKafkaProperties.consumerGroups.payment}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderConfirmed(String payload) {
        OrderConfirmedEvent event =
                readEvent(payload, OrderConfirmedEvent.class);

        log.info(
                "PAYMENT: Received order.confirmed for orderId={}",
                event.orderId()
        );

        paymentService.createFromOrderConfirmed(event)
                .doOnError(error ->
                        log.error(
                                "PAYMENT: Failed to process payment for orderId={}",
                                event.orderId(),
                                error
                        )
                )
                .onErrorResume(error -> Mono.empty())
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
                    "Failed to deserialize order.confirmed payload: " + payload,
                    ex
            );
        }
    }
}