package com.order.infrastructure.messaging.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.order.application.service.OrderService;
import com.order.application.service.ShipmentService;
import com.order.domain.event.PaymentCompletedEvent;
import com.order.domain.event.PaymentExpiredEvent;
import com.order.domain.event.PaymentFailedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventConsumer {

    private final ObjectMapper objectMapper;
    private final ShipmentService shipmentService;
    private final OrderService orderService;

    @KafkaListener(
            topics = "#{@orderKafkaProperties.topics.paymentCompleted}",
            groupId = "#{@orderKafkaProperties.consumerGroups.shipment}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumePaymentCompleted(String payload) {
        PaymentCompletedEvent event =
                readEvent(payload, PaymentCompletedEvent.class);

        log.info(
                "PAYMENT: Received payment.completed for orderId={}",
                event.orderId()
        );

        shipmentService.createFromPaymentCompleted(event)
                .doOnError(error ->
                        log.error(
                                "PAYMENT: Failed to create shipment from payment.completed. orderId={}",
                                event.orderId(),
                                error
                        )
                )
                .onErrorResume(error -> Mono.empty())
                .subscribe();
    }

    @KafkaListener(
            topics = "#{@orderKafkaProperties.topics.paymentFailed}",
            groupId = "#{@orderKafkaProperties.consumerGroups.order}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumePaymentFailed(String payload) {
        PaymentFailedEvent event =
                readEvent(payload, PaymentFailedEvent.class);

        log.info(
                "PAYMENT: Received payment.failed for orderId={}, reason={}",
                event.orderId(),
                event.reason()
        );

        orderService.cancelFromPaymentFailure(
                        event.orderId(),
                        event.reason()
                )
                .doOnError(error ->
                        log.error(
                                "PAYMENT: Failed to cancel order from payment.failed. orderId={}",
                                event.orderId(),
                                error
                        )
                )
                .onErrorResume(error -> Mono.empty())
                .subscribe();
    }

    @KafkaListener(
            topics = "#{@orderKafkaProperties.topics.paymentExpired}",
            groupId = "#{@orderKafkaProperties.consumerGroups.order}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumePaymentExpired(String payload) {
        PaymentExpiredEvent event = readEvent(payload, PaymentExpiredEvent.class);

        log.info(
                "PAYMENT: Received payment.expired for orderId={}, reason={}",
                event.orderId(),
                event.reason()
        );

        orderService.cancelFromPaymentFailure(
                        event.orderId(),
                        event.reason()
                )
                .doOnError(error ->
                        log.error(
                                "PAYMENT: Failed to cancel order from payment.expired. orderId={}",
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
                    "Failed to deserialize payment event payload: " + payload,
                    ex
            );
        }
    }
}