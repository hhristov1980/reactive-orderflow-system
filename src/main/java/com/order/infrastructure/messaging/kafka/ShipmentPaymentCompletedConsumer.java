//package com.order.infrastructure.messaging.kafka;
//
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.order.application.service.ShipmentService;
//import com.order.domain.event.PaymentCompletedEvent;
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.stereotype.Component;
//import reactor.core.publisher.Mono;
//
//@Component
//@RequiredArgsConstructor
//@Slf4j
//public class ShipmentPaymentCompletedConsumer {
//
//    private final ObjectMapper objectMapper;
//    private final ShipmentService shipmentService;
//
//    @KafkaListener(
//            topics = "#{@orderKafkaProperties.topics.paymentCompleted}",
//            groupId = "#{@orderKafkaProperties.consumerGroups.shipment}",
//            containerFactory = "kafkaListenerContainerFactory"
//    )
//    public void consumePaymentCompleted(String payload) {
//        PaymentCompletedEvent event =
//                readEvent(payload, PaymentCompletedEvent.class);
//
//        log.info(
//                "SHIPMENT: Received payment.completed for orderId={}",
//                event.orderId()
//        );
//
//        shipmentService.createFromPaymentCompleted(event)
//                .doOnError(error ->
//                        log.error(
//                                "SHIPMENT: Failed to create shipment for paid orderId={}",
//                                event.orderId(),
//                                error
//                        )
//                )
//                .onErrorResume(error -> Mono.empty())
//                .subscribe();
//    }
//
//    private <T> T readEvent(
//            String payload,
//            Class<T> eventType
//    ) {
//        try {
//            return objectMapper.readValue(payload, eventType);
//        } catch (JsonProcessingException ex) {
//            throw new IllegalArgumentException(
//                    "Failed to deserialize payment.completed payload: " + payload,
//                    ex
//            );
//        }
//    }
//}