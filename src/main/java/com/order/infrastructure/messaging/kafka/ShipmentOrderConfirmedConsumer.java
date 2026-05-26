//package com.order.infrastructure.messaging.kafka;
//
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.order.application.service.ShipmentService;
//import com.order.domain.event.OrderConfirmedEvent;
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.stereotype.Component;
//import reactor.core.publisher.Mono;
//
//@Component
//@RequiredArgsConstructor
//@Slf4j
//public class ShipmentOrderConfirmedConsumer {
//
//    private final ObjectMapper objectMapper;
//    private final ShipmentService shipmentService;
//
//    @KafkaListener(
//            topics = "#{@orderKafkaProperties.topics.orderConfirmed}",
//            groupId = "#{@orderKafkaProperties.consumerGroups.shipment}",
//            containerFactory = "kafkaListenerContainerFactory"
//    )
//    public void consumeOrderConfirmed(String payload) {
//        OrderConfirmedEvent event =
//                readEvent(payload, OrderConfirmedEvent.class);
//
//        log.info(
//                "SHIPMENT: Received order.confirmed for orderId={}",
//                event.orderId()
//        );
//
//        shipmentService.createFromOrderConfirmed(event)
//                .doOnError(error ->
//                        log.error(
//                                "SHIPMENT: Failed to create shipment for orderId={}",
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
//                    "Failed to deserialize order.confirmed payload: " + payload,
//                    ex
//            );
//        }
//    }
//}
