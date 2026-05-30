package com.order.infrastructure.messaging.kafka;

import com.order.domain.dto.event.ProductCreatedEvent;
import com.order.infrastructure.messaging.KafkaTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import reactor.core.publisher.Mono;

//@Component
@RequiredArgsConstructor
@Slf4j
public class ProductEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public Mono<Void> publishProductCreated(ProductCreatedEvent event) {
        log.info("Start publishing product-created event for productId={}", event.productId());
        return Mono.fromRunnable(() -> {
            kafkaTemplate.send(KafkaTopics.PRODUCT_CREATED,
                    event.productId().toString(),
                    event);
            log.info("Published product-created event for productId={}", event.productId());
        });
    }
}