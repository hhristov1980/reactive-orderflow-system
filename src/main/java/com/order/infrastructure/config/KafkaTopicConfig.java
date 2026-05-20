package com.order.infrastructure.config;

import com.order.infrastructure.config.properties.OrderKafkaProperties;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@RequiredArgsConstructor
public class KafkaTopicConfig {

    private final OrderKafkaProperties kafkaProperties;

    @Bean
    public NewTopic orderCreatedTopic() {
        return topic(kafkaProperties.getTopics().getOrderCreated());
    }

    @Bean
    public NewTopic orderConfirmedTopic() {
        return topic(kafkaProperties.getTopics().getOrderConfirmed());
    }

    @Bean
    public NewTopic orderCancelledTopic() {
        return topic(kafkaProperties.getTopics().getOrderCancelled());
    }

    @Bean
    public NewTopic inventoryReservedTopic() {
        return topic(kafkaProperties.getTopics().getInventoryReserved());
    }

    @Bean
    public NewTopic inventoryFailedTopic() {
        return topic(kafkaProperties.getTopics().getInventoryFailed());
    }

    @Bean
    public NewTopic inventoryReleasedTopic() {
        return topic(kafkaProperties.getTopics().getInventoryReleased());
    }

    @Bean
    public NewTopic shipmentCreatedTopic() {
        return topic(kafkaProperties.getTopics().getShipmentCreated());
    }

    @Bean
    public NewTopic shipmentShippedTopic() {
        return topic(kafkaProperties.getTopics().getShipmentShipped());
    }

    @Bean
    public NewTopic shipmentDeliveredTopic() {
        return topic(kafkaProperties.getTopics().getShipmentDelivered());
    }

    private NewTopic topic(String name) {
        return TopicBuilder.name(name)
                .partitions(kafkaProperties.getTopicSettings().getPartitions())
                .replicas(kafkaProperties.getTopicSettings().getReplicas())
                .build();
    }
}