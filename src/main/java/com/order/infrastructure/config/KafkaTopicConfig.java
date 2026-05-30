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

    @Bean
    public NewTopic paymentCreatedTopic() {
        return topic(kafkaProperties.getTopics().getPaymentCreated());
    }

    @Bean
    public NewTopic paymentCompletedTopic() {
        return topic(kafkaProperties.getTopics().getPaymentCompleted());
    }

    @Bean
    public NewTopic paymentFailedTopic() {
        return topic(kafkaProperties.getTopics().getPaymentFailed());
    }

    @Bean
    public NewTopic paymentExpiredTopic() {
        return topic(kafkaProperties.getTopics().getPaymentExpired());
    }

    @Bean
    public NewTopic orderCreatedDltTopic() {
        return dltTopic(kafkaProperties.getTopics().getOrderCreated());
    }

    @Bean
    public NewTopic orderConfirmedDltTopic() {
        return dltTopic(kafkaProperties.getTopics().getOrderConfirmed());
    }

    @Bean
    public NewTopic orderCancelledDltTopic() {
        return dltTopic(kafkaProperties.getTopics().getOrderCancelled());
    }

    @Bean
    public NewTopic inventoryReservedDltTopic() {
        return dltTopic(kafkaProperties.getTopics().getInventoryReserved());
    }

    @Bean
    public NewTopic inventoryFailedDltTopic() {
        return dltTopic(kafkaProperties.getTopics().getInventoryFailed());
    }

    @Bean
    public NewTopic inventoryReleasedDltTopic() {
        return dltTopic(kafkaProperties.getTopics().getInventoryReleased());
    }

    @Bean
    public NewTopic shipmentCreatedDltTopic() {
        return dltTopic(kafkaProperties.getTopics().getShipmentCreated());
    }

    @Bean
    public NewTopic shipmentShippedDltTopic() {
        return dltTopic(kafkaProperties.getTopics().getShipmentShipped());
    }

    @Bean
    public NewTopic shipmentDeliveredDltTopic() {
        return dltTopic(kafkaProperties.getTopics().getShipmentDelivered());
    }

    @Bean
    public NewTopic paymentCreatedDltTopic() {
        return dltTopic(kafkaProperties.getTopics().getPaymentCreated());
    }

    @Bean
    public NewTopic paymentCompletedDltTopic() {
        return dltTopic(kafkaProperties.getTopics().getPaymentCompleted());
    }

    @Bean
    public NewTopic paymentFailedDltTopic() {
        return dltTopic(kafkaProperties.getTopics().getPaymentFailed());
    }

    @Bean
    public NewTopic paymentExpiredDltTopic() {
        return dltTopic(kafkaProperties.getTopics().getPaymentExpired());
    }

    private NewTopic topic(String name) {
        return TopicBuilder.name(name)
                .partitions(kafkaProperties.getTopicSettings().getPartitions())
                .replicas(kafkaProperties.getTopicSettings().getReplicas())
                .build();
    }

    private NewTopic dltTopic(String sourceTopic) {
        return topic(sourceTopic + ".DLT");
    }
}
