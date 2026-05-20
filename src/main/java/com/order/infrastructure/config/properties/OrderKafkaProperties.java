package com.order.infrastructure.config.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "orderflow.kafka")
public class OrderKafkaProperties {

    private String bootstrapServers;
    private String autoOffsetReset;

    private TopicSettings topicSettings = new TopicSettings();
    private ConsumerGroups consumerGroups = new ConsumerGroups();
    private Topics topics = new Topics();

    @Getter
    @Setter
    public static class TopicSettings {

        private Integer partitions;
        private Short replicas;
    }

    @Getter
    @Setter
    public static class ConsumerGroups {

        private String audit;
        private String inventory;
        private String order;
        private String shipment;
    }

    @Getter
    @Setter
    public static class Topics {

        private String orderCreated;
        private String orderConfirmed;
        private String orderCancelled;

        private String inventoryReserved;
        private String inventoryFailed;
        private String inventoryReleased;

        private String shipmentCreated;
        private String shipmentShipped;
        private String shipmentDelivered;
    }
}