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
    private String consumerGroupId;
    private String autoOffsetReset;
    private Topics topics = new Topics();

    @Getter
    @Setter
    public static class Topics {

        private String orderCreated;
        private String orderConfirmed;
        private String orderCancelled;
    }
}