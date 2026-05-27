package com.order.infrastructure.config.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "orderflow.scheduler.outbox")
public class OutboxSchedulerProperties {

    private boolean enabled = true;
    private Long fixedDelayMs = 5000L;
    private Integer maxRetries = 5;
}