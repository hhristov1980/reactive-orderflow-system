package com.order.infrastructure.config.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "orderflow.scheduler.unpaid-payments")
public class UnpaidPaymentSchedulerProperties {

    private boolean enabled;
    private Integer expirationDays;
    private Long fixedDelayMs;
}