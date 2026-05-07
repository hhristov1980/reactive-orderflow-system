package com.order.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;

@Slf4j
@Configuration
@ConditionalOnProperty(prefix = "spring.r2dbc", name = "url")
@EnableR2dbcRepositories(basePackages = "com.order")
public class R2dbcConfig {

    public R2dbcConfig() {
        log.info("R2DBC configuration enabled");
    }
}
