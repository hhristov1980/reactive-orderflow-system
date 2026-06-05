package com.order.integration;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.postgresql.PostgreSQLContainer;

abstract class AbstractPostgresTestcontainersTest {

    private static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer("postgres:15")
                    .withDatabaseName("orderflow_test")
                    .withUsername("postgres")
                    .withPassword("postgres");

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void registerPostgresProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.docker.compose.enabled", () -> "false");

        registry.add("spring.r2dbc.url", () ->
                "r2dbc:postgresql://" +
                        POSTGRES.getHost() + ":" +
                        POSTGRES.getMappedPort(5432) + "/" +
                        POSTGRES.getDatabaseName()
        );

        registry.add("spring.r2dbc.username", POSTGRES::getUsername);
        registry.add("spring.r2dbc.password", POSTGRES::getPassword);
        registry.add("spring.sql.init.mode", () -> "always");
    }
}