package com.order.integration;

import com.order.domain.entity.AuditEvent;
import com.order.infrastructure.repository.AuditEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.r2dbc.test.autoconfigure.DataR2dbcTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import reactor.test.StepVerifier;

import java.time.OffsetDateTime;

@DataR2dbcTest
@Testcontainers
class AuditEventRepositoryIntegrationTest {

    @Container
    static final PostgreSQLContainer postgres =
            new PostgreSQLContainer("postgres:15")
                    .withDatabaseName("orderflow_test")
                    .withUsername("postgres")
                    .withPassword("postgres");

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.docker.compose.enabled", () -> "false");

        registry.add("spring.r2dbc.url", () ->
                "r2dbc:postgresql://" +
                        postgres.getHost() + ":" +
                        postgres.getMappedPort(5432) + "/" +
                        postgres.getDatabaseName()
        );

        registry.add("spring.r2dbc.username", postgres::getUsername);
        registry.add("spring.r2dbc.password", postgres::getPassword);
        registry.add("spring.sql.init.mode", () -> "always");
    }

    @Autowired
    private AuditEventRepository auditEventRepository;

    @Test
    void shouldSaveAndFindAuditEvent() {
        AuditEvent auditEvent = newOrderCreatedAuditEvent();

        Long expectedAggregateId = auditEvent.getAggregateId();

        StepVerifier.create(
                        auditEventRepository.deleteAll()
                                .then(auditEventRepository.save(auditEvent))
                                .flatMap(saved -> auditEventRepository.findById(saved.getId()))
                )
                .expectNextMatches(saved ->
                        saved.getEventType().equals("ORDER_CREATED")
                                && saved.getAggregateType().equals("ORDER")
                                && saved.getAggregateId().equals(expectedAggregateId)
                                && saved.getPayload().contains("ORDER_CREATED")
                                && saved.getCreatedAt() != null
                )
                .verifyComplete();
    }

    private static AuditEvent newOrderCreatedAuditEvent() {
        OffsetDateTime now = OffsetDateTime.now();
        Long aggregateId = System.currentTimeMillis();

        return AuditEvent.builder()
                .eventType("ORDER_CREATED")
                .aggregateType("ORDER")
                .aggregateId(aggregateId)
                .payload("""
                        {
                          "eventType": "ORDER_CREATED",
                          "aggregateType": "ORDER",
                          "aggregateId": %d
                        }
                        """.formatted(aggregateId))
                .createdAt(now)
                .build();
    }
}