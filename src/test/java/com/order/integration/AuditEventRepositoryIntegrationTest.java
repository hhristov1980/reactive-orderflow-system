package com.order.integration;

import com.order.domain.entity.AuditEvent;
import com.order.infrastructure.repository.AuditEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.r2dbc.test.autoconfigure.DataR2dbcTest;
import reactor.test.StepVerifier;

import java.time.OffsetDateTime;

@DataR2dbcTest
class AuditEventRepositoryIntegrationTest extends AbstractPostgresTestcontainersTest{

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