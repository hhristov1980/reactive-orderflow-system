package com.order.application.service.impl;

import com.order.application.service.AuditService;
import com.order.domain.entity.AuditEvent;
import com.order.infrastructure.repository.AuditEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;

@Service
@RequiredArgsConstructor
@Slf4j
public class AuditServiceImpl implements AuditService {

    private static final String AGGREGATE_TYPE_ORDER = "ORDER";

    private final AuditEventRepository repository;

    @Override
    public Mono<Void> saveEvent(
            String eventType,
            String aggregateType,
            Long aggregateId,
            String payload
    ) {
        AuditEvent auditEvent = AuditEvent.builder()
                .eventType(eventType)
                .aggregateType(aggregateType)
                .aggregateId(aggregateId)
                .payload(payload)
                .createdAt(OffsetDateTime.now())
                .build();

        return repository.save(auditEvent)
                .doOnSuccess(saved ->
                        {
                            assert saved != null;
                            log.info(
                                    "Audit event saved. eventType={}, aggregateType={}, aggregateId={}",
                                    saved.getEventType(),
                                    saved.getAggregateType(),
                                    saved.getAggregateId()
                            );
                        }
                )
                .then();
    }
}