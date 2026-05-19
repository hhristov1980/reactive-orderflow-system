package com.order.infrastructure.repository;

import com.order.domain.entity.AuditEvent;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

public interface AuditEventRepository extends ReactiveCrudRepository<AuditEvent, Long> {
}