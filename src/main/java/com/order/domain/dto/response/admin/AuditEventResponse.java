package com.order.domain.dto.response.admin;

import java.time.OffsetDateTime;

public record AuditEventResponse(

        Long id,
        String eventType,
        String aggregateType,
        Long aggregateId,
        String payload,
        OffsetDateTime createdAt
) {
}