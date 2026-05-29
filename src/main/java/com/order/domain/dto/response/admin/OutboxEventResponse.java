package com.order.domain.dto.response.admin;

import com.order.domain.enums.OutboxStatus;

import java.time.OffsetDateTime;

public record OutboxEventResponse(

        Long id,
        String aggregateType,
        Long aggregateId,
        String eventType,
        String topic,
        String eventKey,
        String payload,
        OutboxStatus status,
        Integer retryCount,
        String lastError,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt,
        OffsetDateTime publishedAt
) {
}