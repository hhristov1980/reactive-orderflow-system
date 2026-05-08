package com.order.domain.dto.event;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record ProductCreatedEvent(
        Long id,
        String name,
        BigDecimal price,
        Integer stock,
        OffsetDateTime createdAt
) {
}
