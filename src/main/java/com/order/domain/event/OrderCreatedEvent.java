package com.order.domain.event;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;

public record OrderCreatedEvent(
        Long orderId,
        Long userId,
        BigDecimal totalAmount,
        List<OrderItemEvent> items,
        OffsetDateTime createdAt
) {
}