package com.order.domain.event;

import java.time.OffsetDateTime;
import java.util.List;

public record OrderCancelledEvent(
        Long orderId,
        Long userId,
        List<OrderItemEvent> items,
        OffsetDateTime cancelledAt
) {
}