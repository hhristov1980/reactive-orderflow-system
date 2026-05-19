package com.order.domain.event;

import java.time.OffsetDateTime;

public record OrderCancelledEvent(
        Long orderId,
        Long userId,
        OffsetDateTime cancelledAt
) {
}