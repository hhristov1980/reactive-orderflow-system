package com.order.domain.event;

import java.time.OffsetDateTime;

public record OrderConfirmedEvent(
        Long orderId,
        Long userId,
        OffsetDateTime confirmedAt
) {
}