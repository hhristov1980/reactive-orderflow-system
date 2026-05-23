package com.order.domain.event;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record OrderConfirmedEvent(
        Long orderId,
        Long userId,
        BigDecimal totalAmount,
        OffsetDateTime confirmedAt
) {
}