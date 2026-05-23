package com.order.domain.event;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record PaymentFailedEvent(
        Long paymentId,
        Long orderId,
        BigDecimal amount,
        String reason,
        OffsetDateTime failedAt
) {
}