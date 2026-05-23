package com.order.domain.event;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record PaymentCreatedEvent(
        Long paymentId,
        Long orderId,
        BigDecimal amount,
        String provider,
        OffsetDateTime createdAt
) {
}