package com.order.domain.event;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record PaymentCompletedEvent(
        Long paymentId,
        Long orderId,
        BigDecimal amount,
        String transactionId,
        OffsetDateTime paidAt
) {
}