package com.order.domain.event;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record PaymentExpiredEvent(

        Long paymentId,
        Long orderId,
        BigDecimal amount,
        String reason,
        OffsetDateTime expiredAt
) {
}