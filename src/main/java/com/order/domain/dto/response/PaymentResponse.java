package com.order.domain.dto.response;

import com.order.domain.enums.PaymentStatus;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record PaymentResponse(

        Long id,
        Long orderId,
        PaymentStatus status,
        BigDecimal amount,
        String provider,
        String transactionId,
        String failureReason,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt,
        OffsetDateTime paidAt,
        OffsetDateTime failedAt,
        OffsetDateTime expiredAt
) {
}