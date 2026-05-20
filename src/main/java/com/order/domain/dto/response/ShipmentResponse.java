package com.order.domain.dto.response;

import com.order.domain.enums.ShipmentStatus;

import java.time.OffsetDateTime;

public record ShipmentResponse(

        Long id,
        Long orderId,
        ShipmentStatus status,
        String trackingNumber,
        String carrier,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt,
        OffsetDateTime shippedAt,
        OffsetDateTime deliveredAt
) {
}