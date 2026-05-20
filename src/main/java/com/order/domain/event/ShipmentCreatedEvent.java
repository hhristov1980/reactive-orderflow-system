package com.order.domain.event;

import java.time.OffsetDateTime;

public record ShipmentCreatedEvent(
        Long shipmentId,
        Long orderId,
        String trackingNumber,
        String carrier,
        OffsetDateTime createdAt
) {
}