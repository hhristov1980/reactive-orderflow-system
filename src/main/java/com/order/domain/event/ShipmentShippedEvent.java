package com.order.domain.event;

import java.time.OffsetDateTime;

public record ShipmentShippedEvent(
        Long shipmentId,
        Long orderId,
        String trackingNumber,
        OffsetDateTime shippedAt
) {
}