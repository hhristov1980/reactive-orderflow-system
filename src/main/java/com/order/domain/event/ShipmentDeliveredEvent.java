package com.order.domain.event;

import java.time.OffsetDateTime;

public record ShipmentDeliveredEvent(
        Long shipmentId,
        Long orderId,
        String trackingNumber,
        OffsetDateTime deliveredAt
) {
}