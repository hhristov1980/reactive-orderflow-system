package com.order.domain.event;

import java.time.OffsetDateTime;

public record InventoryFailedEvent(
        Long orderId,
        String reason,
        OffsetDateTime failedAt
) {
}