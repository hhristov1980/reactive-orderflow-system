package com.order.domain.event;

import java.time.OffsetDateTime;
import java.util.List;

public record InventoryReleasedEvent(
        Long orderId,
        List<InventoryReleasedItemEvent> items,
        OffsetDateTime releasedAt
) {
}