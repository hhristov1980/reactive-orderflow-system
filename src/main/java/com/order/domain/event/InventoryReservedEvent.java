package com.order.domain.event;

import java.time.OffsetDateTime;
import java.util.List;

public record InventoryReservedEvent(
        Long orderId,
        List<InventoryReservedItemEvent> items,
        OffsetDateTime reservedAt
) {
}