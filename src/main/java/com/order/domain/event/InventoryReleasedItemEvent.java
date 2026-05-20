package com.order.domain.event;

public record InventoryReleasedItemEvent(
        Long productId,
        Integer quantity
) {
}