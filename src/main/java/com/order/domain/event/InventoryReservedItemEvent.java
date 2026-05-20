package com.order.domain.event;

public record InventoryReservedItemEvent(
        Long productId,
        Integer quantity
) {
}