package com.order.domain.dto.response;

import java.time.OffsetDateTime;

public record InventoryResponse(

        Long id,
        Long productId,
        Integer availableQuantity,
        Integer reservedQuantity,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt
) {
}