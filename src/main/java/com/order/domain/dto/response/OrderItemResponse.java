package com.order.domain.dto.response;

import java.math.BigDecimal;

public record OrderItemResponse(

        Long id,
        Long productId,
        Integer quantity,
        BigDecimal price
) {
}