package com.order.domain.dto.request;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.util.List;

public record CreateOrderRequest(

        @NotNull(message = "User id is required")
        @Positive(message = "User id must be positive")
        Long userId,

        @NotEmpty(message = "Order must contain at least one item")
        List<@Valid CreateOrderItemRequest> items
) {
}