package com.order.domain.dto.request;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;

import java.math.BigDecimal;

public record UpdateProductRequest(

        @NotBlank
        String name,

        @NotNull
        @Positive
        BigDecimal price,

        @NotNull
        @PositiveOrZero
        Integer stock
) {
}