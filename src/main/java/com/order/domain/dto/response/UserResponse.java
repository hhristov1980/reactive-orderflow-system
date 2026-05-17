package com.order.domain.dto.response;

import java.time.OffsetDateTime;

public record UserResponse(
        Long id,
        String email,
        String name,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt
) {
}