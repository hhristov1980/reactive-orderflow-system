package com.order.domain.dto.response;

import com.order.domain.enums.UserRole;
import com.order.domain.enums.UserStatus;

import java.time.OffsetDateTime;

public record UserResponse(

        Long id,
        String name,
        String email,
        UserRole role,
        UserStatus status,
        Boolean emailVerified,
        OffsetDateTime lastLoginAt,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt
) {
}