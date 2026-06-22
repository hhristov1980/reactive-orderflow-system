package com.order.domain.dto.response.auth;

import java.util.List;

public record AuthTokenResponse(

        String tokenType,
        String accessToken,
        Long expiresInSeconds,
        String username,
        List<String> roles

) {
}
