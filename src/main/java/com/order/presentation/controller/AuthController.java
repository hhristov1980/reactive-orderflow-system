package com.order.presentation.controller;

import com.order.application.service.JwtTokenService;
import com.order.domain.dto.request.auth.LoginRequest;
import com.order.domain.dto.response.auth.AuthTokenResponse;
import com.order.infrastructure.config.properties.AdminSecurityProperties;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

@RestController
@RequestMapping("/api/v1/auth")
@RequiredArgsConstructor
public class AuthController {

    private static final String TOKEN_TYPE = "Bearer";

    private final JwtTokenService jwtTokenService;
    private final AdminSecurityProperties adminSecurityProperties;

    @PostMapping("/login")
    public Mono<ResponseEntity<AuthTokenResponse>> login(
            @Valid @RequestBody LoginRequest request
    ) {
        if (!isValidAdminCredentials(request)) {
            ResponseEntity<AuthTokenResponse> unauthorizedResponse =
                    ResponseEntity
                            .status(HttpStatus.UNAUTHORIZED)
                            .body(null);

            return Mono.just(unauthorizedResponse);
        }

        List<String> roles = List.of(adminSecurityProperties.getRole());

        String accessToken = jwtTokenService.createToken(
                adminSecurityProperties.getUsername(),
                roles
        );

        long expiresInSeconds = Duration.ofMinutes(
                adminSecurityProperties.getJwt().getExpirationMinutes()
        ).toSeconds();

        AuthTokenResponse response = new AuthTokenResponse(
                TOKEN_TYPE,
                accessToken,
                expiresInSeconds,
                adminSecurityProperties.getUsername(),
                roles
        );

        return Mono.just(ResponseEntity.ok(response));
    }

    private boolean isValidAdminCredentials(LoginRequest request) {
        return adminSecurityProperties.getUsername().equals(request.username())
                && adminSecurityProperties.getPassword().equals(request.password());
    }
}