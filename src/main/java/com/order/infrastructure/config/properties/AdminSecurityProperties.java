package com.order.infrastructure.config.properties;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "orderflow.security.admin")
public class AdminSecurityProperties {

    @NotBlank
    private String username;

    @NotBlank
    private String password;

    @NotBlank
    private String role = "ADMIN";

    @Valid
    private Jwt jwt = new Jwt();

    @Getter
    @Setter
    public static class Jwt {

        @NotBlank
        @Size(min = 32)
        private String secret;

        @NotBlank
        private String issuer = "orderflow";

        @Positive
        private long expirationMinutes = 60;
    }
}