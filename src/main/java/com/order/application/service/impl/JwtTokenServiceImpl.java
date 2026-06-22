package com.order.application.service.impl;

import com.order.application.service.JwtTokenService;
import com.order.infrastructure.config.properties.AdminSecurityProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Service
@RequiredArgsConstructor
public class JwtTokenServiceImpl implements JwtTokenService {

    private static final String ROLES_CLAIM = "roles";

    private final JwtEncoder jwtEncoder;
    private final AdminSecurityProperties adminSecurityProperties;

    @Override
    public String createToken(String username, List<String> roles) {
        Instant now = Instant.now();

        Instant expiresAt = now.plus(
                Duration.ofMinutes(
                        adminSecurityProperties.getJwt().getExpirationMinutes()
                )
        );

        JwtClaimsSet claims = JwtClaimsSet.builder()
                .issuer(adminSecurityProperties.getJwt().getIssuer())
                .issuedAt(now)
                .expiresAt(expiresAt)
                .subject(username)
                .claim(ROLES_CLAIM, roles)
                .build();

        JwsHeader header = JwsHeader
                .with(MacAlgorithm.HS256)
                .build();

        return jwtEncoder
                .encode(JwtEncoderParameters.from(header, claims))
                .getTokenValue();
    }
}