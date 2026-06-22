package com.order.integration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@TestPropertySource(properties = {
        "spring.docker.compose.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "orderflow.scheduler.outbox.enabled=false",
        "orderflow.scheduler.unpaid-payments.enabled=false",
        "orderflow.security.admin.username=admin",
        "orderflow.security.admin.password=admin",
        "orderflow.security.admin.role=ADMIN",
        "orderflow.security.admin.jwt.secret=test-orderflow-secret-key-with-at-least-32-chars",
        "orderflow.security.admin.jwt.issuer=orderflow-test",
        "orderflow.security.admin.jwt.expiration-minutes=60"
})
class AdminSecurityIntegrationTest extends AbstractPostgresTestcontainersTest {

    @Autowired
    private WebTestClient webTestClient;

    @Test
    void shouldRejectAdminEndpointWithoutToken() {
        webTestClient.get()
                .uri("/api/v1/admin/dashboard")
                .exchange()
                .expectStatus()
                .isUnauthorized();
    }

    @Test
    void shouldRejectAdminEndpointWithInvalidToken() {
        webTestClient.get()
                .uri("/api/v1/admin/dashboard")
                .headers(headers -> headers.setBearerAuth("invalid-token"))
                .exchange()
                .expectStatus()
                .isUnauthorized();
    }

    @Test
    void shouldIssueTokenAndAllowAdminEndpointWithAdminRole() {
        String accessToken = loginAndExtractToken("admin", "admin");

        webTestClient.get()
                .uri("/api/v1/admin/dashboard")
                .headers(headers -> headers.setBearerAuth(accessToken))
                .exchange()
                .expectStatus()
                .isOk();
    }

    @Test
    void shouldRejectLoginWithInvalidCredentials() {
        webTestClient.post()
                .uri("/api/v1/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of(
                        "username", "admin",
                        "password", "wrong-password"
                ))
                .exchange()
                .expectStatus()
                .isUnauthorized();
    }

    @Test
    void shouldAllowPublicEndpointWithoutToken() {
        webTestClient.get()
                .uri("/api/v1/products")
                .exchange()
                .expectStatus()
                .isOk();
    }

    private String loginAndExtractToken(String username, String password) {
        Map<?, ?> responseBody = webTestClient.post()
                .uri("/api/v1/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of(
                        "username", username,
                        "password", password
                ))
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(Map.class)
                .returnResult()
                .getResponseBody();

        assertThat(responseBody).isNotNull();

        Object token = responseBody.get("accessToken");
        assertThat(token).isInstanceOf(String.class);
        assertThat((String) token).isNotBlank();

        Object tokenType = responseBody.get("tokenType");
        assertThat(tokenType).isEqualTo("Bearer");

        Object roles = responseBody.get("roles");
        assertThat(roles).asList().contains("ADMIN");

        return (String) token;
    }
}