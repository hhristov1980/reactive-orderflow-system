package com.order.integration;

import com.order.domain.entity.User;
import com.order.domain.enums.UserRole;
import com.order.domain.enums.UserStatus;
import com.order.infrastructure.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.r2dbc.test.autoconfigure.DataR2dbcTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import reactor.test.StepVerifier;

import java.time.OffsetDateTime;
import java.util.UUID;

@DataR2dbcTest
@Testcontainers
class UserRepositoryIntegrationTest {

    @Container
    static final PostgreSQLContainer postgres =
            new PostgreSQLContainer("postgres:15")
                    .withDatabaseName("orderflow_test")
                    .withUsername("postgres")
                    .withPassword("postgres");

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.docker.compose.enabled", () -> "false");

        registry.add("spring.r2dbc.url", () ->
                "r2dbc:postgresql://" +
                        postgres.getHost() + ":" +
                        postgres.getMappedPort(5432) + "/" +
                        postgres.getDatabaseName()
        );

        registry.add("spring.r2dbc.username", postgres::getUsername);
        registry.add("spring.r2dbc.password", postgres::getPassword);
        registry.add("spring.sql.init.mode", () -> "always");
    }

    @Autowired
    private UserRepository userRepository;

    @Test
    void shouldSaveAndFindUser() {
        String uniqueEmail = "test.user." + UUID.randomUUID() + "@example.com";
        String uniqueName = "Test User " + UUID.randomUUID();

        User user = newActiveCustomer(uniqueEmail, uniqueName);

        StepVerifier.create(
                        userRepository.deleteAll()
                                .then(userRepository.save(user))
                                .flatMap(saved -> userRepository.findById(saved.getId()))
                )
                .expectNextMatches(saved ->
                        saved.getEmail().equals(uniqueEmail)
                                && saved.getName().equals(uniqueName)
                                && saved.getRole() == UserRole.CUSTOMER
                                && saved.getStatus() == UserStatus.ACTIVE
                )
                .verifyComplete();
    }

    private static User newActiveCustomer(String email, String name) {
        OffsetDateTime now = OffsetDateTime.now();

        return User.builder()
                .email(email)
                .name(name)
                .role(UserRole.CUSTOMER)
                .status(UserStatus.ACTIVE)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }
}