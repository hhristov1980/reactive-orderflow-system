package com.order.integration;

import com.order.domain.entity.User;
import com.order.domain.enums.UserRole;
import com.order.domain.enums.UserStatus;
import com.order.infrastructure.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.r2dbc.test.autoconfigure.DataR2dbcTest;
import reactor.test.StepVerifier;

import java.time.OffsetDateTime;
import java.util.UUID;

@DataR2dbcTest
class UserRepositoryIntegrationTest extends AbstractPostgresTestcontainersTest {

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