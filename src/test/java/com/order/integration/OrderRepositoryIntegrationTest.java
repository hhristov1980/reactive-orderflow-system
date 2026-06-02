package com.order.integration;

import com.order.domain.entity.Order;
import com.order.domain.entity.User;
import com.order.domain.enums.OrderStatus;
import com.order.domain.enums.UserRole;
import com.order.domain.enums.UserStatus;
import com.order.infrastructure.repository.OrderRepository;
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

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@DataR2dbcTest
@Testcontainers
class OrderRepositoryIntegrationTest {

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

    @Autowired
    private OrderRepository orderRepository;

    @Test
    void shouldSaveAndFindOrder() {
        User user = newActiveCustomer();

        StepVerifier.create(
                        orderRepository.deleteAll()
                                .then(userRepository.deleteAll())
                                .then(userRepository.save(user))
                                .flatMap(savedUser -> {
                                    Order order = newCreatedOrder(savedUser.getId());
                                    return orderRepository.save(order);
                                })
                                .flatMap(savedOrder -> orderRepository.findById(savedOrder.getId()))
                )
                .expectNextMatches(saved ->
                        saved.getUserId() != null
                                && saved.getStatus() == OrderStatus.CREATED
                                && saved.getTotalAmount().compareTo(new BigDecimal("49.80")) == 0
                )
                .verifyComplete();
    }

    private static User newActiveCustomer() {
        OffsetDateTime now = OffsetDateTime.now();

        return User.builder()
                .email("order.test.user." + UUID.randomUUID() + "@example.com")
                .name("Order Test User " + UUID.randomUUID())
                .role(UserRole.CUSTOMER)
                .status(UserStatus.ACTIVE)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private static Order newCreatedOrder(Long userId) {
        OffsetDateTime now = OffsetDateTime.now();

        return Order.builder()
                .userId(userId)
                .status(OrderStatus.CREATED)
                .totalAmount(new BigDecimal("49.80"))
                .createdAt(now)
                .updatedAt(now)
                .build();
    }
}