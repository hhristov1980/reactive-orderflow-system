package com.order.integration;

import com.order.domain.entity.Order;
import com.order.domain.entity.OrderItem;
import com.order.domain.entity.Product;
import com.order.domain.entity.User;
import com.order.domain.enums.OrderStatus;
import com.order.domain.enums.UserRole;
import com.order.domain.enums.UserStatus;
import com.order.infrastructure.repository.OrderItemRepository;
import com.order.infrastructure.repository.OrderRepository;
import com.order.infrastructure.repository.ProductRepository;
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
class OrderItemRepositoryIntegrationTest {

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
    private ProductRepository productRepository;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OrderItemRepository orderItemRepository;

    @Test
    void shouldSaveAndFindOrderItem() {
        StepVerifier.create(
                        orderItemRepository.deleteAll()
                                .then(orderRepository.deleteAll())
                                .then(productRepository.deleteAll())
                                .then(userRepository.deleteAll())
                                .then(userRepository.save(newActiveCustomer()))
                                .flatMap(savedUser ->
                                        orderRepository.save(newCreatedOrder(savedUser.getId()))
                                )
                                .zipWith(productRepository.save(newProduct()))
                                .flatMap(tuple -> {
                                    Order savedOrder = tuple.getT1();
                                    Product savedProduct = tuple.getT2();

                                    OrderItem orderItem = newOrderItem(
                                            savedOrder.getId(),
                                            savedProduct.getId(),
                                            savedProduct.getPrice()
                                    );

                                    return orderItemRepository.save(orderItem);
                                })
                                .flatMap(savedOrderItem -> orderItemRepository.findById(savedOrderItem.getId()))
                )
                .expectNextMatches(saved ->
                        saved.getOrderId() != null
                                && saved.getProductId() != null
                                && saved.getQuantity() == 2
                                && saved.getPrice().compareTo(new BigDecimal("24.90")) == 0
                )
                .verifyComplete();
    }

    private static User newActiveCustomer() {
        OffsetDateTime now = OffsetDateTime.now();

        return User.builder()
                .email("order.item.user." + UUID.randomUUID() + "@example.com")
                .name("Order Item Test User " + UUID.randomUUID())
                .role(UserRole.CUSTOMER)
                .status(UserStatus.ACTIVE)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private static Product newProduct() {
        OffsetDateTime now = OffsetDateTime.now();

        return Product.builder()
                .name("Order Item Test Product " + UUID.randomUUID())
                .price(new BigDecimal("24.90"))
                .stock(50)
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

    private static OrderItem newOrderItem(Long orderId, Long productId, BigDecimal price) {
        return OrderItem.builder()
                .orderId(orderId)
                .productId(productId)
                .quantity(2)
                .price(price)
                .build();
    }
}