package com.order.integration;

import com.order.domain.entity.Inventory;
import com.order.domain.entity.Product;
import com.order.infrastructure.repository.InventoryRepository;
import com.order.infrastructure.repository.ProductRepository;
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
class InventoryRepositoryIntegrationTest {

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
    private ProductRepository productRepository;

    @Autowired
    private InventoryRepository inventoryRepository;

    @Test
    void shouldSaveAndFindInventory() {
        StepVerifier.create(
                        inventoryRepository.deleteAll()
                                .then(productRepository.deleteAll())
                                .then(productRepository.save(newProduct()))
                                .flatMap(savedProduct ->
                                        inventoryRepository.save(newInventory(savedProduct.getId()))
                                )
                                .flatMap(savedInventory ->
                                        inventoryRepository.findById(savedInventory.getId())
                                )
                )
                .expectNextMatches(saved ->
                        saved.getProductId() != null
                                && saved.getAvailableQuantity() == 50
                                && saved.getReservedQuantity() == 0
                )
                .verifyComplete();
    }

    private static Product newProduct() {
        OffsetDateTime now = OffsetDateTime.now();

        return Product.builder()
                .name("Inventory Test Product " + UUID.randomUUID())
                .price(new BigDecimal("24.90"))
                .stock(50)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private static Inventory newInventory(Long productId) {
        OffsetDateTime now = OffsetDateTime.now();

        return Inventory.builder()
                .productId(productId)
                .availableQuantity(50)
                .reservedQuantity(0)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }
}