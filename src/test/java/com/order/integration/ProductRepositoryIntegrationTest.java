package com.order.integration;

import com.order.domain.entity.Product;
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

@DataR2dbcTest
@Testcontainers
class ProductRepositoryIntegrationTest {

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

    @Test
    void shouldSaveAndFindProduct() {
        OffsetDateTime now = OffsetDateTime.now();

        Product product = Product.builder()
                .name("Ethiopian Yirgacheffe")
                .price(new BigDecimal("24.90"))
                .stock(50)
                .createdAt(now)
                .updatedAt(now)
                .build();

        StepVerifier.create(
                        productRepository.deleteAll()
                                .then(productRepository.save(product))
                                .flatMap(saved -> productRepository.findById(saved.getId()))
                )
                .expectNextMatches(saved ->
                        saved.getName().equals("Ethiopian Yirgacheffe")
                                && saved.getPrice().compareTo(new BigDecimal("24.90")) == 0
                                && saved.getStock() == 50
                )
                .verifyComplete();
    }
}