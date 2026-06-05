package com.order.integration;

import com.order.domain.entity.Product;
import com.order.infrastructure.repository.ProductRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.r2dbc.test.autoconfigure.DataR2dbcTest;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@DataR2dbcTest
class ProductRepositoryIntegrationTest extends AbstractPostgresTestcontainersTest {

    @Autowired
    private ProductRepository productRepository;

    @Test
    void shouldSaveAndFindProduct() {
        Product product = newProduct();

        StepVerifier.create(
                        productRepository.deleteAll()
                                .then(productRepository.save(product))
                                .flatMap(saved -> productRepository.findById(saved.getId()))
                )
                .expectNextMatches(saved ->
                        saved.getName().startsWith("Product Test ")
                                && saved.getPrice().compareTo(new BigDecimal("24.90")) == 0
                                && saved.getStock() == 50
                )
                .verifyComplete();
    }

    private static Product newProduct() {
        OffsetDateTime now = OffsetDateTime.now();

        return Product.builder()
                .name("Product Test " + UUID.randomUUID())
                .price(new BigDecimal("24.90"))
                .stock(50)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }
}