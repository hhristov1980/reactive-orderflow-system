package com.order.integration;

import com.order.domain.entity.Inventory;
import com.order.domain.entity.Product;
import com.order.infrastructure.repository.InventoryRepository;
import com.order.infrastructure.repository.ProductRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.r2dbc.test.autoconfigure.DataR2dbcTest;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@DataR2dbcTest
@Testcontainers
class InventoryAtomicReservationIntegrationTest {

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

    @Autowired
    private DatabaseClient databaseClient;

    @Test
    void shouldAtomicallyReserveInventoryWhenEnoughStockIsAvailable() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(productRepository.save(newProduct()))
                                .flatMap(savedProduct ->
                                        inventoryRepository.save(newInventory(savedProduct.getId(), 10, 0))
                                )
                                .flatMap(savedInventory ->
                                        reserveInventory(savedInventory.getProductId(), 3)
                                                .flatMap(rowsUpdated ->
                                                        inventoryRepository.findById(savedInventory.getId())
                                                                .map(updatedInventory ->
                                                                        new ReservationResult(rowsUpdated, updatedInventory)
                                                                )
                                                )
                                )
                )
                .expectNextMatches(result ->
                        result.rowsUpdated() == 1L
                                && result.inventory().getAvailableQuantity() == 7
                                && result.inventory().getReservedQuantity() == 3
                )
                .verifyComplete();
    }

    @Test
    void shouldNotReserveInventoryWhenStockIsInsufficient() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(productRepository.save(newProduct()))
                                .flatMap(savedProduct ->
                                        inventoryRepository.save(newInventory(savedProduct.getId(), 2, 0))
                                )
                                .flatMap(savedInventory ->
                                        reserveInventory(savedInventory.getProductId(), 5)
                                                .flatMap(rowsUpdated ->
                                                        inventoryRepository.findById(savedInventory.getId())
                                                                .map(updatedInventory ->
                                                                        new ReservationResult(rowsUpdated, updatedInventory)
                                                                )
                                                )
                                )
                )
                .expectNextMatches(result ->
                        result.rowsUpdated() == 0L
                                && result.inventory().getAvailableQuantity() == 2
                                && result.inventory().getReservedQuantity() == 0
                )
                .verifyComplete();
    }

    private Mono<Long> reserveInventory(Long productId, int quantity) {
        return databaseClient.sql("""
                        UPDATE inventory
                        SET available_quantity = available_quantity - :quantity,
                            reserved_quantity = reserved_quantity + :quantity
                        WHERE product_id = :productId
                          AND available_quantity >= :quantity
                        """)
                .bind("productId", productId)
                .bind("quantity", quantity)
                .fetch()
                .rowsUpdated();
    }

    private Mono<Void> cleanDatabase() {
        return inventoryRepository.deleteAll()
                .then(productRepository.deleteAll());
    }

    private static Product newProduct() {
        OffsetDateTime now = OffsetDateTime.now();

        return Product.builder()
                .name("Atomic Inventory Test Product " + UUID.randomUUID())
                .price(new BigDecimal("24.90"))
                .stock(50)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private static Inventory newInventory(Long productId, int availableQuantity, int reservedQuantity) {
        OffsetDateTime now = OffsetDateTime.now();

        return Inventory.builder()
                .productId(productId)
                .availableQuantity(availableQuantity)
                .reservedQuantity(reservedQuantity)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private record ReservationResult(Long rowsUpdated, Inventory inventory) {
    }
}