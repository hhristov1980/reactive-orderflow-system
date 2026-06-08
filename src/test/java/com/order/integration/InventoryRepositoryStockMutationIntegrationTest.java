package com.order.integration;

import com.order.domain.entity.Inventory;
import com.order.domain.entity.Product;
import com.order.infrastructure.repository.InventoryRepository;
import com.order.infrastructure.repository.ProductRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.r2dbc.test.autoconfigure.DataR2dbcTest;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@DataR2dbcTest
class InventoryRepositoryStockMutationIntegrationTest extends AbstractPostgresTestcontainersTest {

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private InventoryRepository inventoryRepository;

    @Test
    void shouldFindInventoryByProductId() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(productRepository.save(newProduct()))
                                .flatMap(savedProduct ->
                                        inventoryRepository.save(newInventory(savedProduct.getId(), 10, 0))
                                                .then(inventoryRepository.findByProductId(savedProduct.getId()))
                                )
                )
                .expectNextMatches(inventory ->
                        inventory.getProductId() != null
                                && inventory.getAvailableQuantity() == 10
                                && inventory.getReservedQuantity() == 0
                )
                .verifyComplete();
    }

    @Test
    void shouldCheckIfInventoryExistsByProductId() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(productRepository.save(newProduct()))
                                .flatMap(savedProduct ->
                                        inventoryRepository.save(newInventory(savedProduct.getId(), 10, 0))
                                                .then(inventoryRepository.existsByProductId(savedProduct.getId()))
                                )
                )
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    void shouldReturnFalseWhenInventoryDoesNotExistByProductId() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(inventoryRepository.existsByProductId(999_999L))
                )
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    void shouldReserveStockWhenEnoughAvailableQuantityExists() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(productRepository.save(newProduct()))
                                .flatMap(savedProduct ->
                                        inventoryRepository.save(newInventory(savedProduct.getId(), 10, 0))
                                )
                                .flatMap(savedInventory ->
                                        inventoryRepository.reserveStock(savedInventory.getProductId(), 3)
                                                .flatMap(rowsUpdated ->
                                                        inventoryRepository.findById(savedInventory.getId())
                                                                .map(updatedInventory ->
                                                                        new StockMutationResult(rowsUpdated, updatedInventory)
                                                                )
                                                )
                                )
                )
                .expectNextMatches(result ->
                        result.rowsUpdated() == 1
                                && result.inventory().getAvailableQuantity() == 7
                                && result.inventory().getReservedQuantity() == 3
                )
                .verifyComplete();
    }

    @Test
    void shouldNotReserveStockWhenAvailableQuantityIsInsufficient() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(productRepository.save(newProduct()))
                                .flatMap(savedProduct ->
                                        inventoryRepository.save(newInventory(savedProduct.getId(), 2, 0))
                                )
                                .flatMap(savedInventory ->
                                        inventoryRepository.reserveStock(savedInventory.getProductId(), 5)
                                                .flatMap(rowsUpdated ->
                                                        inventoryRepository.findById(savedInventory.getId())
                                                                .map(updatedInventory ->
                                                                        new StockMutationResult(rowsUpdated, updatedInventory)
                                                                )
                                                )
                                )
                )
                .expectNextMatches(result ->
                        result.rowsUpdated() == 0
                                && result.inventory().getAvailableQuantity() == 2
                                && result.inventory().getReservedQuantity() == 0
                )
                .verifyComplete();
    }

    @Test
    void shouldReleaseStockWhenEnoughReservedQuantityExists() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(productRepository.save(newProduct()))
                                .flatMap(savedProduct ->
                                        inventoryRepository.save(newInventory(savedProduct.getId(), 7, 3))
                                )
                                .flatMap(savedInventory ->
                                        inventoryRepository.releaseStock(savedInventory.getProductId(), 2)
                                                .flatMap(rowsUpdated ->
                                                        inventoryRepository.findById(savedInventory.getId())
                                                                .map(updatedInventory ->
                                                                        new StockMutationResult(rowsUpdated, updatedInventory)
                                                                )
                                                )
                                )
                )
                .expectNextMatches(result ->
                        result.rowsUpdated() == 1
                                && result.inventory().getAvailableQuantity() == 9
                                && result.inventory().getReservedQuantity() == 1
                )
                .verifyComplete();
    }

    @Test
    void shouldNotReleaseStockWhenReservedQuantityIsInsufficient() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(productRepository.save(newProduct()))
                                .flatMap(savedProduct ->
                                        inventoryRepository.save(newInventory(savedProduct.getId(), 7, 1))
                                )
                                .flatMap(savedInventory ->
                                        inventoryRepository.releaseStock(savedInventory.getProductId(), 3)
                                                .flatMap(rowsUpdated ->
                                                        inventoryRepository.findById(savedInventory.getId())
                                                                .map(updatedInventory ->
                                                                        new StockMutationResult(rowsUpdated, updatedInventory)
                                                                )
                                                )
                                )
                )
                .expectNextMatches(result ->
                        result.rowsUpdated() == 0
                                && result.inventory().getAvailableQuantity() == 7
                                && result.inventory().getReservedQuantity() == 1
                )
                .verifyComplete();
    }

    private Mono<Void> cleanDatabase() {
        return inventoryRepository.deleteAll()
                .then(productRepository.deleteAll());
    }

    private static Product newProduct() {
        OffsetDateTime now = OffsetDateTime.now();

        return Product.builder()
                .name("Stock Mutation Test Product " + UUID.randomUUID())
                .price(new BigDecimal("24.90"))
                .stock(50)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private static Inventory newInventory(
            Long productId,
            int availableQuantity,
            int reservedQuantity
    ) {
        OffsetDateTime now = OffsetDateTime.now();

        return Inventory.builder()
                .productId(productId)
                .availableQuantity(availableQuantity)
                .reservedQuantity(reservedQuantity)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private record StockMutationResult(Integer rowsUpdated, Inventory inventory) {
    }
}