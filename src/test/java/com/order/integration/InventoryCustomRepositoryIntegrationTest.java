package com.order.integration;

import com.order.domain.entity.Inventory;
import com.order.domain.entity.Product;
import com.order.domain.enums.InventorySortField;
import com.order.domain.enums.SortDirection;
import com.order.infrastructure.repository.InventoryRepository;
import com.order.infrastructure.repository.ProductRepository;
import com.order.infrastructure.repository.custom.InventoryCustomRepository;
import com.order.infrastructure.repository.custom.InventoryCustomRepositoryImpl;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.r2dbc.test.autoconfigure.DataR2dbcTest;
import org.springframework.context.annotation.Import;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@DataR2dbcTest
@Import(InventoryCustomRepositoryImpl.class)
class InventoryCustomRepositoryIntegrationTest extends AbstractPostgresTestcontainersTest {

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private InventoryRepository inventoryRepository;

    @Autowired
    private InventoryCustomRepository inventoryCustomRepository;

    @Test
    void shouldCountAllInventoryItems() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(createProductWithInventory(10, 0))
                                .then(createProductWithInventory(5, 1))
                                .then(createProductWithInventory(0, 3))
                                .then(inventoryCustomRepository.countAll())
                )
                .expectNext(3L)
                .verifyComplete();
    }

    @Test
    void shouldFindAllPagedSortedByAvailableQuantityAscending() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(createProductWithInventory(10, 0))
                                .then(createProductWithInventory(3, 1))
                                .then(createProductWithInventory(7, 2))
                                .thenMany(inventoryCustomRepository.findAllPaged(
                                        0,
                                        3,
                                        InventorySortField.AVAILABLE_QUANTITY,
                                        SortDirection.ASC
                                ))
                                .collectList()
                )
                .expectNextMatches(inventory ->
                        inventory.size() == 3
                                && inventory.get(0).getAvailableQuantity() == 3
                                && inventory.get(1).getAvailableQuantity() == 7
                                && inventory.get(2).getAvailableQuantity() == 10
                )
                .verifyComplete();
    }

    @Test
    void shouldFindAllPagedSortedByAvailableQuantityDescending() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(createProductWithInventory(10, 0))
                                .then(createProductWithInventory(3, 1))
                                .then(createProductWithInventory(7, 2))
                                .thenMany(inventoryCustomRepository.findAllPaged(
                                        0,
                                        3,
                                        InventorySortField.AVAILABLE_QUANTITY,
                                        SortDirection.DESC
                                ))
                                .collectList()
                )
                .expectNextMatches(inventory ->
                        inventory.size() == 3
                                && inventory.get(0).getAvailableQuantity() == 10
                                && inventory.get(1).getAvailableQuantity() == 7
                                && inventory.get(2).getAvailableQuantity() == 3
                )
                .verifyComplete();
    }

    @Test
    void shouldApplyPaginationOffset() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(createProductWithInventory(1, 0))
                                .then(createProductWithInventory(2, 0))
                                .then(createProductWithInventory(3, 0))
                                .then(createProductWithInventory(4, 0))
                                .thenMany(inventoryCustomRepository.findAllPaged(
                                        1,
                                        2,
                                        InventorySortField.AVAILABLE_QUANTITY,
                                        SortDirection.ASC
                                ))
                                .collectList()
                )
                .expectNextMatches(inventory ->
                        inventory.size() == 2
                                && inventory.get(0).getAvailableQuantity() == 3
                                && inventory.get(1).getAvailableQuantity() == 4
                )
                .verifyComplete();
    }

    @Test
    void shouldFindAllPagedSortedByReservedQuantityDescending() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(createProductWithInventory(10, 1))
                                .then(createProductWithInventory(10, 5))
                                .then(createProductWithInventory(10, 3))
                                .thenMany(inventoryCustomRepository.findAllPaged(
                                        0,
                                        3,
                                        InventorySortField.RESERVED_QUANTITY,
                                        SortDirection.DESC
                                ))
                                .collectList()
                )
                .expectNextMatches(inventory ->
                        inventory.size() == 3
                                && inventory.get(0).getReservedQuantity() == 5
                                && inventory.get(1).getReservedQuantity() == 3
                                && inventory.get(2).getReservedQuantity() == 1
                )
                .verifyComplete();
    }

    private Mono<Void> cleanDatabase() {
        return inventoryRepository.deleteAll()
                .then(productRepository.deleteAll());
    }

    private Mono<Inventory> createProductWithInventory(
            int availableQuantity,
            int reservedQuantity
    ) {
        return productRepository.save(newProduct())
                .flatMap(savedProduct ->
                        inventoryRepository.save(newInventory(
                                savedProduct.getId(),
                                availableQuantity,
                                reservedQuantity
                        ))
                );
    }

    private static Product newProduct() {
        OffsetDateTime now = OffsetDateTime.now();

        return Product.builder()
                .name("Inventory Custom Test Product " + UUID.randomUUID())
                .price(new BigDecimal("24.90"))
                .stock(100)
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
}