package com.order.integration;

import com.order.application.mapper.InventoryMapper;
import com.order.application.service.OutboxService;
import com.order.application.service.impl.InventoryServiceImpl;
import com.order.domain.entity.Inventory;
import com.order.domain.entity.Order;
import com.order.domain.entity.Product;
import com.order.domain.entity.User;
import com.order.domain.enums.InventoryReservationStatus;
import com.order.domain.enums.OrderStatus;
import com.order.domain.enums.UserRole;
import com.order.domain.enums.UserStatus;
import com.order.domain.event.OrderCancelledEvent;
import com.order.domain.event.OrderCreatedEvent;
import com.order.domain.event.OrderItemEvent;
import com.order.exception.InventoryReservationException;
import com.order.infrastructure.config.properties.OrderKafkaProperties;
import com.order.infrastructure.repository.InventoryRepository;
import com.order.infrastructure.repository.InventoryReservationRepository;
import com.order.infrastructure.repository.OrderRepository;
import com.order.infrastructure.repository.ProductRepository;
import com.order.infrastructure.repository.UserRepository;
import com.order.infrastructure.repository.custom.InventoryCustomRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.data.r2dbc.test.autoconfigure.DataR2dbcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DataR2dbcTest
@Import(InventoryServiceImpl.class)
@EnableConfigurationProperties(OrderKafkaProperties.class)
@TestPropertySource(properties = {
        "orderflow.kafka.topics.inventory-reserved=inventory.reserved",
        "orderflow.kafka.topics.inventory-failed=inventory.failed",
        "orderflow.kafka.topics.inventory-released=inventory.released"
})
class InventoryServiceIntegrationTest extends AbstractPostgresTestcontainersTest {

    @Autowired
    private InventoryServiceImpl inventoryService;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private InventoryRepository inventoryRepository;

    @Autowired
    private InventoryReservationRepository reservationRepository;

    @MockitoBean
    private InventoryMapper inventoryMapper;

    @MockitoBean
    private InventoryCustomRepository inventoryCustomRepository;

    @MockitoBean
    private OutboxService outboxService;

    @Test
    void shouldReserveInventoryAndSaveOutboxEvent() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> productIdRef = new AtomicReference<>();
        AtomicReference<Long> orderIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(createUserOrderProductAndInventory(10, 0))
                                .flatMap(seed -> {
                                    productIdRef.set(seed.productId());
                                    orderIdRef.set(seed.orderId());

                                    return inventoryService.reserve(new OrderCreatedEvent(
                                            seed.orderId(),
                                            seed.userId(),
                                            new BigDecimal("30.00"),
                                            List.of(new OrderItemEvent(
                                                    seed.productId(),
                                                    3,
                                                    new BigDecimal("10.00")
                                            )),
                                            OffsetDateTime.now()
                                    ));
                                })
                )
                .expectNextMatches(event ->
                        event.orderId().equals(orderIdRef.get())
                                && event.items().size() == 1
                                && event.items().get(0).productId().equals(productIdRef.get())
                                && event.items().get(0).quantity() == 3
                )
                .verifyComplete();

        StepVerifier.create(inventoryRepository.findByProductId(productIdRef.get()))
                .expectNextMatches(inventory ->
                        inventory.getAvailableQuantity() == 7
                                && inventory.getReservedQuantity() == 3
                )
                .verifyComplete();

        StepVerifier.create(reservationRepository.findAll().collectList())
                .expectNextMatches(reservations ->
                        reservations.size() == 1
                                && reservations.get(0).getOrderId().equals(orderIdRef.get())
                                && reservations.get(0).getProductId().equals(productIdRef.get())
                                && reservations.get(0).getQuantity() == 3
                                && reservations.get(0).getStatus() == InventoryReservationStatus.RESERVED
                )
                .verifyComplete();

        verify(outboxService).saveEvent(
                eq("INVENTORY"),
                eq(orderIdRef.get()),
                eq("INVENTORY_RESERVED"),
                eq("inventory.reserved"),
                eq(orderIdRef.get().toString()),
                any()
        );
    }

    @Test
    void shouldFailReservationWhenStockIsInsufficientAndRollbackReservation() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> productIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(createUserOrderProductAndInventory(2, 0))
                                .flatMap(seed -> {
                                    productIdRef.set(seed.productId());

                                    return inventoryService.reserve(new OrderCreatedEvent(
                                            seed.orderId(),
                                            seed.userId(),
                                            new BigDecimal("50.00"),
                                            List.of(new OrderItemEvent(
                                                    seed.productId(),
                                                    5,
                                                    new BigDecimal("10.00")
                                            )),
                                            OffsetDateTime.now()
                                    ));
                                })
                )
                .expectError(InventoryReservationException.class)
                .verify();

        StepVerifier.create(inventoryRepository.findByProductId(productIdRef.get()))
                .expectNextMatches(inventory ->
                        inventory.getAvailableQuantity() == 2
                                && inventory.getReservedQuantity() == 0
                )
                .verifyComplete();

        StepVerifier.create(reservationRepository.findAll().collectList())
                .expectNextMatches(List::isEmpty)
                .verifyComplete();

        verify(outboxService, never()).saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        );
    }

    @Test
    void shouldReleaseInventoryAndSaveOutboxEvent() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> productIdRef = new AtomicReference<>();
        AtomicReference<Long> orderIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(createUserOrderProductAndInventory(7, 3))
                                .flatMap(seed -> {
                                    productIdRef.set(seed.productId());
                                    orderIdRef.set(seed.orderId());

                                    return reservationRepository.createReservation(
                                                    seed.orderId(),
                                                    seed.productId(),
                                                    3
                                            )
                                            .then(inventoryService.release(new OrderCancelledEvent(
                                                    seed.orderId(),
                                                    seed.userId(),
                                                    List.of(new OrderItemEvent(
                                                            seed.productId(),
                                                            3,
                                                            new BigDecimal("10.00")
                                                    )),
                                                    OffsetDateTime.now()
                                            )));
                                })
                )
                .expectNextMatches(event ->
                        event.orderId().equals(orderIdRef.get())
                                && event.items().size() == 1
                                && event.items().get(0).productId().equals(productIdRef.get())
                                && event.items().get(0).quantity() == 3
                )
                .verifyComplete();

        StepVerifier.create(inventoryRepository.findByProductId(productIdRef.get()))
                .expectNextMatches(inventory ->
                        inventory.getAvailableQuantity() == 10
                                && inventory.getReservedQuantity() == 0
                )
                .verifyComplete();

        StepVerifier.create(reservationRepository.findAll().collectList())
                .expectNextMatches(reservations ->
                        reservations.size() == 1
                                && reservations.get(0).getOrderId().equals(orderIdRef.get())
                                && reservations.get(0).getProductId().equals(productIdRef.get())
                                && reservations.get(0).getQuantity() == 3
                                && reservations.get(0).getStatus() == InventoryReservationStatus.RELEASED
                )
                .verifyComplete();

        verify(outboxService).saveEvent(
                eq("INVENTORY"),
                eq(orderIdRef.get()),
                eq("INVENTORY_RELEASED"),
                eq("inventory.released"),
                eq(orderIdRef.get().toString()),
                any()
        );
    }

    @Test
    void shouldFailReleaseWhenCancelledEventHasNoItems() {
        StepVerifier.create(
                        inventoryService.release(new OrderCancelledEvent(
                                999L,
                                100L,
                                List.of(),
                                OffsetDateTime.now()
                        ))
                )
                .expectError(InventoryReservationException.class)
                .verify();

        verify(outboxService, never()).saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        );
    }

    @Test
    void shouldTreatDuplicateReservationAsAlreadyReserved() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> productIdRef = new AtomicReference<>();
        AtomicReference<Long> orderIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(createUserOrderProductAndInventory(7, 3))
                                .flatMap(seed -> {
                                    productIdRef.set(seed.productId());
                                    orderIdRef.set(seed.orderId());

                                    return reservationRepository.createReservation(
                                                    seed.orderId(),
                                                    seed.productId(),
                                                    3
                                            )
                                            .then(inventoryService.reserve(new OrderCreatedEvent(
                                                    seed.orderId(),
                                                    seed.userId(),
                                                    new BigDecimal("30.00"),
                                                    List.of(new OrderItemEvent(
                                                            seed.productId(),
                                                            3,
                                                            new BigDecimal("10.00")
                                                    )),
                                                    OffsetDateTime.now()
                                            )));
                                })
                )
                .expectNextMatches(event ->
                        event.orderId().equals(orderIdRef.get())
                                && event.items().size() == 1
                                && event.items().get(0).productId().equals(productIdRef.get())
                                && event.items().get(0).quantity() == 3
                )
                .verifyComplete();

        StepVerifier.create(inventoryRepository.findByProductId(productIdRef.get()))
                .expectNextMatches(inventory ->
                        inventory.getAvailableQuantity() == 7
                                && inventory.getReservedQuantity() == 3
                )
                .verifyComplete();

        StepVerifier.create(reservationRepository.findAll().collectList())
                .expectNextMatches(reservations ->
                        reservations.size() == 1
                                && reservations.get(0).getOrderId().equals(orderIdRef.get())
                                && reservations.get(0).getProductId().equals(productIdRef.get())
                                && reservations.get(0).getQuantity() == 3
                                && reservations.get(0).getStatus() == InventoryReservationStatus.RESERVED
                )
                .verifyComplete();

        verify(outboxService).saveEvent(
                eq("INVENTORY"),
                eq(orderIdRef.get()),
                eq("INVENTORY_RESERVED"),
                eq("inventory.reserved"),
                eq(orderIdRef.get().toString()),
                any()
        );
    }

    @Test
    void shouldTreatDuplicateReleaseAsAlreadyReleased() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> productIdRef = new AtomicReference<>();
        AtomicReference<Long> orderIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(createUserOrderProductAndInventory(10, 0))
                                .flatMap(seed -> {
                                    productIdRef.set(seed.productId());
                                    orderIdRef.set(seed.orderId());

                                    return reservationRepository.createReservation(
                                                    seed.orderId(),
                                                    seed.productId(),
                                                    3
                                            )
                                            .then(reservationRepository.markReleased(
                                                    seed.orderId(),
                                                    seed.productId(),
                                                    3
                                            ))
                                            .then(inventoryService.release(new OrderCancelledEvent(
                                                    seed.orderId(),
                                                    seed.userId(),
                                                    List.of(new OrderItemEvent(
                                                            seed.productId(),
                                                            3,
                                                            new BigDecimal("10.00")
                                                    )),
                                                    OffsetDateTime.now()
                                            )));
                                })
                )
                .expectNextMatches(event ->
                        event.orderId().equals(orderIdRef.get())
                                && event.items().size() == 1
                                && event.items().get(0).productId().equals(productIdRef.get())
                                && event.items().get(0).quantity() == 3
                )
                .verifyComplete();

        StepVerifier.create(inventoryRepository.findByProductId(productIdRef.get()))
                .expectNextMatches(inventory ->
                        inventory.getAvailableQuantity() == 10
                                && inventory.getReservedQuantity() == 0
                )
                .verifyComplete();

        StepVerifier.create(reservationRepository.findAll().collectList())
                .expectNextMatches(reservations ->
                        reservations.size() == 1
                                && reservations.get(0).getOrderId().equals(orderIdRef.get())
                                && reservations.get(0).getProductId().equals(productIdRef.get())
                                && reservations.get(0).getQuantity() == 3
                                && reservations.get(0).getStatus() == InventoryReservationStatus.RELEASED
                )
                .verifyComplete();

        verify(outboxService).saveEvent(
                eq("INVENTORY"),
                eq(orderIdRef.get()),
                eq("INVENTORY_RELEASED"),
                eq("inventory.released"),
                eq(orderIdRef.get().toString()),
                any()
        );
    }

    private Mono<Void> cleanDatabase() {
        return reservationRepository.deleteAll()
                .then(inventoryRepository.deleteAll())
                .then(orderRepository.deleteAll())
                .then(productRepository.deleteAll())
                .then(userRepository.deleteAll());
    }

    private Mono<SeedData> createUserOrderProductAndInventory(
            int availableQuantity,
            int reservedQuantity
    ) {
        return userRepository.save(newUser())
                .flatMap(savedUser ->
                        orderRepository.save(newOrder(savedUser.getId()))
                                .flatMap(savedOrder ->
                                        productRepository.save(newProduct())
                                                .flatMap(savedProduct ->
                                                        inventoryRepository.save(newInventory(
                                                                        savedProduct.getId(),
                                                                        availableQuantity,
                                                                        reservedQuantity
                                                                ))
                                                                .thenReturn(new SeedData(
                                                                        savedUser.getId(),
                                                                        savedOrder.getId(),
                                                                        savedProduct.getId()
                                                                ))
                                                )
                                )
                );
    }

    private static User newUser() {
        OffsetDateTime now = OffsetDateTime.now();

        return User.builder()
                .email("inventory.service.user." + UUID.randomUUID() + "@example.com")
                .name("Inventory Service Test User " + UUID.randomUUID())
                .role(UserRole.CUSTOMER)
                .status(UserStatus.ACTIVE)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private static Order newOrder(Long userId) {
        OffsetDateTime now = OffsetDateTime.now();

        return Order.builder()
                .userId(userId)
                .status(OrderStatus.CREATED)
                .totalAmount(new BigDecimal("30.00"))
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private static Product newProduct() {
        OffsetDateTime now = OffsetDateTime.now();

        return Product.builder()
                .name("Inventory Service Test Product " + UUID.randomUUID())
                .price(new BigDecimal("10.00"))
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

    private record SeedData(
            Long userId,
            Long orderId,
            Long productId
    ) {
    }
}