package com.order.integration;

import com.order.application.service.OutboxService;
import com.order.application.service.impl.OrderServiceImpl;
import com.order.domain.dto.request.CreateOrderItemRequest;
import com.order.domain.dto.request.CreateOrderRequest;
import com.order.domain.entity.Order;
import com.order.domain.entity.OrderItem;
import com.order.domain.entity.Product;
import com.order.domain.entity.User;
import com.order.domain.enums.OrderStatus;
import com.order.domain.enums.UserRole;
import com.order.domain.enums.UserStatus;
import com.order.exception.*;
import com.order.infrastructure.config.properties.OrderKafkaProperties;
import com.order.infrastructure.repository.OrderItemRepository;
import com.order.infrastructure.repository.OrderRepository;
import com.order.infrastructure.repository.ProductRepository;
import com.order.infrastructure.repository.UserRepository;
import com.order.infrastructure.repository.custom.OrderCustomRepository;
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
@Import(OrderServiceImpl.class)
@EnableConfigurationProperties(OrderKafkaProperties.class)
@TestPropertySource(properties = {
        "orderflow.kafka.topics.order-created=order.created",
        "orderflow.kafka.topics.order-confirmed=order.confirmed",
        "orderflow.kafka.topics.order-cancelled=order.cancelled"
})
class OrderServiceIntegrationTest extends AbstractPostgresTestcontainersTest {

    @Autowired
    private OrderServiceImpl orderService;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OrderItemRepository orderItemRepository;

    @MockitoBean
    private OrderCustomRepository orderCustomRepository;

    @MockitoBean
    private OutboxService outboxService;

    @Test
    void shouldCreateOrderWithItemsAndSaveOutboxEvent() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> userIdRef = new AtomicReference<>();
        AtomicReference<Long> firstProductIdRef = new AtomicReference<>();
        AtomicReference<Long> secondProductIdRef = new AtomicReference<>();
        AtomicReference<Long> orderIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newUser(UserStatus.ACTIVE)))
                                .flatMap(savedUser -> {
                                    userIdRef.set(savedUser.getId());

                                    return productRepository.save(newProduct("Coffee A", new BigDecimal("10.00")))
                                            .flatMap(firstProduct -> {
                                                firstProductIdRef.set(firstProduct.getId());

                                                return productRepository.save(newProduct("Coffee B", new BigDecimal("20.00")))
                                                        .flatMap(secondProduct -> {
                                                            secondProductIdRef.set(secondProduct.getId());

                                                            CreateOrderRequest request = new CreateOrderRequest(
                                                                    savedUser.getId(),
                                                                    List.of(
                                                                            new CreateOrderItemRequest(
                                                                                    firstProduct.getId(),
                                                                                    2
                                                                            ),
                                                                            new CreateOrderItemRequest(
                                                                                    secondProduct.getId(),
                                                                                    3
                                                                            )
                                                                    )
                                                            );

                                                            return orderService.create(request);
                                                        });
                                            });
                                })
                )
                .expectNextMatches(response -> {
                    orderIdRef.set(response.id());

                    return response.userId().equals(userIdRef.get())
                            && response.status() == OrderStatus.CREATED
                            && response.totalAmount().compareTo(new BigDecimal("80.00")) == 0
                            && response.items().size() == 2;
                })
                .verifyComplete();

        StepVerifier.create(orderRepository.findById(orderIdRef.get()))
                .expectNextMatches(order ->
                        order.getUserId().equals(userIdRef.get())
                                && order.getStatus() == OrderStatus.CREATED
                                && order.getTotalAmount().compareTo(new BigDecimal("80.00")) == 0
                )
                .verifyComplete();

        StepVerifier.create(orderItemRepository.findByOrderId(orderIdRef.get()).collectList())
                .expectNextMatches(items ->
                        items.size() == 2
                                && containsOrderItem(items, firstProductIdRef.get(), 2, new BigDecimal("10.00"))
                                && containsOrderItem(items, secondProductIdRef.get(), 3, new BigDecimal("20.00"))
                )
                .verifyComplete();

        verify(outboxService).saveEvent(
                eq("ORDER"),
                eq(orderIdRef.get()),
                eq("ORDER_CREATED"),
                eq("order.created"),
                eq(orderIdRef.get().toString()),
                any()
        );
    }

    @Test
    void shouldRejectOrderCreationWhenUserIsBlocked() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> userIdRef = new AtomicReference<>();
        AtomicReference<Long> productIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newUser(UserStatus.BLOCKED)))
                                .flatMap(savedUser -> {
                                    userIdRef.set(savedUser.getId());

                                    return productRepository.save(newProduct("Blocked User Product", new BigDecimal("15.00")));
                                })
                                .flatMap(savedProduct -> {
                                    productIdRef.set(savedProduct.getId());

                                    return orderService.create(new CreateOrderRequest(
                                            userIdRef.get(),
                                            List.of(new CreateOrderItemRequest(
                                                    savedProduct.getId(),
                                                    1
                                            ))
                                    ));
                                })
                )
                .expectError(UserBlockedException.class)
                .verify();

        StepVerifier.create(orderRepository.findAll().collectList())
                .expectNextMatches(List::isEmpty)
                .verifyComplete();

        StepVerifier.create(orderItemRepository.findAll().collectList())
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
    void shouldRejectOrderCreationWithDuplicateProducts() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> userIdRef = new AtomicReference<>();
        AtomicReference<Long> productIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newUser(UserStatus.ACTIVE)))
                                .flatMap(savedUser -> {
                                    userIdRef.set(savedUser.getId());

                                    return productRepository.save(newProduct("Duplicate Product", new BigDecimal("12.00")));
                                })
                                .flatMap(savedProduct -> {
                                    productIdRef.set(savedProduct.getId());

                                    return orderService.create(new CreateOrderRequest(
                                            userIdRef.get(),
                                            List.of(
                                                    new CreateOrderItemRequest(
                                                            savedProduct.getId(),
                                                            1
                                                    ),
                                                    new CreateOrderItemRequest(
                                                            savedProduct.getId(),
                                                            2
                                                    )
                                            )
                                    ));
                                })
                )
                .expectError(DuplicateOrderItemException.class)
                .verify();

        StepVerifier.create(orderRepository.findAll().collectList())
                .expectNextMatches(List::isEmpty)
                .verifyComplete();

        StepVerifier.create(orderItemRepository.findAll().collectList())
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
    void shouldConfirmCreatedOrderAndSaveOutboxEvent() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> orderIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newUser(UserStatus.ACTIVE)))
                                .flatMap(savedUser ->
                                        orderRepository.save(newOrder(savedUser.getId(), OrderStatus.CREATED))
                                )
                                .flatMap(savedOrder -> {
                                    orderIdRef.set(savedOrder.getId());

                                    return orderItemRepository.save(newOrderItem(
                                                    savedOrder.getId(),
                                                    1001L,
                                                    2,
                                                    new BigDecimal("10.00")
                                            ))
                                            .then(orderService.confirm(savedOrder.getId()));
                                })
                )
                .expectNextMatches(response ->
                        response.id().equals(orderIdRef.get())
                                && response.status() == OrderStatus.CONFIRMED
                                && response.items().size() == 1
                )
                .verifyComplete();

        StepVerifier.create(orderRepository.findById(orderIdRef.get()))
                .expectNextMatches(order ->
                        order.getStatus() == OrderStatus.CONFIRMED
                )
                .verifyComplete();

        verify(outboxService).saveEvent(
                eq("ORDER"),
                eq(orderIdRef.get()),
                eq("ORDER_CONFIRMED"),
                eq("order.confirmed"),
                eq(orderIdRef.get().toString()),
                any()
        );
    }

    @Test
    void shouldCancelCreatedOrderAndSaveOutboxEvent() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> orderIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newUser(UserStatus.ACTIVE)))
                                .flatMap(savedUser ->
                                        orderRepository.save(newOrder(savedUser.getId(), OrderStatus.CREATED))
                                )
                                .flatMap(savedOrder -> {
                                    orderIdRef.set(savedOrder.getId());

                                    return orderItemRepository.save(newOrderItem(
                                                    savedOrder.getId(),
                                                    1001L,
                                                    2,
                                                    new BigDecimal("10.00")
                                            ))
                                            .then(orderService.cancel(savedOrder.getId()));
                                })
                )
                .expectNextMatches(response ->
                        response.id().equals(orderIdRef.get())
                                && response.status() == OrderStatus.CANCELLED
                                && response.items().size() == 1
                )
                .verifyComplete();

        StepVerifier.create(orderRepository.findById(orderIdRef.get()))
                .expectNextMatches(order ->
                        order.getStatus() == OrderStatus.CANCELLED
                )
                .verifyComplete();

        verify(outboxService).saveEvent(
                eq("ORDER"),
                eq(orderIdRef.get()),
                eq("ORDER_CANCELLED"),
                eq("order.cancelled"),
                eq(orderIdRef.get().toString()),
                any()
        );
    }

    @Test
    void shouldRejectConfirmingAlreadyConfirmedOrder() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> orderIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newUser(UserStatus.ACTIVE)))
                                .flatMap(savedUser ->
                                        orderRepository.save(newOrder(savedUser.getId(), OrderStatus.CONFIRMED))
                                )
                                .doOnNext(savedOrder -> orderIdRef.set(savedOrder.getId()))
                                .flatMap(savedOrder -> orderService.confirm(savedOrder.getId()))
                )
                .expectError(OrderAlreadyConfirmedException.class)
                .verify();

        StepVerifier.create(orderRepository.findById(orderIdRef.get()))
                .expectNextMatches(order ->
                        order.getStatus() == OrderStatus.CONFIRMED
                )
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
    void shouldRejectConfirmingCancelledOrder() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> orderIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newUser(UserStatus.ACTIVE)))
                                .flatMap(savedUser ->
                                        orderRepository.save(newOrder(savedUser.getId(), OrderStatus.CANCELLED))
                                )
                                .doOnNext(savedOrder -> orderIdRef.set(savedOrder.getId()))
                                .flatMap(savedOrder -> orderService.confirm(savedOrder.getId()))
                )
                .expectError(OrderCannotBeConfirmedException.class)
                .verify();

        StepVerifier.create(orderRepository.findById(orderIdRef.get()))
                .expectNextMatches(order ->
                        order.getStatus() == OrderStatus.CANCELLED
                )
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
    void shouldRejectCancellingAlreadyCancelledOrder() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> orderIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newUser(UserStatus.ACTIVE)))
                                .flatMap(savedUser ->
                                        orderRepository.save(newOrder(savedUser.getId(), OrderStatus.CANCELLED))
                                )
                                .doOnNext(savedOrder -> orderIdRef.set(savedOrder.getId()))
                                .flatMap(savedOrder -> orderService.cancel(savedOrder.getId()))
                )
                .expectError(OrderAlreadyCancelledException.class)
                .verify();

        StepVerifier.create(orderRepository.findById(orderIdRef.get()))
                .expectNextMatches(order ->
                        order.getStatus() == OrderStatus.CANCELLED
                )
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
    void shouldConfirmOrderFromInventoryAndSaveOutboxEvent() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> orderIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newUser(UserStatus.ACTIVE)))
                                .flatMap(savedUser ->
                                        orderRepository.save(newOrder(savedUser.getId(), OrderStatus.CREATED))
                                )
                                .doOnNext(savedOrder -> orderIdRef.set(savedOrder.getId()))
                                .flatMap(savedOrder -> orderService.confirmFromInventory(savedOrder.getId()))
                )
                .verifyComplete();

        StepVerifier.create(orderRepository.findById(orderIdRef.get()))
                .expectNextMatches(order ->
                        order.getStatus() == OrderStatus.CONFIRMED
                )
                .verifyComplete();

        verify(outboxService).saveEvent(
                eq("ORDER"),
                eq(orderIdRef.get()),
                eq("ORDER_CONFIRMED"),
                eq("order.confirmed"),
                eq(orderIdRef.get().toString()),
                any()
        );
    }

    @Test
    void shouldSkipConfirmFromInventoryWhenOrderIsNotCreated() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> orderIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newUser(UserStatus.ACTIVE)))
                                .flatMap(savedUser ->
                                        orderRepository.save(newOrder(savedUser.getId(), OrderStatus.CANCELLED))
                                )
                                .doOnNext(savedOrder -> orderIdRef.set(savedOrder.getId()))
                                .flatMap(savedOrder -> orderService.confirmFromInventory(savedOrder.getId()))
                )
                .verifyComplete();

        StepVerifier.create(orderRepository.findById(orderIdRef.get()))
                .expectNextMatches(order ->
                        order.getStatus() == OrderStatus.CANCELLED
                )
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
    void shouldFailOrderFromInventoryWhenOrderIsCreated() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> orderIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newUser(UserStatus.ACTIVE)))
                                .flatMap(savedUser ->
                                        orderRepository.save(newOrder(savedUser.getId(), OrderStatus.CREATED))
                                )
                                .doOnNext(savedOrder -> orderIdRef.set(savedOrder.getId()))
                                .flatMap(savedOrder ->
                                        orderService.failFromInventory(
                                                savedOrder.getId(),
                                                "Insufficient stock"
                                        )
                                )
                )
                .verifyComplete();

        StepVerifier.create(orderRepository.findById(orderIdRef.get()))
                .expectNextMatches(order ->
                        order.getStatus() == OrderStatus.FAILED
                )
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
    void shouldSkipFailFromInventoryWhenOrderIsNotCreated() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> orderIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newUser(UserStatus.ACTIVE)))
                                .flatMap(savedUser ->
                                        orderRepository.save(newOrder(savedUser.getId(), OrderStatus.CONFIRMED))
                                )
                                .doOnNext(savedOrder -> orderIdRef.set(savedOrder.getId()))
                                .flatMap(savedOrder ->
                                        orderService.failFromInventory(
                                                savedOrder.getId(),
                                                "Insufficient stock"
                                        )
                                )
                )
                .verifyComplete();

        StepVerifier.create(orderRepository.findById(orderIdRef.get()))
                .expectNextMatches(order ->
                        order.getStatus() == OrderStatus.CONFIRMED
                )
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
    void shouldCancelOrderFromPaymentFailureAndSaveOutboxEvent() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> orderIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newUser(UserStatus.ACTIVE)))
                                .flatMap(savedUser ->
                                        orderRepository.save(newOrder(savedUser.getId(), OrderStatus.CONFIRMED))
                                )
                                .flatMap(savedOrder -> {
                                    orderIdRef.set(savedOrder.getId());

                                    return orderItemRepository.save(newOrderItem(
                                                    savedOrder.getId(),
                                                    1001L,
                                                    2,
                                                    new BigDecimal("10.00")
                                            ))
                                            .then(orderService.cancelFromPaymentFailure(
                                                    savedOrder.getId(),
                                                    "Payment failed"
                                            ));
                                })
                )
                .verifyComplete();

        StepVerifier.create(orderRepository.findById(orderIdRef.get()))
                .expectNextMatches(order ->
                        order.getStatus() == OrderStatus.CANCELLED
                )
                .verifyComplete();

        verify(outboxService).saveEvent(
                eq("ORDER"),
                eq(orderIdRef.get()),
                eq("ORDER_CANCELLED"),
                eq("order.cancelled"),
                eq(orderIdRef.get().toString()),
                any()
        );
    }

    @Test
    void shouldSkipCancelFromPaymentFailureWhenOrderIsAlreadyCancelled() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.empty());

        AtomicReference<Long> orderIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newUser(UserStatus.ACTIVE)))
                                .flatMap(savedUser ->
                                        orderRepository.save(newOrder(savedUser.getId(), OrderStatus.CANCELLED))
                                )
                                .doOnNext(savedOrder -> orderIdRef.set(savedOrder.getId()))
                                .flatMap(savedOrder ->
                                        orderService.cancelFromPaymentFailure(
                                                savedOrder.getId(),
                                                "Payment failed"
                                        )
                                )
                )
                .verifyComplete();

        StepVerifier.create(orderRepository.findById(orderIdRef.get()))
                .expectNextMatches(order ->
                        order.getStatus() == OrderStatus.CANCELLED
                )
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
    void shouldRollbackOrderCreationWhenOutboxSaveFails() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.error(new RuntimeException("Outbox failure")));

        AtomicReference<Long> userIdRef = new AtomicReference<>();
        AtomicReference<Long> productIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newUser(UserStatus.ACTIVE)))
                                .flatMap(savedUser -> {
                                    userIdRef.set(savedUser.getId());

                                    return productRepository.save(newProduct(
                                            "Rollback Create Product",
                                            new BigDecimal("25.00")
                                    ));
                                })
                                .flatMap(savedProduct -> {
                                    productIdRef.set(savedProduct.getId());

                                    return orderService.create(new CreateOrderRequest(
                                            userIdRef.get(),
                                            List.of(new CreateOrderItemRequest(
                                                    productIdRef.get(),
                                                    2
                                            ))
                                    ));
                                })
                )
                .expectError(RuntimeException.class)
                .verify();

        StepVerifier.create(orderRepository.findAll().collectList())
                .expectNextMatches(List::isEmpty)
                .verifyComplete();

        StepVerifier.create(orderItemRepository.findAll().collectList())
                .expectNextMatches(List::isEmpty)
                .verifyComplete();

        verify(outboxService).saveEvent(
                anyString(),
                anyLong(),
                eq("ORDER_CREATED"),
                anyString(),
                anyString(),
                any()
        );
    }

    @Test
    void shouldRollbackOrderConfirmationWhenOutboxSaveFails() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.error(new RuntimeException("Outbox failure")));

        AtomicReference<Long> orderIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newUser(UserStatus.ACTIVE)))
                                .flatMap(savedUser ->
                                        orderRepository.save(newOrder(
                                                savedUser.getId(),
                                                OrderStatus.CREATED
                                        ))
                                )
                                .flatMap(savedOrder -> {
                                    orderIdRef.set(savedOrder.getId());

                                    return orderItemRepository.save(newOrderItem(
                                                    savedOrder.getId(),
                                                    1001L,
                                                    2,
                                                    new BigDecimal("10.00")
                                            ))
                                            .then(orderService.confirm(savedOrder.getId()));
                                })
                )
                .expectError(RuntimeException.class)
                .verify();

        StepVerifier.create(orderRepository.findById(orderIdRef.get()))
                .expectNextMatches(order ->
                        order.getStatus() == OrderStatus.CREATED
                )
                .verifyComplete();

        verify(outboxService).saveEvent(
                anyString(),
                eq(orderIdRef.get()),
                eq("ORDER_CONFIRMED"),
                anyString(),
                anyString(),
                any()
        );
    }

    @Test
    void shouldRollbackOrderCancellationWhenOutboxSaveFails() {
        when(outboxService.saveEvent(
                anyString(),
                anyLong(),
                anyString(),
                anyString(),
                anyString(),
                any()
        )).thenReturn(Mono.error(new RuntimeException("Outbox failure")));

        AtomicReference<Long> orderIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(userRepository.save(newUser(UserStatus.ACTIVE)))
                                .flatMap(savedUser ->
                                        orderRepository.save(newOrder(
                                                savedUser.getId(),
                                                OrderStatus.CREATED
                                        ))
                                )
                                .flatMap(savedOrder -> {
                                    orderIdRef.set(savedOrder.getId());

                                    return orderItemRepository.save(newOrderItem(
                                                    savedOrder.getId(),
                                                    1001L,
                                                    2,
                                                    new BigDecimal("10.00")
                                            ))
                                            .then(orderService.cancel(savedOrder.getId()));
                                })
                )
                .expectError(RuntimeException.class)
                .verify();

        StepVerifier.create(orderRepository.findById(orderIdRef.get()))
                .expectNextMatches(order ->
                        order.getStatus() == OrderStatus.CREATED
                )
                .verifyComplete();

        verify(outboxService).saveEvent(
                anyString(),
                eq(orderIdRef.get()),
                eq("ORDER_CANCELLED"),
                anyString(),
                anyString(),
                any()
        );
    }

    private Mono<Void> cleanDatabase() {
        return orderItemRepository.deleteAll()
                .then(orderRepository.deleteAll())
                .then(productRepository.deleteAll())
                .then(userRepository.deleteAll());
    }

    private static User newUser(UserStatus status) {
        OffsetDateTime now = OffsetDateTime.now();

        return User.builder()
                .email("order.service.user." + UUID.randomUUID() + "@example.com")
                .name("Order Service Test User " + UUID.randomUUID())
                .role(UserRole.CUSTOMER)
                .status(status)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private static Product newProduct(
            String name,
            BigDecimal price
    ) {
        OffsetDateTime now = OffsetDateTime.now();

        return Product.builder()
                .name(name + " " + UUID.randomUUID())
                .price(price)
                .stock(100)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private static boolean containsOrderItem(
            List<OrderItem> items,
            Long productId,
            int quantity,
            BigDecimal price
    ) {
        return items.stream()
                .anyMatch(item ->
                        item.getProductId().equals(productId)
                                && item.getQuantity() == quantity
                                && item.getPrice().compareTo(price) == 0
                );
    }

    private static Order newOrder(
            Long userId,
            OrderStatus status
    ) {
        OffsetDateTime now = OffsetDateTime.now();

        return Order.builder()
                .userId(userId)
                .status(status)
                .totalAmount(new BigDecimal("20.00"))
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private static OrderItem newOrderItem(
            Long orderId,
            Long productId,
            int quantity,
            BigDecimal price
    ) {
        return OrderItem.builder()
                .orderId(orderId)
                .productId(productId)
                .quantity(quantity)
                .price(price)
                .build();
    }
}