package com.order.application.service.impl;

import com.order.application.service.OrderService;
import com.order.domain.dto.request.CreateOrderRequest;
import com.order.domain.dto.request.CreateOrderItemRequest;
import com.order.domain.dto.response.OrderItemResponse;
import com.order.domain.dto.response.OrderResponse;
import com.order.domain.dto.response.PagedResponse;
import com.order.domain.entity.Order;
import com.order.domain.entity.OrderItem;
import com.order.domain.entity.Product;
import com.order.domain.enums.OrderSortField;
import com.order.domain.enums.OrderStatus;
import com.order.domain.enums.SortDirection;
import com.order.domain.event.OrderCancelledEvent;
import com.order.domain.event.OrderConfirmedEvent;
import com.order.domain.event.OrderCreatedEvent;
import com.order.domain.event.OrderItemEvent;
import com.order.exception.*;
import com.order.infrastructure.messaging.kafka.OrderEventProducer;
import com.order.infrastructure.repository.OrderItemRepository;
import com.order.infrastructure.repository.OrderRepository;
import com.order.infrastructure.repository.ProductRepository;
import com.order.infrastructure.repository.UserRepository;
import com.order.infrastructure.repository.custom.OrderCustomRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderServiceImpl implements OrderService {

    private final OrderRepository orderRepository;
    private final OrderItemRepository orderItemRepository;
    private final UserRepository userRepository;
    private final ProductRepository productRepository;
    private final OrderCustomRepository customRepository;
    private final TransactionalOperator transactionalOperator;
    private final OrderEventProducer orderEventProducer;

    @Override
    public Mono<OrderResponse> create(CreateOrderRequest request) {
        log.info("Creating order for userId {}", request.userId());

        validateNoDuplicateProducts(request);

        Mono<OrderResponse> createOrderFlow =
                userRepository.existsById(request.userId())
                        .flatMap(userExists -> {
                            if (!userExists) {
                                return Mono.error(
                                        new UserNotFoundException(request.userId())
                                );
                            }
                            return buildOrderItems(request.items())
                                    .collectList()
                                    .flatMap(orderItems -> {
                                        BigDecimal totalAmount = calculateTotalAmount(orderItems);
                                        Order order = buildOrder(request.userId(), totalAmount);
                                        return orderRepository.save(order)
                                                .flatMap(savedOrder ->
                                                        saveOrderItems(savedOrder, orderItems)
                                                                .collectList()
                                                                .flatMap(savedItems ->
                                                                        orderEventProducer
                                                                                .publishOrderCreated(
                                                                                        toOrderCreatedEvent(
                                                                                                savedOrder,
                                                                                                savedItems
                                                                                        )
                                                                                )
                                                                                .thenReturn(
                                                                                        toResponse(
                                                                                                savedOrder,
                                                                                                savedItems
                                                                                        )
                                                                                )
                                                                )
                                                );
                                    });
                        });
        return transactionalOperator.transactional(createOrderFlow);
    }

    @Override
    public Mono<OrderResponse> getById(Long id) {
        log.info("Getting order with id {}", id);

        return orderRepository.findById(id)
                .switchIfEmpty(Mono.error(new OrderNotFoundException(id)))
                .flatMap(order ->
                        orderItemRepository.findByOrderId(order.getId())
                                .collectList()
                                .map(items -> toResponse(order, items))
                );
    }

    @Override
    public Mono<PagedResponse<OrderResponse>> getAll(
            int page,
            int size,
            OrderSortField sortBy,
            SortDirection direction
    ) {
        log.info("Getting all orders: page {}, size {}, sortBy {}, direction {}",
                page,
                size,
                sortBy,
                direction
        );

        int validatedPage = Math.max(page, 0);
        int validatedSize = Math.clamp(size, 1, 100);

        Mono<List<OrderResponse>> ordersMono =
                customRepository.findAllPaged(
                                validatedPage,
                                validatedSize,
                                sortBy,
                                direction
                        )
                        .flatMap(order ->
                                orderItemRepository.findByOrderId(order.getId())
                                        .collectList()
                                        .map(items -> toResponse(order, items))
                        )
                        .collectList();

        Mono<Long> countMono =
                customRepository.countAll();

        return Mono.zip(ordersMono, countMono)
                .map(tuple -> {
                    List<OrderResponse> orders = tuple.getT1();
                    long totalElements = tuple.getT2();
                    int totalPages = (int) Math.ceil((double) totalElements / validatedSize);
                    boolean first = validatedPage == 0;
                    boolean last = totalPages == 0 || validatedPage >= totalPages - 1;
                    boolean hasNext = validatedPage < totalPages - 1;
                    boolean hasPrevious = validatedPage > 0;
                    return new PagedResponse<>(
                            orders,
                            validatedPage,
                            validatedSize,
                            totalElements,
                            totalPages,
                            first,
                            last,
                            hasNext,
                            hasPrevious
                    );
                });
    }

    @Override
    public Mono<OrderResponse> confirm(Long id) {
        log.info("Confirming order with id {}", id);

        Mono<OrderResponse> confirmFlow =
                orderRepository.findById(id)
                        .switchIfEmpty(Mono.error(new OrderNotFoundException(id)))
                        .flatMap(order -> {
                            if (order.getStatus() == OrderStatus.CONFIRMED) {
                                return Mono.error(
                                        new OrderAlreadyConfirmedException(id)
                                );
                            }

                            if (order.getStatus() != OrderStatus.CREATED) {
                                return Mono.error(
                                        new OrderCannotBeConfirmedException(
                                                id,
                                                order.getStatus().name()
                                        )
                                );
                            }

                            return updateOrderAsConfirmed(order)
                                    .flatMap(confirmedOrder ->
                                            orderItemRepository.findByOrderId(confirmedOrder.getId())
                                                    .collectList()
                                                    .flatMap(items ->
                                                            orderEventProducer
                                                                    .publishOrderConfirmed(
                                                                            toOrderConfirmedEvent(
                                                                                    confirmedOrder
                                                                            )
                                                                    )
                                                                    .thenReturn(
                                                                            toResponse(
                                                                                    confirmedOrder,
                                                                                    items
                                                                            )
                                                                    )
                                                    )
                                    );
                        });

        return transactionalOperator.transactional(confirmFlow);
    }

    @Override
    public Mono<OrderResponse> cancel(Long id) {
        log.info("Cancelling order with id {}", id);

        Mono<OrderResponse> cancelFlow =
                orderRepository.findById(id)
                        .switchIfEmpty(Mono.error(new OrderNotFoundException(id)))
                        .flatMap(order -> {
                            if (order.getStatus() == OrderStatus.CANCELLED) {
                                return Mono.error(
                                        new OrderAlreadyCancelledException(id)
                                );
                            }

                            return orderItemRepository.findByOrderId(order.getId())
                                    .collectList()
                                    .flatMap(items ->
                                            restoreStock(items)
                                                    .then(updateOrderAsCancelled(order))
                                                    .flatMap(cancelledOrder ->
                                                            orderEventProducer
                                                                    .publishOrderCancelled(
                                                                            toOrderCancelledEvent(
                                                                                    cancelledOrder
                                                                            )
                                                                    )
                                                                    .thenReturn(
                                                                            toResponse(
                                                                                    cancelledOrder,
                                                                                    items
                                                                            )
                                                                    )
                                                    )
                                    );
                        });

        return transactionalOperator.transactional(cancelFlow);
    }

    @Override
    public Mono<Void> confirmFromInventory(Long orderId) {
        log.info("Confirming order from inventory event. orderId={}", orderId);

        Mono<Void> flow =
                orderRepository.findById(orderId)
                        .switchIfEmpty(Mono.error(new OrderNotFoundException(orderId)))
                        .flatMap(order -> {
                            if (order.getStatus() != OrderStatus.CREATED) {
                                log.warn(
                                        "Order {} cannot be confirmed from inventory because status is {}",
                                        orderId,
                                        order.getStatus()
                                );
                                return Mono.empty();
                            }

                            order.setStatus(OrderStatus.CONFIRMED);
                            order.setUpdatedAt(OffsetDateTime.now());

                            return orderRepository.save(order);
                        })
                        .doOnSuccess(ignored ->
                                log.info(
                                        "Order confirmed from inventory event. orderId={}",
                                        orderId
                                )
                        )
                        .then();

        return transactionalOperator.transactional(flow);
    }

    @Override
    public Mono<Void> failFromInventory(
            Long orderId,
            String reason
    ) {
        log.info(
                "Marking order as failed from inventory event. orderId={}, reason={}",
                orderId,
                reason
        );

        Mono<Void> flow =
                orderRepository.findById(orderId)
                        .switchIfEmpty(Mono.error(new OrderNotFoundException(orderId)))
                        .flatMap(order -> {
                            if (order.getStatus() != OrderStatus.CREATED) {
                                log.warn(
                                        "Order {} cannot be failed from inventory because status is {}",
                                        orderId,
                                        order.getStatus()
                                );
                                return Mono.empty();
                            }

                            order.setStatus(OrderStatus.FAILED);
                            order.setUpdatedAt(OffsetDateTime.now());

                            return orderRepository.save(order);
                        })
                        .doOnSuccess(ignored ->
                                log.info(
                                        "Order marked as FAILED from inventory event. orderId={}",
                                        orderId
                                )
                        )
                        .then();

        return transactionalOperator.transactional(flow);
    }

    private OrderItem toOrderItem(
            CreateOrderItemRequest request,
            Product product
    ) {
        return OrderItem.builder()
                .productId(product.getId())
                .quantity(request.quantity())
                .price(product.getPrice())
                .build();
    }

    private Flux<OrderItem> saveOrderItems(
            Order savedOrder,
            List<OrderItem> orderItems
    ) {
        return Flux.fromIterable(orderItems)
                .map(item -> {
                    item.setOrderId(savedOrder.getId());
                    return item;
                })
                .flatMap(orderItemRepository::save);
    }

    private Order buildOrder(
            Long userId,
            BigDecimal totalAmount
    ) {
        OffsetDateTime now = OffsetDateTime.now();

        return Order.builder()
                .userId(userId)
                .status(OrderStatus.CREATED)
                .totalAmount(totalAmount)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private BigDecimal calculateTotalAmount(List<OrderItem> orderItems) {
        return orderItems.stream()
                .map(item ->
                        item.getPrice().multiply(
                                BigDecimal.valueOf(item.getQuantity())
                        )
                )
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private OrderResponse toResponse(
            Order order,
            List<OrderItem> items
    ) {
        List<OrderItemResponse> itemResponses = items.stream()
                .map(item -> new OrderItemResponse(
                        item.getId(),
                        item.getProductId(),
                        item.getQuantity(),
                        item.getPrice()
                ))
                .toList();

        return new OrderResponse(
                order.getId(),
                order.getUserId(),
                order.getStatus(),
                order.getTotalAmount(),
                itemResponses,
                order.getCreatedAt(),
                order.getUpdatedAt()
        );
    }

    private Flux<OrderItem> buildOrderItems(
            List<CreateOrderItemRequest> requests
    ) {
        return Flux.fromIterable(requests)
                .flatMap(itemRequest ->
                        productRepository.findById(itemRequest.productId())
                                .switchIfEmpty(
                                        Mono.error(
                                                new ProductNotFoundException(
                                                        itemRequest.productId()
                                                )
                                        )
                                )
                                .map(product ->
                                        toOrderItem(
                                                itemRequest,
                                                product
                                        )
                                )
                );
    }

    private void validateStock(
            Product product,
            Integer requestedQuantity
    ) {
        if (product.getStock() < requestedQuantity) {
            throw new InsufficientStockException(
                    product.getId(),
                    requestedQuantity,
                    product.getStock()
            );
        }
    }

    private void validateNoDuplicateProducts(
            CreateOrderRequest request
    ) {
        Set<Long> productIds = new HashSet<>();

        request.items().forEach(item -> {
            boolean added = productIds.add(item.productId());

            if (!added) {
                throw new DuplicateOrderItemException(item.productId());
            }
        });
    }

    private Mono<Void> restoreStock(List<OrderItem> items) {
        return Flux.fromIterable(items)
                .flatMap(item ->
                        productRepository.findById(item.getProductId())
                                .switchIfEmpty(Mono.error(
                                        new ProductNotFoundException(item.getProductId())
                                ))
                                .flatMap(product -> {
                                    product.setStock(
                                            product.getStock() + item.getQuantity()
                                    );

                                    return productRepository.save(product);
                                })
                )
                .then();
    }

    private Mono<Order> updateOrderAsCancelled(Order order) {
        order.setStatus(OrderStatus.CANCELLED);
        order.setUpdatedAt(OffsetDateTime.now());

        return orderRepository.save(order);
    }

    private Mono<Order> updateOrderAsConfirmed(Order order) {
        order.setStatus(OrderStatus.CONFIRMED);
        order.setUpdatedAt(OffsetDateTime.now());

        return orderRepository.save(order);
    }

    private OrderCreatedEvent toOrderCreatedEvent(
            Order order,
            List<OrderItem> items
    ) {
        List<OrderItemEvent> itemEvents =
                items.stream()
                        .map(item -> new OrderItemEvent(
                                item.getProductId(),
                                item.getQuantity(),
                                item.getPrice()
                        ))
                        .toList();

        return new OrderCreatedEvent(
                order.getId(),
                order.getUserId(),
                order.getTotalAmount(),
                itemEvents,
                order.getCreatedAt()
        );
    }

    private OrderConfirmedEvent toOrderConfirmedEvent(Order order) {
        return new OrderConfirmedEvent(
                order.getId(),
                order.getUserId(),
                OffsetDateTime.now()
        );
    }

    private OrderCancelledEvent toOrderCancelledEvent(Order order) {
        return new OrderCancelledEvent(
                order.getId(),
                order.getUserId(),
                OffsetDateTime.now()
        );
    }
}