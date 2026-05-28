package com.order.application.service.impl;

import com.order.application.mapper.InventoryMapper;
import com.order.application.service.InventoryService;
import com.order.application.service.OutboxService;
import com.order.domain.dto.response.InventoryResponse;
import com.order.domain.dto.response.PagedResponse;
import com.order.domain.enums.InventorySortField;
import com.order.domain.enums.SortDirection;
import com.order.domain.event.*;
import com.order.exception.InventoryNotFoundException;
import com.order.exception.InventoryReservationException;
import com.order.infrastructure.config.properties.OrderKafkaProperties;
import com.order.infrastructure.repository.InventoryRepository;
import com.order.infrastructure.repository.custom.InventoryCustomRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class InventoryServiceImpl implements InventoryService {

    private final InventoryRepository repository;
    private final InventoryCustomRepository customRepository;
    private final TransactionalOperator transactionalOperator;
    private final InventoryMapper mapper;
    private final OutboxService outboxService;
    private final OrderKafkaProperties kafkaProperties;

    private static final String AGGREGATE_TYPE_INVENTORY = "INVENTORY";

    private static final String EVENT_TYPE_INVENTORY_RESERVED = "INVENTORY_RESERVED";
    private static final String EVENT_TYPE_INVENTORY_FAILED = "INVENTORY_FAILED";
    private static final String EVENT_TYPE_INVENTORY_RELEASED = "INVENTORY_RELEASED";

    @Override
    public Mono<InventoryResponse> getByProductId(Long productId) {
        log.info("Getting inventory for productId {}", productId);

        return repository.findByProductId(productId)
                .switchIfEmpty(
                        Mono.error(
                                new InventoryNotFoundException(productId)
                        )
                )
                .map(mapper::toResponse);
    }

    @Override
    public Mono<PagedResponse<InventoryResponse>> getAll(
            int page,
            int size,
            InventorySortField sortBy,
            SortDirection direction
    ) {
        log.info(
                "Getting all inventory: page {}, size {}, sortBy {}, direction {}",
                page,
                size,
                sortBy,
                direction
        );

        int validatedPage = Math.max(page, 0);
        int validatedSize = Math.clamp(size, 1, 100);

        Mono<List<InventoryResponse>> inventoryMono =
                customRepository.findAllPaged(
                                validatedPage,
                                validatedSize,
                                sortBy,
                                direction
                        )
                        .map(mapper::toResponse)
                        .collectList();

        Mono<Long> countMono =
                customRepository.countAll();

        return Mono.zip(inventoryMono, countMono)
                .map(tuple -> {
                    List<InventoryResponse> inventory = tuple.getT1();
                    long totalElements = tuple.getT2();

                    int totalPages = (int) Math.ceil((double) totalElements / validatedSize);

                    boolean first = validatedPage == 0;
                    boolean last = totalPages == 0 || validatedPage >= totalPages - 1;
                    boolean hasNext = validatedPage < totalPages - 1;
                    boolean hasPrevious = validatedPage > 0;

                    return new PagedResponse<>(
                            inventory,
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
    public Mono<InventoryReservedEvent> reserve(OrderCreatedEvent event) {
        log.info("Reserving inventory for orderId {}", event.orderId());

        Mono<InventoryReservedEvent> reservationFlow =
                Flux.fromIterable(event.items())
                        .flatMap(this::reserveSingleItem)
                        .collectList()
                        .map(reservedItems ->
                                new InventoryReservedEvent(
                                        event.orderId(),
                                        reservedItems,
                                        OffsetDateTime.now()
                                )
                        )
                        .flatMap(reservedEvent ->
                                outboxService.saveEvent(
                                                AGGREGATE_TYPE_INVENTORY,
                                                reservedEvent.orderId(),
                                                EVENT_TYPE_INVENTORY_RESERVED,
                                                kafkaProperties.getTopics().getInventoryReserved(),
                                                reservedEvent.orderId().toString(),
                                                reservedEvent
                                        )
                                        .thenReturn(reservedEvent)
                        );

        return transactionalOperator.transactional(reservationFlow);
    }

    private Mono<InventoryReservedItemEvent> reserveSingleItem(
            OrderItemEvent item
    ) {
        return repository.findByProductId(item.productId())
                .switchIfEmpty(
                        Mono.error(
                                new InventoryReservationException(
                                        "Inventory not found for product id: " + item.productId()
                                )
                        )
                )
                .flatMap(inventory -> {
                    if (inventory.getAvailableQuantity() < item.quantity()) {
                        return Mono.error(
                                new InventoryReservationException(
                                        "Insufficient inventory for product id: "
                                                + item.productId()
                                                + ". Requested: "
                                                + item.quantity()
                                                + ", available: "
                                                + inventory.getAvailableQuantity()
                                )
                        );
                    }

                    inventory.setAvailableQuantity(
                            inventory.getAvailableQuantity() - item.quantity()
                    );

                    inventory.setReservedQuantity(
                            inventory.getReservedQuantity() + item.quantity()
                    );

                    inventory.setUpdatedAt(OffsetDateTime.now());

                    return repository.save(inventory);
                })
                .map(savedInventory ->
                        new InventoryReservedItemEvent(
                                savedInventory.getProductId(),
                                item.quantity()
                        )
                );
    }

    @Override
    public Mono<InventoryReleasedEvent> release(OrderCancelledEvent event) {
        log.info("Releasing inventory for cancelled orderId {}", event.orderId());

        if (event.items() == null || event.items().isEmpty()) {
            return Mono.error(
                    new InventoryReservationException(
                            "Cannot release inventory for order id: "
                                    + event.orderId()
                                    + ". Cancelled event does not contain order items."
                    )
            );
        }

        Mono<InventoryReleasedEvent> releaseFlow =
                Flux.fromIterable(event.items())
                        .flatMap(this::releaseSingleItem)
                        .collectList()
                        .map(releasedItems ->
                                new InventoryReleasedEvent(
                                        event.orderId(),
                                        releasedItems,
                                        OffsetDateTime.now()
                                )
                        )
                        .flatMap(releasedEvent ->
                                outboxService.saveEvent(
                                                AGGREGATE_TYPE_INVENTORY,
                                                releasedEvent.orderId(),
                                                EVENT_TYPE_INVENTORY_RELEASED,
                                                kafkaProperties.getTopics().getInventoryReleased(),
                                                releasedEvent.orderId().toString(),
                                                releasedEvent
                                        )
                                        .thenReturn(releasedEvent)
                        );

        return transactionalOperator.transactional(releaseFlow);
    }

    private Mono<InventoryReleasedItemEvent> releaseSingleItem(
            OrderItemEvent item
    ) {
        return repository.findByProductId(item.productId())
                .switchIfEmpty(
                        Mono.error(
                                new InventoryReservationException(
                                        "Inventory not found for product id: " + item.productId()
                                )
                        )
                )
                .flatMap(inventory -> {
                    int reservedQuantity =
                            inventory.getReservedQuantity();

                    if (reservedQuantity < item.quantity()) {
                        return Mono.error(
                                new InventoryReservationException(
                                        "Cannot release inventory for product id: "
                                                + item.productId()
                                                + ". Requested release: "
                                                + item.quantity()
                                                + ", reserved: "
                                                + reservedQuantity
                                )
                        );
                    }

                    inventory.setAvailableQuantity(
                            inventory.getAvailableQuantity() + item.quantity()
                    );

                    inventory.setReservedQuantity(
                            inventory.getReservedQuantity() - item.quantity()
                    );

                    inventory.setUpdatedAt(OffsetDateTime.now());

                    return repository.save(inventory);
                })
                .map(savedInventory ->
                        new InventoryReleasedItemEvent(
                                savedInventory.getProductId(),
                                item.quantity()
                        )
                );
    }
}