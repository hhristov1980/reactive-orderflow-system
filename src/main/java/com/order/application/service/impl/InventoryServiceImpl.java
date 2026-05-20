package com.order.application.service.impl;

import com.order.application.mapper.InventoryMapper;
import com.order.application.service.InventoryService;
import com.order.domain.dto.response.InventoryResponse;
import com.order.domain.dto.response.PagedResponse;
import com.order.domain.enums.InventorySortField;
import com.order.domain.enums.SortDirection;
import com.order.domain.event.InventoryReservedEvent;
import com.order.domain.event.InventoryReservedItemEvent;
import com.order.domain.event.OrderCreatedEvent;
import com.order.domain.event.OrderItemEvent;
import com.order.exception.InventoryNotFoundException;
import com.order.exception.InventoryReservationException;
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
}