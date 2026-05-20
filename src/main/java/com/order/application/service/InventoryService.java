package com.order.application.service;

import com.order.domain.dto.response.InventoryResponse;
import com.order.domain.dto.response.PagedResponse;
import com.order.domain.enums.InventorySortField;
import com.order.domain.enums.SortDirection;
import com.order.domain.event.InventoryReservedEvent;
import com.order.domain.event.OrderCreatedEvent;
import reactor.core.publisher.Mono;

public interface InventoryService {

    Mono<InventoryResponse> getByProductId(Long productId);

    Mono<PagedResponse<InventoryResponse>> getAll(
            int page,
            int size,
            InventorySortField sortBy,
            SortDirection direction
    );

    Mono<InventoryReservedEvent> reserve(OrderCreatedEvent event);
}