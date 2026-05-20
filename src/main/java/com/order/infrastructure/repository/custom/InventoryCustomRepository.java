package com.order.infrastructure.repository.custom;

import com.order.domain.entity.Inventory;
import com.order.domain.enums.InventorySortField;
import com.order.domain.enums.SortDirection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface InventoryCustomRepository {

    Flux<Inventory> findAllPaged(
            int page,
            int size,
            InventorySortField sortBy,
            SortDirection direction
    );

    Mono<Long> countAll();
}