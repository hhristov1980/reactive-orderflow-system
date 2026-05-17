package com.order.infrastructure.repository.custom;

import com.order.domain.entity.Order;
import com.order.domain.enums.OrderSortField;
import com.order.domain.enums.SortDirection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface OrderCustomRepository {

    Flux<Order> findAllPaged(
            int page,
            int size,
            OrderSortField sortBy,
            SortDirection direction
    );

    Mono<Long> countAll();
}