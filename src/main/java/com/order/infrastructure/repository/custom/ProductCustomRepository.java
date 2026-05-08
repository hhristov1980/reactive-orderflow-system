package com.order.infrastructure.repository.custom;

import com.order.domain.entity.Product;
import com.order.domain.enums.ProductSortField;
import com.order.domain.enums.SortDirection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ProductCustomRepository {

    Flux<Product> findAllPaged(
            int page,
            int size,
            ProductSortField sortBy,
            SortDirection direction
    );

    Mono<Long> countAll();
}