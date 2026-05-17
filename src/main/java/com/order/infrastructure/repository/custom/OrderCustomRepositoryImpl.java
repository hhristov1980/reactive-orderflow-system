package com.order.infrastructure.repository.custom;

import com.order.domain.entity.Order;
import com.order.domain.enums.OrderSortField;
import com.order.domain.enums.SortDirection;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.relational.core.query.Query;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
@RequiredArgsConstructor
public class OrderCustomRepositoryImpl implements OrderCustomRepository {

    private final R2dbcEntityTemplate template;

    @Override
    public Flux<Order> findAllPaged(
            int page,
            int size,
            OrderSortField sortBy,
            SortDirection direction
    ) {

        Sort.Direction sortDirection =
                direction == SortDirection.DESC
                        ? Sort.Direction.DESC
                        : Sort.Direction.ASC;

        Query query = Query.empty()
                .sort(Sort.by(sortDirection, sortBy.field()))
                .limit(size)
                .offset((long) page * size);

        return template.select(query, Order.class);
    }

    @Override
    public Mono<Long> countAll() {
        return template.count(
                Query.empty(),
                Order.class
        );
    }
}