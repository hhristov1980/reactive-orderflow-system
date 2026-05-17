package com.order.infrastructure.repository.custom;

import com.order.domain.entity.User;
import com.order.domain.enums.SortDirection;
import com.order.domain.enums.UserSortField;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface UserCustomRepository {

    Flux<User> findAllPaged(
            int page,
            int size,
            UserSortField sortBy,
            SortDirection direction
    );

    Mono<Long> countAll();
}