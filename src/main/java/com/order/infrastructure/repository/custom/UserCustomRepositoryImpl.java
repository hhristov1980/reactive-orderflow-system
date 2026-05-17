package com.order.infrastructure.repository.custom;

import com.order.domain.entity.User;
import com.order.domain.enums.SortDirection;
import com.order.domain.enums.UserSortField;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.relational.core.query.Query;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
@RequiredArgsConstructor
public class UserCustomRepositoryImpl implements UserCustomRepository {

    private final R2dbcEntityTemplate template;

    @Override
    public Flux<User> findAllPaged(
            int page,
            int size,
            UserSortField sortBy,
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

        return template.select(query, User.class);
    }

    @Override
    public Mono<Long> countAll() {
        return template.count(
                Query.empty(),
                User.class
        );
    }
}