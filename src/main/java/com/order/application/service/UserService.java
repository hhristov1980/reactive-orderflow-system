package com.order.application.service;

import com.order.domain.dto.request.CreateUserRequest;
import com.order.domain.dto.request.UpdateUserRequest;
import com.order.domain.dto.response.PagedResponse;
import com.order.domain.dto.response.UserResponse;
import com.order.domain.enums.SortDirection;
import com.order.domain.enums.UserSortField;
import reactor.core.publisher.Mono;

public interface UserService {

    Mono<UserResponse> create(CreateUserRequest request);

    Mono<PagedResponse<UserResponse>> getAll(
            int page,
            int size,
            UserSortField sortBy,
            SortDirection direction
    );

    Mono<UserResponse> getById(Long id);

    Mono<UserResponse> update(Long id, UpdateUserRequest request);

    Mono<Void> delete(Long id);
}