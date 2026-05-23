package com.order.application.service;

import com.order.domain.dto.request.CreateOrderRequest;
import com.order.domain.dto.response.OrderResponse;
import com.order.domain.dto.response.PagedResponse;
import com.order.domain.enums.OrderSortField;
import com.order.domain.enums.SortDirection;
import reactor.core.publisher.Mono;

public interface OrderService {

    Mono<OrderResponse> create(CreateOrderRequest request);

    Mono<PagedResponse<OrderResponse>> getAll(
            int page,
            int size,
            OrderSortField sortBy,
            SortDirection direction
    );

    Mono<OrderResponse> getById(Long id);

    Mono<OrderResponse> cancel(Long id);

    Mono<OrderResponse> confirm(Long id);

    Mono<Void> confirmFromInventory(Long orderId);

    Mono<Void> failFromInventory(Long orderId, String reason);

    Mono<Void> cancelFromPaymentFailure(Long orderId, String reason);
}