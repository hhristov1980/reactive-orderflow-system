package com.order.application.service;

import com.order.domain.dto.request.CreateProductRequest;
import com.order.domain.dto.request.UpdateProductRequest;
import com.order.domain.dto.response.PagedResponse;
import com.order.domain.dto.response.ProductResponse;
import com.order.domain.enums.ProductSortField;
import com.order.domain.enums.SortDirection;
import reactor.core.publisher.Mono;

import java.util.List;

public interface ProductService {
    Mono<ProductResponse> create(CreateProductRequest request);

    Mono<PagedResponse<ProductResponse>> getAll(
            int page,
            int size,
            ProductSortField sortBy,
            SortDirection direction
    );

    Mono<ProductResponse> getById(Long id);

    Mono<ProductResponse> update(
            Long id,
            UpdateProductRequest request
    );

    Mono<Void> delete(Long id);
}
