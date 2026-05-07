package com.order.service;

import com.order.domain.dto.request.CreateProductRequest;
import com.order.domain.dto.response.ProductResponse;
import reactor.core.publisher.Mono;

import java.util.List;

public interface ProductService {
    Mono<ProductResponse> create(CreateProductRequest request);

    Mono<List<ProductResponse>> getAll();

    Mono<ProductResponse> getById(Long id);
}
