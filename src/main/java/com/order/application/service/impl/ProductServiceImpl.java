package com.order.service.impl;

import com.order.mapper.ProductMapper;
import com.order.domain.dto.request.CreateProductRequest;
import com.order.domain.dto.response.ProductResponse;
import com.order.domain.entity.Product;
import com.order.exception.ProductNotFoundException;
import com.order.infrastructure.repository.ProductRepository;
import com.order.service.ProductService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;

@Service
@RequiredArgsConstructor
public class ProductServiceImpl implements ProductService {

    private final ProductRepository repository;
    private final ProductMapper mapper;

    @Override
    public Mono<ProductResponse> create(CreateProductRequest request) {

        Product entity = mapper.toEntity(request);

        return repository.save(entity)
                .map(mapper::toResponse);
    }

    @Override
    public Mono<List<ProductResponse>> getAll() {

        return repository.findAll()
                .map(mapper::toResponse)
                .collectList();
    }

    @Override
    public Mono<ProductResponse> getById(Long id) {

        return repository.findById(id)
                .switchIfEmpty(Mono.error(new ProductNotFoundException(id)))
                .map(mapper::toResponse);
    }
}
