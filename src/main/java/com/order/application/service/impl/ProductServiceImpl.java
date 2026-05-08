package com.order.application.service.impl;

import com.order.application.mapper.ProductMapper;
import com.order.application.service.ProductService;
import com.order.domain.dto.request.CreateProductRequest;
import com.order.domain.dto.response.PagedResponse;
import com.order.domain.dto.response.ProductResponse;
import com.order.domain.entity.Product;
import com.order.domain.enums.ProductSortField;
import com.order.domain.enums.SortDirection;
import com.order.exception.ProductNotFoundException;
import com.order.infrastructure.repository.ProductRepository;
import com.order.infrastructure.repository.custom.ProductCustomRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
public class ProductServiceImpl implements ProductService {

    private final ProductRepository repository;
    private final ProductCustomRepository customRepository;
    private final ProductMapper mapper;

    @Override
    public Mono<ProductResponse> create(
            CreateProductRequest request
    ) {

        Product product = mapper.toEntity(request);

        OffsetDateTime now = OffsetDateTime.now();

        product.setCreatedAt(now);
        product.setUpdatedAt(now);

        return repository.save(product)
                .map(mapper::toResponse);
    }

    @Override
    public Mono<PagedResponse<ProductResponse>> getAll(
            int page,
            int size,
            ProductSortField sortBy,
            SortDirection direction
    ) {

        int validatedPage = Math.max(page, 0);

        int validatedSize = Math.clamp(size, 1, 100);

        Mono<List<ProductResponse>> productsMono =
                customRepository.findAllPaged(
                                validatedPage,
                                validatedSize,
                                sortBy,
                                direction
                        )
                        .map(mapper::toResponse)
                        .collectList();

        Mono<Long> countMono =
                customRepository.countAll();

        return Mono.zip(productsMono, countMono)
                .map(tuple -> {

                    List<ProductResponse> products =
                            tuple.getT1();

                    long totalElements =
                            tuple.getT2();

                    int totalPages =
                            (int) Math.ceil(
                                    (double) totalElements / validatedSize
                            );

                    return new PagedResponse<>(
                            products,
                            validatedPage,
                            validatedSize,
                            totalElements,
                            totalPages,
                            page == 0,
                            page >= totalPages - 1
                    );
                });
    }

    @Override
    public Mono<ProductResponse> getById(Long id) {

        return repository.findById(id)
                .switchIfEmpty(
                        Mono.error(
                                new ProductNotFoundException(id)
                        )
                )

                .map(mapper::toResponse);
    }
}
