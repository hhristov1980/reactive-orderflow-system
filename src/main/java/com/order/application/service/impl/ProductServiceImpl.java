package com.order.application.service.impl;

import com.order.application.mapper.ProductMapper;
import com.order.application.service.ProductService;
import com.order.domain.dto.request.CreateProductRequest;
import com.order.domain.dto.request.UpdateProductRequest;
import com.order.domain.dto.response.PagedResponse;
import com.order.domain.dto.response.ProductResponse;
import com.order.domain.entity.Product;
import com.order.domain.enums.ProductSortField;
import com.order.domain.enums.SortDirection;
import com.order.exception.ProductNotFoundException;
import com.order.infrastructure.repository.ProductRepository;
import com.order.infrastructure.repository.custom.ProductCustomRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class ProductServiceImpl implements ProductService {

    private final ProductRepository repository;
    private final ProductCustomRepository customRepository;
    private final ProductMapper mapper;

    @Override
    public Mono<ProductResponse> create(CreateProductRequest request) {
        log.info("Creating product with name: {}", request.name());

        Product product = mapper.toEntity(request);
        product.setId(null);
        initializeAuditFields(product);
        return repository.save(product)
                .map(mapper::toResponse)
                .doOnSuccess(saved ->
                        {
                            assert saved != null;
                            log.info("Product created successfully with id {}", saved.id());
                        }
                )
                .doOnError(error ->
                        log.error("Failed to create product",error)
                );
    }

    @Override
    public Mono<PagedResponse<ProductResponse>> getAll(
            int page,
            int size,
            ProductSortField sortBy,
            SortDirection direction) {

        log.info("Getting all products: page {}, size {}, sortBy {}, direction {}", page, size, sortBy, direction);
        page = page - 1; // avoid having page == 0 in the request
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
                    List<ProductResponse> products = tuple.getT1();
                    long totalElements = tuple.getT2();
                    int totalPages = (int) Math.ceil((double) totalElements / validatedSize);
                    return new PagedResponse<>(
                            products,
                            validatedPage,
                            validatedSize,
                            totalElements,
                            totalPages,
                            validatedPage == 0,
                            validatedPage >= totalPages - 1,
                            validatedPage < totalPages - 1,
                            validatedPage > 0
                    );
                });
    }

    @Override
    public Mono<ProductResponse> getById(Long id) {
        log.info("Getting product with id {}", id);
        return repository.findById(id)
                .switchIfEmpty(Mono.error(new ProductNotFoundException(id)))
                .map(mapper::toResponse);
    }

    @Override
    public Mono<ProductResponse> update(Long id, UpdateProductRequest request) {
        log.info("Updating product with id {}", id);
        return repository.findById(id)
                .switchIfEmpty(Mono.error(new ProductNotFoundException(id)))
                .flatMap(product -> {
                    mapper.updateProduct(request, product);
                    updateAuditFields(product);
                    return repository.save(product);
                })
                .map(mapper::toResponse);
    }

    @Override
    public Mono<Void> delete(Long id) {
        log.info("Deleting product with id {}", id);
        return repository.findById(id)
                .switchIfEmpty(Mono.error(new ProductNotFoundException(id)))
                .flatMap(repository::delete);
    }

    private void initializeAuditFields(Product product) {
        OffsetDateTime now = OffsetDateTime.now();
        product.setCreatedAt(now);
        product.setUpdatedAt(now);
    }

    private void updateAuditFields(Product product) {
        product.setUpdatedAt(OffsetDateTime.now());
    }
}