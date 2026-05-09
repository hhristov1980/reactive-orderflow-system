package com.order.presentation.controller;

import com.order.domain.dto.request.CreateProductRequest;
import com.order.domain.dto.request.UpdateProductRequest;
import com.order.domain.dto.response.PagedResponse;
import com.order.domain.dto.response.ProductResponse;
import com.order.application.service.ProductService;
import com.order.domain.enums.ProductSortField;
import com.order.domain.enums.SortDirection;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/products")
@Tag(name = "Products", description = "Product APIs")
public class ProductController {


    private final ProductService service;

    @Operation(summary = "Create product")
    @PostMapping
    public Mono<ResponseEntity<ProductResponse>> create(
            @Valid @RequestBody CreateProductRequest request
    ) {

        return service.create(request)
                .map(response ->
                        ResponseEntity
                                .status(HttpStatus.CREATED)
                                .body(response)
                );
    }

    @GetMapping
    public Mono<ResponseEntity<PagedResponse<ProductResponse>>> getAll(
            @RequestParam(defaultValue = "1") @Valid @Min(value = 1)
            int page,
            @RequestParam(defaultValue = "20") @Valid @Min(value = 1)
            int size,
            @RequestParam(defaultValue = "ID")
            ProductSortField sortBy,
            @RequestParam(defaultValue = "ASC")
            SortDirection direction) {

        return service.getAll(
                        page,
                        size,
                        sortBy,
                        direction
                )
                .map(ResponseEntity::ok);
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<ProductResponse>> getById(
            @Positive @PathVariable Long id) {

        return service.getById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}")
    @Operation(summary = "Update product")
    public Mono<ResponseEntity<ProductResponse>> update(
            @PathVariable
            @Positive
            Long id,
            @Valid
            @RequestBody
            UpdateProductRequest request) {
        return service.update(id, request)
                .map(ResponseEntity::ok);
    }

    @DeleteMapping("/{id}")
    @Operation(summary = "Delete product")
    public Mono<ResponseEntity<Void>> delete(
            @PathVariable
            @Positive
            Long id) {
        return service.delete(id)
                .then(Mono.just(ResponseEntity.noContent().build()));
    }
}
