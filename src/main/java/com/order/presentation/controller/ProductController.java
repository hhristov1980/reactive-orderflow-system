package com.order.controller;

import com.order.domain.dto.request.CreateProductRequest;
import com.order.domain.dto.response.ProductResponse;
import com.order.service.ProductService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequestMapping("/products")
@RequiredArgsConstructor
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
    public Mono<ResponseEntity<List<ProductResponse>>> getAll() {

        return service.getAll()
                .map(ResponseEntity::ok);
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<ProductResponse>> getById(
           @Valid @PathVariable Long id
    ) {

        return service.getById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }
}
