package com.order.presentation.controller;

import com.order.application.service.InventoryService;
import com.order.domain.dto.response.InventoryResponse;
import com.order.domain.dto.response.PagedResponse;
import com.order.domain.enums.InventorySortField;
import com.order.domain.enums.SortDirection;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/inventory")
@RequiredArgsConstructor
@Validated
public class InventoryController {

    private final InventoryService service;

    @Operation(summary = "Get inventory by product id")
    @GetMapping("/products/{productId}")
    public Mono<ResponseEntity<InventoryResponse>> getByProductId(
            @PathVariable @Positive Long productId
    ) {
        return service.getByProductId(productId)
                .map(ResponseEntity::ok);
    }

    @Operation(summary = "Get all inventory")
    @GetMapping("/products")
    public Mono<ResponseEntity<PagedResponse<InventoryResponse>>> getAll(

            @RequestParam(defaultValue = "0")
            int page,
            @RequestParam(defaultValue = "20")
            int size,
            @RequestParam(defaultValue = "ID")
            InventorySortField sortBy,
            @RequestParam(defaultValue = "ASC")
            SortDirection direction
    ) {
        return service.getAll(
                        page,
                        size,
                        sortBy,
                        direction
                )
                .map(ResponseEntity::ok);
    }
}