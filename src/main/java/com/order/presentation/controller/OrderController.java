package com.order.presentation.controller;

import com.order.application.service.OrderService;
import com.order.domain.dto.request.CreateOrderRequest;
import com.order.domain.dto.response.OrderResponse;
import com.order.domain.dto.response.PagedResponse;
import com.order.domain.enums.OrderSortField;
import com.order.domain.enums.SortDirection;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
@Validated
public class OrderController {

    private final OrderService service;

    @Operation(summary = "Create order")
    @PostMapping
    public Mono<ResponseEntity<OrderResponse>> create(
            @Valid @RequestBody CreateOrderRequest request
    ) {
        return service.create(request)
                .map(response ->
                        ResponseEntity
                                .status(HttpStatus.CREATED)
                                .body(response)
                );
    }

    @Operation(summary = "Get order by id")
    @GetMapping("/{id}")
    public Mono<ResponseEntity<OrderResponse>> getById(
            @PathVariable @Positive Long id
    ) {
        return service.getById(id)
                .map(ResponseEntity::ok);
    }

    @Operation(summary = "Get all orders")
    @GetMapping
    public Mono<ResponseEntity<PagedResponse<OrderResponse>>> getAll(

            @RequestParam(defaultValue = "0")
            int page,
            @RequestParam(defaultValue = "20")
            int size,
            @RequestParam(defaultValue = "ID")
            OrderSortField sortBy,
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

    @Operation(summary = "Confirm order")
    @PatchMapping("/{id}/confirm")
    public Mono<ResponseEntity<OrderResponse>> confirm(
            @PathVariable @Positive Long id
    ) {
        return service.confirm(id)
                .map(ResponseEntity::ok);
    }

    @Operation(summary = "Cancel order")
    @PatchMapping("/{id}/cancel")
    public Mono<ResponseEntity<OrderResponse>> cancel(
            @PathVariable @Positive Long id
    ) {
        return service.cancel(id)
                .map(ResponseEntity::ok);
    }
}