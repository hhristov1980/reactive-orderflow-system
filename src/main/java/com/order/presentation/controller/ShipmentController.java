package com.order.presentation.controller;

import com.order.application.service.ShipmentService;
import com.order.domain.dto.response.ShipmentResponse;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/shipments")
@RequiredArgsConstructor
@Validated
public class ShipmentController {

    private final ShipmentService service;

    @Operation(summary = "Get shipment by id")
    @GetMapping("/{id}")
    public Mono<ResponseEntity<ShipmentResponse>> getById(
            @PathVariable @Positive Long id
    ) {
        return service.getById(id)
                .map(ResponseEntity::ok);
    }

    @Operation(summary = "Get shipment by order id")
    @GetMapping("/orders/{orderId}")
    public Mono<ResponseEntity<ShipmentResponse>> getByOrderId(
            @PathVariable @Positive Long orderId
    ) {
        return service.getByOrderId(orderId)
                .map(ResponseEntity::ok);
    }

    @Operation(summary = "Mark shipment as shipped")
    @PatchMapping("/{id}/ship")
    public Mono<ResponseEntity<ShipmentResponse>> markAsShipped(
            @PathVariable @Positive Long id
    ) {
        return service.markAsShipped(id)
                .map(ResponseEntity::ok);
    }

    @Operation(summary = "Mark shipment as delivered")
    @PatchMapping("/{id}/deliver")
    public Mono<ResponseEntity<ShipmentResponse>> markAsDelivered(
            @PathVariable @Positive Long id
    ) {
        return service.markAsDelivered(id)
                .map(ResponseEntity::ok);
    }
}