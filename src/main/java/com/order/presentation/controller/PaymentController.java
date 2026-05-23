package com.order.presentation.controller;

import com.order.application.service.PaymentService;
import com.order.domain.dto.response.PaymentResponse;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/payments")
@RequiredArgsConstructor
@Validated
public class PaymentController {

    private final PaymentService service;

    @Operation(summary = "Get payment by id")
    @GetMapping("/{id}")
    public Mono<ResponseEntity<PaymentResponse>> getById(
            @PathVariable @Positive Long id
    ) {
        return service.getById(id)
                .map(ResponseEntity::ok);
    }

    @Operation(summary = "Get payment by order id")
    @GetMapping("/orders/{orderId}")
    public Mono<ResponseEntity<PaymentResponse>> getByOrderId(
            @PathVariable @Positive Long orderId
    ) {
        return service.getByOrderId(orderId)
                .map(ResponseEntity::ok);
    }

    @Operation(summary = "Complete payment manually")
    @PatchMapping("/{id}/complete")
    public Mono<ResponseEntity<PaymentResponse>> complete(
            @PathVariable @Positive Long id
    ) {
        return service.complete(id)
                .map(ResponseEntity::ok);
    }

    @Operation(summary = "Fail payment manually")
    @PatchMapping("/{id}/fail")
    public Mono<ResponseEntity<PaymentResponse>> fail(
            @PathVariable @Positive Long id,
            @RequestParam(defaultValue = "Manual payment failure")
            String reason
    ) {
        return service.fail(id, reason)
                .map(ResponseEntity::ok);
    }
}