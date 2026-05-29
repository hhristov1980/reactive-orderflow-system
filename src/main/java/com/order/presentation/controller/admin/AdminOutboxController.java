package com.order.presentation.controller.admin;

import com.order.application.service.AdminService;
import com.order.domain.dto.response.PagedResponse;
import com.order.domain.dto.response.admin.OutboxEventResponse;
import com.order.domain.enums.OutboxStatus;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/admin/outbox-events")
@RequiredArgsConstructor
@Validated
public class AdminOutboxController {

    private final AdminService adminService;

    @Operation(summary = "Get outbox events")
    @GetMapping
    public Mono<ResponseEntity<PagedResponse<OutboxEventResponse>>> getOutboxEvents(
            @RequestParam(defaultValue = "0")
            int page,

            @RequestParam(defaultValue = "20")
            int size,

            @RequestParam(required = false)
            OutboxStatus status
    ) {
        return adminService.getOutboxEvents(
                        page,
                        size,
                        status
                )
                .map(ResponseEntity::ok);
    }

    @Operation(summary = "Get outbox event by id")
    @GetMapping("/{id}")
    public Mono<ResponseEntity<OutboxEventResponse>> getOutboxEventById(
            @PathVariable @Positive Long id
    ) {
        return adminService.getOutboxEventById(id)
                .map(ResponseEntity::ok);
    }

    @Operation(summary = "Retry outbox event")
    @PatchMapping("/{id}/retry")
    public Mono<ResponseEntity<OutboxEventResponse>> retryOutboxEvent(
            @PathVariable @Positive Long id
    ) {
        return adminService.retryOutboxEvent(id)
                .map(ResponseEntity::ok);
    }
}