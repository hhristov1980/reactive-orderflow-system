package com.order.presentation.controller.admin;

import com.order.application.service.AdminService;
import com.order.domain.dto.response.PagedResponse;
import com.order.domain.dto.response.admin.AuditEventResponse;
import io.swagger.v3.oas.annotations.Operation;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/admin/audit-events")
@RequiredArgsConstructor
@Validated
public class AdminAuditController {

    private final AdminService adminService;

    @Operation(summary = "Get audit events")
    @GetMapping
    public Mono<ResponseEntity<PagedResponse<AuditEventResponse>>> getAuditEvents(
            @RequestParam(defaultValue = "0")
            int page,

            @RequestParam(defaultValue = "20")
            int size
    ) {
        return adminService.getAuditEvents(page, size)
                .map(ResponseEntity::ok);
    }

    @Operation(summary = "Get audit event by id")
    @GetMapping("/{id}")
    public Mono<ResponseEntity<AuditEventResponse>> getAuditEventById(
            @PathVariable @Positive Long id
    ) {
        return adminService.getAuditEventById(id)
                .map(ResponseEntity::ok);
    }

    @Operation(summary = "Get audit events by order id")
    @GetMapping("/orders/{orderId}")
    public Mono<ResponseEntity<PagedResponse<AuditEventResponse>>> getAuditEventsByOrderId(
            @PathVariable @Positive Long orderId,

            @RequestParam(defaultValue = "0")
            int page,

            @RequestParam(defaultValue = "20")
            int size
    ) {
        return adminService.getAuditEventsByOrderId(
                        orderId,
                        page,
                        size
                )
                .map(ResponseEntity::ok);
    }
}