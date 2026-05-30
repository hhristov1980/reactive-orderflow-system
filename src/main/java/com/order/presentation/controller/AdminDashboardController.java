package com.order.presentation.controller.admin;

import com.order.application.service.AdminService;
import com.order.domain.dto.response.admin.AdminDashboardResponse;
import io.swagger.v3.oas.annotations.Operation;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/admin")
@RequiredArgsConstructor
public class AdminDashboardController {

    private final AdminService adminService;

    @Operation(summary = "Get admin dashboard")
    @GetMapping("/dashboard")
    public Mono<ResponseEntity<AdminDashboardResponse>> getDashboard() {
        return adminService.getDashboard()
                .map(ResponseEntity::ok);
    }
}