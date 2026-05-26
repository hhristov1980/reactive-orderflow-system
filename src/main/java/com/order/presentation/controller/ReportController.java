package com.order.presentation.controller;

import com.order.application.service.ReportService;
import com.order.domain.dto.response.report.*;
import io.swagger.v3.oas.annotations.Operation;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequestMapping("/api/v1/reports")
@RequiredArgsConstructor
public class ReportController {

    private final ReportService service;

    @Operation(summary = "Get order summary report")
    @GetMapping("/orders/summary")
    public Mono<ResponseEntity<OrderSummaryReportResponse>> getOrderSummary() {
        return service.getOrderSummary()
                .map(ResponseEntity::ok);
    }

    @Operation(summary = "Get revenue report")
    @GetMapping("/revenue")
    public Mono<ResponseEntity<RevenueReportResponse>> getRevenueReport() {
        return service.getRevenueReport()
                .map(ResponseEntity::ok);
    }

    @Operation(summary = "Get inventory report")
    @GetMapping("/inventory")
    public Mono<ResponseEntity<InventoryReportResponse>> getInventoryReport() {
        return service.getInventoryReport()
                .map(ResponseEntity::ok);
    }

    @Operation(summary = "Get payment report")
    @GetMapping("/payments")
    public Mono<ResponseEntity<PaymentReportResponse>> getPaymentReport() {
        return service.getPaymentReport()
                .map(ResponseEntity::ok);
    }

    @Operation(summary = "Get dashboard report")
    @GetMapping("/dashboard")
    public Mono<ResponseEntity<DashboardReportResponse>> getDashboard() {
        return service.getDashboard()
                .map(ResponseEntity::ok);
    }

    @Operation(summary = "Get top products report")
    @GetMapping("/top-products")
    public Mono<ResponseEntity<List<TopProductReportResponse>>> getTopProducts(
            @RequestParam(defaultValue = "5")
            int limit
    ) {
        return service.getTopProducts(limit)
                .collectList()
                .map(ResponseEntity::ok);
    }
}