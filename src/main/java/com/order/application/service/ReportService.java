package com.order.application.service;

import com.order.domain.dto.response.report.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReportService {

    Mono<OrderSummaryReportResponse> getOrderSummary();

    Mono<RevenueReportResponse> getRevenueReport();

    Mono<InventoryReportResponse> getInventoryReport();

    Mono<PaymentReportResponse> getPaymentReport();

    Mono<DashboardReportResponse> getDashboard();

    Flux<TopProductReportResponse> getTopProducts(int limit);
}