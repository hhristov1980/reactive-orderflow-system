package com.order.infrastructure.repository.report;

import com.order.domain.dto.response.admin.OutboxSummaryResponse;
import com.order.domain.dto.response.report.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReportRepository {

    Mono<OrderSummaryReportResponse> getOrderSummary();

    Mono<RevenueReportResponse> getRevenueReport();

    Mono<InventoryReportResponse> getInventoryReport();

    Mono<PaymentReportResponse> getPaymentReport();

    Flux<TopProductReportResponse> getTopProducts(int limit);

    Mono<OutboxSummaryResponse> getOutboxSummary();
}