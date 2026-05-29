package com.order.application.service.impl;

import com.order.application.service.AdminService;
import com.order.domain.dto.response.admin.AdminDashboardResponse;
import com.order.domain.dto.response.admin.OutboxSummaryResponse;
import com.order.domain.dto.response.report.InventoryReportResponse;
import com.order.domain.dto.response.report.OrderSummaryReportResponse;
import com.order.domain.dto.response.report.PaymentReportResponse;
import com.order.domain.dto.response.report.RevenueReportResponse;
import com.order.domain.dto.response.report.TopProductReportResponse;
import com.order.infrastructure.config.properties.DashboardReportProperties;
import com.order.infrastructure.repository.report.ReportRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class AdminServiceImpl implements AdminService {

    private final ReportRepository reportRepository;
    private final DashboardReportProperties dashboardReportProperties;

    @Override
    public Mono<AdminDashboardResponse> getDashboard() {
        log.info("Generating admin dashboard with parallel operations");

        int topProductsLimit =
                Math.clamp(
                        dashboardReportProperties.getTopProductsLimit(),
                        1,
                        50
                );

        Mono<OrderSummaryReportResponse> ordersMono =
                reportRepository.getOrderSummary();

        Mono<PaymentReportResponse> paymentsMono =
                reportRepository.getPaymentReport();

        Mono<RevenueReportResponse> revenueMono =
                reportRepository.getRevenueReport();

        Mono<InventoryReportResponse> inventoryMono =
                reportRepository.getInventoryReport();

        Mono<List<TopProductReportResponse>> topProductsMono =
                reportRepository.getTopProducts(topProductsLimit)
                        .collectList();

        Mono<OutboxSummaryResponse> outboxMono =
                reportRepository.getOutboxSummary();

        return Mono.zip(
                        ordersMono,
                        paymentsMono,
                        revenueMono,
                        inventoryMono,
                        topProductsMono,
                        outboxMono
                )
                .map(tuple -> new AdminDashboardResponse(
                        tuple.getT1(),
                        tuple.getT2(),
                        tuple.getT3(),
                        tuple.getT4(),
                        tuple.getT5(),
                        tuple.getT6()
                ));
    }
}