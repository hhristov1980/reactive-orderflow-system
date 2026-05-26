package com.order.application.service.impl;

import com.order.application.service.ReportService;
import com.order.domain.dto.response.report.*;
import com.order.infrastructure.config.properties.DashboardReportProperties;
import com.order.infrastructure.repository.report.ReportRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class ReportServiceImpl implements ReportService {

    private final ReportRepository repository;
    private final DashboardReportProperties dashboardReportProperties;

    @Override
    public Mono<OrderSummaryReportResponse> getOrderSummary() {
        log.info("Generating order summary report");

        return repository.getOrderSummary();
    }

    @Override
    public Mono<RevenueReportResponse> getRevenueReport() {
        log.info("Generating revenue report");

        return repository.getRevenueReport();
    }

    @Override
    public Mono<InventoryReportResponse> getInventoryReport() {
        log.info("Generating inventory report");

        return repository.getInventoryReport();
    }

    @Override
    public Mono<PaymentReportResponse> getPaymentReport() {
        log.info("Generating payment report");

        return repository.getPaymentReport();
    }

    @Override
    public Mono<DashboardReportResponse> getDashboard() {
        log.info("Generating dashboard report with parallel operations");

        Mono<OrderSummaryReportResponse> ordersMono =
                repository.getOrderSummary();

        Mono<RevenueReportResponse> revenueMono =
                repository.getRevenueReport();

        Mono<InventoryReportResponse> inventoryMono =
                repository.getInventoryReport();

        Mono<PaymentReportResponse> paymentsMono =
                repository.getPaymentReport();
        Mono<List<TopProductReportResponse>> topProductsMono =
                repository.getTopProducts(dashboardReportProperties.getTopProductsLimit())
                        .collectList();

        return Mono.zip(
                        ordersMono,
                        revenueMono,
                        inventoryMono,
                        paymentsMono,
                        topProductsMono
                )
                .map(tuple -> new DashboardReportResponse(
                        tuple.getT1(),
                        tuple.getT2(),
                        tuple.getT3(),
                        tuple.getT4(),
                        tuple.getT5()
                ));
    }

    @Override
    public Flux<TopProductReportResponse> getTopProducts(int limit) {
        log.info("Generating top products report with limit={}", limit);

        int validatedLimit = Math.clamp(limit, 1, 50);

        return repository.getTopProducts(validatedLimit);
    }
}