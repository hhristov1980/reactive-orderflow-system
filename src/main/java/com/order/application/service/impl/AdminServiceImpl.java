package com.order.application.service.impl;

import com.order.application.service.AdminService;
import com.order.domain.dto.response.PagedResponse;
import com.order.domain.dto.response.admin.AdminDashboardResponse;
import com.order.domain.dto.response.admin.OutboxEventResponse;
import com.order.domain.dto.response.admin.OutboxSummaryResponse;
import com.order.domain.dto.response.report.InventoryReportResponse;
import com.order.domain.dto.response.report.OrderSummaryReportResponse;
import com.order.domain.dto.response.report.PaymentReportResponse;
import com.order.domain.dto.response.report.RevenueReportResponse;
import com.order.domain.dto.response.report.TopProductReportResponse;
import com.order.domain.entity.OutboxEvent;
import com.order.domain.enums.OutboxStatus;
import com.order.exception.OutboxEventCannotBeRetriedException;
import com.order.exception.OutboxEventNotFoundException;
import com.order.infrastructure.config.properties.DashboardReportProperties;
import com.order.infrastructure.repository.OutboxEventRepository;
import com.order.infrastructure.repository.report.ReportRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class AdminServiceImpl implements AdminService {

    private final ReportRepository reportRepository;
    private final OutboxEventRepository outboxEventRepository;
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

    @Override
    public Mono<PagedResponse<OutboxEventResponse>> getOutboxEvents(
            int page,
            int size,
            OutboxStatus status
    ) {
        log.info(
                "Getting outbox events: page={}, size={}, status={}",
                page,
                size,
                status
        );

        int validatedPage = Math.max(page, 0);
        int validatedSize = Math.clamp(size, 1, 100);
        long offset = (long) validatedPage * validatedSize;

        Flux<OutboxEvent> eventsFlux =
                status == null
                        ? outboxEventRepository.findAllPaged(
                        validatedSize,
                        offset
                )
                        : outboxEventRepository.findByStatusPaged(
                        status,
                        validatedSize,
                        offset
                );

        Mono<Long> countMono =
                status == null
                        ? outboxEventRepository.countAll()
                        : outboxEventRepository.countByStatus(status);

        Mono<List<OutboxEventResponse>> eventsMono =
                eventsFlux
                        .map(this::toOutboxEventResponse)
                        .collectList();

        return Mono.zip(eventsMono, countMono)
                .map(tuple -> {
                    List<OutboxEventResponse> events = tuple.getT1();
                    long totalElements = tuple.getT2();

                    int totalPages =
                            (int) Math.ceil((double) totalElements / validatedSize);

                    boolean first = validatedPage == 0;
                    boolean last =
                            totalPages == 0 || validatedPage >= totalPages - 1;
                    boolean hasNext = validatedPage < totalPages - 1;
                    boolean hasPrevious = validatedPage > 0;

                    return new PagedResponse<>(
                            events,
                            validatedPage,
                            validatedSize,
                            totalElements,
                            totalPages,
                            first,
                            last,
                            hasNext,
                            hasPrevious
                    );
                });
    }

    @Override
    public Mono<OutboxEventResponse> getOutboxEventById(Long id) {
        log.info("Getting outbox event with id={}", id);

        return outboxEventRepository.findById(id)
                .switchIfEmpty(Mono.error(new OutboxEventNotFoundException(id)))
                .map(this::toOutboxEventResponse);
    }

    @Override
    public Mono<OutboxEventResponse> retryOutboxEvent(Long id) {
        log.info("Scheduling outbox event for retry. id={}", id);

        return outboxEventRepository.findById(id)
                .switchIfEmpty(Mono.error(new OutboxEventNotFoundException(id)))
                .flatMap(event -> {
                    if (event.getStatus() != OutboxStatus.FAILED) {
                        return Mono.error(
                                new OutboxEventCannotBeRetriedException(
                                        id,
                                        event.getStatus().name()
                                )
                        );
                    }

                    event.setStatus(OutboxStatus.PENDING);
                    event.setLastError(null);
                    event.setPublishedAt(null);
                    event.setUpdatedAt(OffsetDateTime.now());

                    return outboxEventRepository.save(event);
                })
                .map(this::toOutboxEventResponse);
    }

    private OutboxEventResponse toOutboxEventResponse(OutboxEvent event) {
        return new OutboxEventResponse(
                event.getId(),
                event.getAggregateType(),
                event.getAggregateId(),
                event.getEventType(),
                event.getTopic(),
                event.getEventKey(),
                event.getPayload(),
                event.getStatus(),
                event.getRetryCount(),
                event.getLastError(),
                event.getCreatedAt(),
                event.getUpdatedAt(),
                event.getPublishedAt()
        );
    }
}