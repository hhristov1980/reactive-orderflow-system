package com.order.application.service.impl;

import com.order.application.service.AdminService;
import com.order.domain.dto.response.PagedResponse;
import com.order.domain.dto.response.admin.AdminDashboardResponse;
import com.order.domain.dto.response.admin.AuditEventResponse;
import com.order.domain.dto.response.admin.OutboxEventResponse;
import com.order.domain.dto.response.admin.OutboxSummaryResponse;
import com.order.domain.dto.response.report.InventoryReportResponse;
import com.order.domain.dto.response.report.OrderSummaryReportResponse;
import com.order.domain.dto.response.report.PaymentReportResponse;
import com.order.domain.dto.response.report.RevenueReportResponse;
import com.order.domain.dto.response.report.TopProductReportResponse;
import com.order.domain.entity.AuditEvent;
import com.order.domain.entity.OutboxEvent;
import com.order.domain.enums.OutboxStatus;
import com.order.exception.AuditEventNotFoundException;
import com.order.exception.OutboxEventCannotBeRetriedException;
import com.order.exception.OutboxEventNotFoundException;
import com.order.infrastructure.config.properties.DashboardReportProperties;
import com.order.infrastructure.repository.AuditEventRepository;
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

    private static final String AGGREGATE_TYPE_ORDER = "ORDER";

    private final ReportRepository reportRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final AuditEventRepository auditEventRepository;
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
                .map(tuple -> toPagedResponse(
                        tuple.getT1(),
                        validatedPage,
                        validatedSize,
                        tuple.getT2()
                ));
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

    @Override
    public Mono<PagedResponse<AuditEventResponse>> getAuditEvents(
            int page,
            int size
    ) {
        log.info("Getting audit events: page={}, size={}", page, size);

        int validatedPage = Math.max(page, 0);
        int validatedSize = Math.clamp(size, 1, 100);
        long offset = (long) validatedPage * validatedSize;

        Mono<List<AuditEventResponse>> eventsMono =
                auditEventRepository.findAllPaged(validatedSize, offset)
                        .map(this::toAuditEventResponse)
                        .collectList();

        Mono<Long> countMono =
                auditEventRepository.countAll();

        return Mono.zip(eventsMono, countMono)
                .map(tuple -> toPagedResponse(
                        tuple.getT1(),
                        validatedPage,
                        validatedSize,
                        tuple.getT2()
                ));
    }

    @Override
    public Mono<AuditEventResponse> getAuditEventById(Long id) {
        log.info("Getting audit event with id={}", id);

        return auditEventRepository.findById(id)
                .switchIfEmpty(Mono.error(new AuditEventNotFoundException(id)))
                .map(this::toAuditEventResponse);
    }

    @Override
    public Mono<PagedResponse<AuditEventResponse>> getAuditEventsByOrderId(
            Long orderId,
            int page,
            int size
    ) {
        log.info(
                "Getting audit events for orderId={}, page={}, size={}",
                orderId,
                page,
                size
        );

        int validatedPage = Math.max(page, 0);
        int validatedSize = Math.clamp(size, 1, 100);
        long offset = (long) validatedPage * validatedSize;

        Mono<List<AuditEventResponse>> eventsMono =
                auditEventRepository.findByAggregatePaged(
                                AGGREGATE_TYPE_ORDER,
                                orderId,
                                validatedSize,
                                offset
                        )
                        .map(this::toAuditEventResponse)
                        .collectList();

        Mono<Long> countMono =
                auditEventRepository.countByAggregate(
                        AGGREGATE_TYPE_ORDER,
                        orderId
                );

        return Mono.zip(eventsMono, countMono)
                .map(tuple -> toPagedResponse(
                        tuple.getT1(),
                        validatedPage,
                        validatedSize,
                        tuple.getT2()
                ));
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

    private AuditEventResponse toAuditEventResponse(AuditEvent event) {
        return new AuditEventResponse(
                event.getId(),
                event.getEventType(),
                event.getAggregateType(),
                event.getAggregateId(),
                event.getPayload(),
                event.getCreatedAt()
        );
    }

    private <T> PagedResponse<T> toPagedResponse(
            List<T> content,
            int page,
            int size,
            long totalElements
    ) {
        int totalPages =
                (int) Math.ceil((double) totalElements / size);

        boolean first = page == 0;
        boolean last =
                totalPages == 0 || page >= totalPages - 1;
        boolean hasNext = page < totalPages - 1;
        boolean hasPrevious = page > 0;

        return new PagedResponse<>(
                content,
                page,
                size,
                totalElements,
                totalPages,
                first,
                last,
                hasNext,
                hasPrevious
        );
    }
}