package com.order.application.service;

import com.order.domain.dto.response.PagedResponse;
import com.order.domain.dto.response.admin.AdminDashboardResponse;
import com.order.domain.dto.response.admin.AuditEventResponse;
import com.order.domain.dto.response.admin.OutboxEventResponse;
import com.order.domain.enums.OutboxStatus;
import reactor.core.publisher.Mono;

public interface AdminService {

    Mono<AdminDashboardResponse> getDashboard();

    Mono<PagedResponse<OutboxEventResponse>> getOutboxEvents(
            int page,
            int size,
            OutboxStatus status
    );

    Mono<OutboxEventResponse> getOutboxEventById(Long id);

    Mono<OutboxEventResponse> retryOutboxEvent(Long id);

    Mono<PagedResponse<AuditEventResponse>> getAuditEvents(
            int page,
            int size
    );

    Mono<AuditEventResponse> getAuditEventById(Long id);

    Mono<PagedResponse<AuditEventResponse>> getAuditEventsByOrderId(
            Long orderId,
            int page,
            int size
    );
}