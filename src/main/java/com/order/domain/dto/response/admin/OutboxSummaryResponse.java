package com.order.domain.dto.response.admin;

public record OutboxSummaryResponse(

        Long totalEvents,
        Long pendingEvents,
        Long publishedEvents,
        Long failedEvents
) {
}