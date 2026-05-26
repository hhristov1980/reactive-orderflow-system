package com.order.domain.dto.response.report;

public record OrderSummaryReportResponse(

        Long totalOrders,
        Long createdOrders,
        Long confirmedOrders,
        Long cancelledOrders,
        Long failedOrders
) {
}