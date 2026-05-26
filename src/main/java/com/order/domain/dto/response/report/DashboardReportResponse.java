package com.order.domain.dto.response.report;

import java.util.List;

public record DashboardReportResponse(

        OrderSummaryReportResponse orders,
        RevenueReportResponse revenue,
        InventoryReportResponse inventory,
        PaymentReportResponse payments,
        List<TopProductReportResponse> topProducts
) {
}