package com.order.domain.dto.response.admin;

import com.order.domain.dto.response.report.InventoryReportResponse;
import com.order.domain.dto.response.report.OrderSummaryReportResponse;
import com.order.domain.dto.response.report.PaymentReportResponse;
import com.order.domain.dto.response.report.RevenueReportResponse;
import com.order.domain.dto.response.report.TopProductReportResponse;

import java.util.List;

public record AdminDashboardResponse(

        OrderSummaryReportResponse orders,
        PaymentReportResponse payments,
        RevenueReportResponse revenue,
        InventoryReportResponse inventory,
        List<TopProductReportResponse> topProducts,
        OutboxSummaryResponse outbox
) {
}