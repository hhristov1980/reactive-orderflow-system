package com.order.domain.dto.response.report;

import java.math.BigDecimal;

public record RevenueReportResponse(

        BigDecimal totalRevenue,
        Long completedPayments,
        Long failedPayments,
        Long expiredPayments,
        Long pendingPayments
) {
}