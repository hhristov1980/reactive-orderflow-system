package com.order.domain.dto.response.report;

public record PaymentReportResponse(

        Long totalPayments,
        Long pendingPayments,
        Long completedPayments,
        Long failedPayments,
        Long expiredPayments
) {
}