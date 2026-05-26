package com.order.domain.dto.response.report;

import java.math.BigDecimal;

public record TopProductReportResponse(

        Long productId,
        String productName,
        Long quantitySold,
        BigDecimal revenue
) {
}