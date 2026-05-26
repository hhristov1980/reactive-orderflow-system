package com.order.domain.dto.response.report;

public record InventoryReportResponse(

        Long totalInventoryItems,
        Long lowStockItems,
        Long outOfStockItems,
        Long totalAvailableQuantity,
        Long totalReservedQuantity
) {
}