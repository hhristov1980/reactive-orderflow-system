package com.order.infrastructure.repository.report;

import com.order.domain.dto.response.report.*;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;

@Repository
@RequiredArgsConstructor
public class ReportRepositoryImpl implements ReportRepository {

    private static final int LOW_STOCK_THRESHOLD = 5;

    private final DatabaseClient databaseClient;

    @Override
    public Mono<OrderSummaryReportResponse> getOrderSummary() {
        String sql = """
                SELECT
                    COUNT(*) AS total_orders,
                    COUNT(*) FILTER (WHERE status = 'CREATED') AS created_orders,
                    COUNT(*) FILTER (WHERE status = 'CONFIRMED') AS confirmed_orders,
                    COUNT(*) FILTER (WHERE status = 'CANCELLED') AS cancelled_orders,
                    COUNT(*) FILTER (WHERE status = 'FAILED') AS failed_orders
                FROM orders
                """;

        return databaseClient.sql(sql)
                .map((row, metadata) -> new OrderSummaryReportResponse(
                        getLong(row, "total_orders"),
                        getLong(row, "created_orders"),
                        getLong(row, "confirmed_orders"),
                        getLong(row, "cancelled_orders"),
                        getLong(row, "failed_orders")
                ))
                .one();
    }

    @Override
    public Mono<RevenueReportResponse> getRevenueReport() {
        String sql = """
                SELECT
                    COALESCE(SUM(amount) FILTER (WHERE status = 'COMPLETED'), 0) AS total_revenue,
                    COUNT(*) FILTER (WHERE status = 'COMPLETED') AS completed_payments,
                    COUNT(*) FILTER (WHERE status = 'FAILED') AS failed_payments,
                    COUNT(*) FILTER (WHERE status = 'EXPIRED') AS expired_payments,
                    COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_payments
                FROM payments
                """;

        return databaseClient.sql(sql)
                .map((row, metadata) -> new RevenueReportResponse(
                        getBigDecimal(row, "total_revenue"),
                        getLong(row, "completed_payments"),
                        getLong(row, "failed_payments"),
                        getLong(row, "expired_payments"),
                        getLong(row, "pending_payments")
                ))
                .one();
    }

    @Override
    public Mono<InventoryReportResponse> getInventoryReport() {
        String sql = """
                SELECT
                    COUNT(*) AS total_inventory_items,
                    COUNT(*) FILTER (WHERE available_quantity > 0 AND available_quantity <= :lowStockThreshold) AS low_stock_items,
                    COUNT(*) FILTER (WHERE available_quantity = 0) AS out_of_stock_items,
                    COALESCE(SUM(available_quantity), 0) AS total_available_quantity,
                    COALESCE(SUM(reserved_quantity), 0) AS total_reserved_quantity
                FROM inventory
                """;

        return databaseClient.sql(sql)
                .bind("lowStockThreshold", LOW_STOCK_THRESHOLD)
                .map((row, metadata) -> new InventoryReportResponse(
                        getLong(row, "total_inventory_items"),
                        getLong(row, "low_stock_items"),
                        getLong(row, "out_of_stock_items"),
                        getLong(row, "total_available_quantity"),
                        getLong(row, "total_reserved_quantity")
                ))
                .one();
    }

    @Override
    public Mono<PaymentReportResponse> getPaymentReport() {
        String sql = """
                SELECT
                    COUNT(*) AS total_payments,
                    COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_payments,
                    COUNT(*) FILTER (WHERE status = 'COMPLETED') AS completed_payments,
                    COUNT(*) FILTER (WHERE status = 'FAILED') AS failed_payments,
                    COUNT(*) FILTER (WHERE status = 'EXPIRED') AS expired_payments
                FROM payments
                """;

        return databaseClient.sql(sql)
                .map((row, metadata) -> new PaymentReportResponse(
                        getLong(row, "total_payments"),
                        getLong(row, "pending_payments"),
                        getLong(row, "completed_payments"),
                        getLong(row, "failed_payments"),
                        getLong(row, "expired_payments")
                ))
                .one();
    }

    @Override
    public Flux<TopProductReportResponse> getTopProducts(int limit) {
        String sql = """
            SELECT
                p.id AS product_id,
                p.name AS product_name,
                COALESCE(SUM(oi.quantity), 0) AS quantity_sold,
                COALESCE(SUM(oi.quantity * oi.price), 0) AS revenue
            FROM order_items oi
            JOIN products p ON p.id = oi.product_id
            JOIN orders o ON o.id = oi.order_id
            JOIN payments pay ON pay.order_id = o.id
            WHERE pay.status = 'COMPLETED'
            GROUP BY p.id, p.name
            ORDER BY quantity_sold DESC, revenue DESC
            LIMIT :limit
            """;

        return databaseClient.sql(sql)
                .bind("limit", limit)
                .map((row, metadata) -> new TopProductReportResponse(
                        getLong(row, "product_id"),
                        row.get("product_name", String.class),
                        getLong(row, "quantity_sold"),
                        getBigDecimal(row, "revenue")
                ))
                .all();
    }

    private Long getLong(io.r2dbc.spi.Row row, String column) {
        Number value = row.get(column, Number.class);
        return value == null ? 0L : value.longValue();
    }

    private BigDecimal getBigDecimal(io.r2dbc.spi.Row row, String column) {
        BigDecimal value = row.get(column, BigDecimal.class);
        return value == null ? BigDecimal.ZERO : value;
    }
}