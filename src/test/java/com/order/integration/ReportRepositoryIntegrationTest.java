package com.order.integration;

import com.order.domain.dto.response.admin.OutboxSummaryResponse;
import com.order.domain.dto.response.report.InventoryReportResponse;
import com.order.domain.dto.response.report.OrderSummaryReportResponse;
import com.order.domain.dto.response.report.PaymentReportResponse;
import com.order.domain.dto.response.report.RevenueReportResponse;
import com.order.domain.enums.OutboxStatus;
import com.order.infrastructure.repository.report.ReportRepository;
import com.order.infrastructure.repository.report.ReportRepositoryImpl;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.r2dbc.test.autoconfigure.DataR2dbcTest;
import org.springframework.context.annotation.Import;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

@DataR2dbcTest
@Import(ReportRepositoryImpl.class)
class ReportRepositoryIntegrationTest extends AbstractPostgresTestcontainersTest {

    @Autowired
    private ReportRepository reportRepository;

    @Autowired
    private DatabaseClient databaseClient;

    @Test
    void shouldReturnOrderSummaryReport() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(insertOrder("CREATED", new BigDecimal("10.00")))
                                .then(insertOrder("CONFIRMED", new BigDecimal("20.00")))
                                .then(insertOrder("CONFIRMED", new BigDecimal("30.00")))
                                .then(insertOrder("CANCELLED", new BigDecimal("40.00")))
                                .then(insertOrder("FAILED", new BigDecimal("50.00")))
                                .then(reportRepository.getOrderSummary())
                )
                .expectNextMatches(this::matchesOrderSummary)
                .verifyComplete();
    }

    @Test
    void shouldReturnRevenueReport() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(insertPayment(1L, "COMPLETED", new BigDecimal("100.00")))
                                .then(insertPayment(2L, "COMPLETED", new BigDecimal("50.50")))
                                .then(insertPayment(3L, "FAILED", new BigDecimal("20.00")))
                                .then(insertPayment(4L, "EXPIRED", new BigDecimal("30.00")))
                                .then(insertPayment(5L, "PENDING", new BigDecimal("40.00")))
                                .then(reportRepository.getRevenueReport())
                )
                .expectNextMatches(this::matchesRevenueReport)
                .verifyComplete();
    }

    @Test
    void shouldReturnInventoryReport() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(insertInventory(1L, 10, 2))
                                .then(insertInventory(2L, 5, 1))
                                .then(insertInventory(3L, 1, 0))
                                .then(insertInventory(4L, 0, 3))
                                .then(reportRepository.getInventoryReport())
                )
                .expectNextMatches(this::matchesInventoryReport)
                .verifyComplete();
    }

    @Test
    void shouldReturnPaymentReport() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(insertPayment(1L, "PENDING", new BigDecimal("10.00")))
                                .then(insertPayment(2L, "PENDING", new BigDecimal("20.00")))
                                .then(insertPayment(3L, "COMPLETED", new BigDecimal("30.00")))
                                .then(insertPayment(4L, "FAILED", new BigDecimal("40.00")))
                                .then(insertPayment(5L, "EXPIRED", new BigDecimal("50.00")))
                                .then(reportRepository.getPaymentReport())
                )
                .expectNextMatches(this::matchesPaymentReport)
                .verifyComplete();
    }

    @Test
    void shouldReturnOutboxSummary() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(insertOutboxEvent(1L, OutboxStatus.PENDING))
                                .then(insertOutboxEvent(2L, OutboxStatus.PENDING))
                                .then(insertOutboxEvent(3L, OutboxStatus.PUBLISHED))
                                .then(insertOutboxEvent(4L, OutboxStatus.FAILED))
                                .then(reportRepository.getOutboxSummary())
                )
                .expectNextMatches(this::matchesOutboxSummary)
                .verifyComplete();
    }

    private boolean matchesOrderSummary(OrderSummaryReportResponse response) {
        return response.totalOrders() == 5
                && response.createdOrders() == 1
                && response.confirmedOrders() == 2
                && response.cancelledOrders() == 1
                && response.failedOrders() == 1;
    }

    private boolean matchesRevenueReport(RevenueReportResponse response) {
        return response.totalRevenue().compareTo(new BigDecimal("150.50")) == 0
                && response.completedPayments() == 2
                && response.failedPayments() == 1
                && response.expiredPayments() == 1
                && response.pendingPayments() == 1;
    }

    private boolean matchesInventoryReport(InventoryReportResponse response) {
        return response.totalInventoryItems() == 4
                && response.lowStockItems() == 2
                && response.outOfStockItems() == 1
                && response.totalAvailableQuantity() == 16
                && response.totalReservedQuantity() == 6;
    }

    private boolean matchesPaymentReport(PaymentReportResponse response) {
        return response.totalPayments() == 5
                && response.pendingPayments() == 2
                && response.completedPayments() == 1
                && response.failedPayments() == 1
                && response.expiredPayments() == 1;
    }

    private boolean matchesOutboxSummary(OutboxSummaryResponse response) {
        return response.totalEvents() == 4
                && response.pendingEvents() == 2
                && response.publishedEvents() == 1
                && response.failedEvents() == 1;
    }

    private Mono<Void> cleanDatabase() {
        return databaseClient.sql("DELETE FROM order_items").fetch().rowsUpdated()
                .then(databaseClient.sql("DELETE FROM payments").fetch().rowsUpdated())
                .then(databaseClient.sql("DELETE FROM shipments").fetch().rowsUpdated())
                .then(databaseClient.sql("DELETE FROM outbox_events").fetch().rowsUpdated())
                .then(databaseClient.sql("DELETE FROM inventory").fetch().rowsUpdated())
                .then(databaseClient.sql("DELETE FROM orders").fetch().rowsUpdated())
                .then(databaseClient.sql("DELETE FROM products").fetch().rowsUpdated())
                .then(databaseClient.sql("DELETE FROM users").fetch().rowsUpdated())
                .then();
    }

    private Mono<Void> insertOrder(String status, BigDecimal totalAmount) {
        return databaseClient.sql("""
                        INSERT INTO orders (user_id, status, total_amount, created_at, updated_at)
                        VALUES (:userId, :status, :totalAmount, :createdAt, :updatedAt)
                        """)
                .bind("userId", "report-user-" + System.nanoTime())
                .bind("status", status)
                .bind("totalAmount", totalAmount)
                .bind("createdAt", OffsetDateTime.now())
                .bind("updatedAt", OffsetDateTime.now())
                .fetch()
                .rowsUpdated()
                .then();
    }

    private Mono<Void> insertPayment(Long orderId, String status, BigDecimal amount) {
        OffsetDateTime now = OffsetDateTime.now();

        String transactionId = status.equals("COMPLETED") ? "tx-" + orderId : null;
        String failureReason = status.equals("FAILED") ? "Test failure" : null;
        OffsetDateTime paidAt = status.equals("COMPLETED") ? now : null;
        OffsetDateTime failedAt = status.equals("FAILED") ? now : null;
        OffsetDateTime expiredAt = status.equals("EXPIRED") ? now : null;

        DatabaseClient.GenericExecuteSpec spec = databaseClient.sql("""
                    INSERT INTO payments (
                        order_id,
                        status,
                        amount,
                        provider,
                        transaction_id,
                        failure_reason,
                        created_at,
                        updated_at,
                        paid_at,
                        failed_at,
                        expired_at
                    )
                    VALUES (
                        :orderId,
                        :status,
                        :amount,
                        :provider,
                        :transactionId,
                        :failureReason,
                        :createdAt,
                        :updatedAt,
                        :paidAt,
                        :failedAt,
                        :expiredAt
                    )
                    """)
                .bind("orderId", orderId)
                .bind("status", status)
                .bind("amount", amount)
                .bind("provider", "TEST_PROVIDER")
                .bind("createdAt", now)
                .bind("updatedAt", now);

        spec = bindNullableString(spec, "transactionId", transactionId);
        spec = bindNullableString(spec, "failureReason", failureReason);
        spec = bindNullableOffsetDateTime(spec, "paidAt", paidAt);
        spec = bindNullableOffsetDateTime(spec, "failedAt", failedAt);
        spec = bindNullableOffsetDateTime(spec, "expiredAt", expiredAt);

        return spec.fetch()
                .rowsUpdated()
                .then();
    }

    private Mono<Void> insertInventory(Long productId, int availableQuantity, int reservedQuantity) {
        OffsetDateTime now = OffsetDateTime.now();

        return databaseClient.sql("""
                        INSERT INTO inventory (
                            product_id,
                            available_quantity,
                            reserved_quantity,
                            created_at,
                            updated_at
                        )
                        VALUES (
                            :productId,
                            :availableQuantity,
                            :reservedQuantity,
                            :createdAt,
                            :updatedAt
                        )
                        """)
                .bind("productId", productId)
                .bind("availableQuantity", availableQuantity)
                .bind("reservedQuantity", reservedQuantity)
                .bind("createdAt", now)
                .bind("updatedAt", now)
                .fetch()
                .rowsUpdated()
                .then();
    }

    private Mono<Void> insertOutboxEvent(Long aggregateId, OutboxStatus status) {
        OffsetDateTime now = OffsetDateTime.now();

        String lastError = status == OutboxStatus.FAILED ? "Test failure" : null;
        OffsetDateTime publishedAt = status == OutboxStatus.PUBLISHED ? now : null;

        DatabaseClient.GenericExecuteSpec spec = databaseClient.sql("""
                    INSERT INTO outbox_events (
                        aggregate_type,
                        aggregate_id,
                        event_type,
                        topic,
                        event_key,
                        payload,
                        status,
                        retry_count,
                        last_error,
                        created_at,
                        updated_at,
                        published_at
                    )
                    VALUES (
                        :aggregateType,
                        :aggregateId,
                        :eventType,
                        :topic,
                        :eventKey,
                        :payload,
                        :status,
                        :retryCount,
                        :lastError,
                        :createdAt,
                        :updatedAt,
                        :publishedAt
                    )
                    """)
                .bind("aggregateType", "ORDER")
                .bind("aggregateId", aggregateId)
                .bind("eventType", "ORDER_CREATED")
                .bind("topic", "order.created")
                .bind("eventKey", "order-" + aggregateId)
                .bind("payload", "{\"eventType\":\"ORDER_CREATED\"}")
                .bind("status", status.name())
                .bind("retryCount", status == OutboxStatus.FAILED ? 1 : 0)
                .bind("createdAt", now)
                .bind("updatedAt", now);

        spec = bindNullableString(spec, "lastError", lastError);
        spec = bindNullableOffsetDateTime(spec, "publishedAt", publishedAt);

        return spec.fetch()
                .rowsUpdated()
                .then();
    }

    @Test
    void shouldReturnTopProductsOrderedByQuantityAndRevenue() {
        StepVerifier.create(
                        cleanDatabase()
                                .then(insertProduct(1L, "Coffee A", new BigDecimal("10.00")))
                                .then(insertProduct(2L, "Coffee B", new BigDecimal("20.00")))
                                .then(insertProduct(3L, "Coffee C", new BigDecimal("30.00")))
                                .then(insertOrderWithId(101L, "CONFIRMED", new BigDecimal("100.00")))
                                .then(insertOrderWithId(102L, "CONFIRMED", new BigDecimal("100.00")))
                                .then(insertOrderWithId(103L, "CONFIRMED", new BigDecimal("100.00")))
                                .then(insertOrderItem(101L, 1L, 5, new BigDecimal("10.00")))
                                .then(insertOrderItem(102L, 2L, 3, new BigDecimal("20.00")))
                                .then(insertOrderItem(103L, 3L, 10, new BigDecimal("30.00")))
                                .then(insertPayment(101L, "COMPLETED", new BigDecimal("50.00")))
                                .then(insertPayment(102L, "COMPLETED", new BigDecimal("60.00")))
                                .then(insertPayment(103L, "PENDING", new BigDecimal("300.00")))
                                .thenMany(reportRepository.getTopProducts(2))
                                .collectList()
                )
                .expectNextMatches(products ->
                        products.size() == 2
                                && products.get(0).productId().equals(1L)
                                && products.get(0).productName().equals("Coffee A")
                                && products.get(0).quantitySold() == 5
                                && products.get(0).revenue().compareTo(new BigDecimal("50.00")) == 0
                                && products.get(1).productId().equals(2L)
                                && products.get(1).productName().equals("Coffee B")
                                && products.get(1).quantitySold() == 3
                                && products.get(1).revenue().compareTo(new BigDecimal("60.00")) == 0
                )
                .verifyComplete();
    }

    private static DatabaseClient.GenericExecuteSpec bindNullableString(
            DatabaseClient.GenericExecuteSpec spec,
            String name,
            String value
    ) {
        return value == null
                ? spec.bindNull(name, String.class)
                : spec.bind(name, value);
    }

    private static DatabaseClient.GenericExecuteSpec bindNullableOffsetDateTime(
            DatabaseClient.GenericExecuteSpec spec,
            String name,
            OffsetDateTime value
    ) {
        return value == null
                ? spec.bindNull(name, OffsetDateTime.class)
                : spec.bind(name, value);
    }

    private Mono<Void> insertProduct(Long id, String name, BigDecimal price) {
        OffsetDateTime now = OffsetDateTime.now();

        return databaseClient.sql("""
                    INSERT INTO products (
                        id,
                        name,
                        price,
                        stock,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        :id,
                        :name,
                        :price,
                        :stock,
                        :createdAt,
                        :updatedAt
                    )
                    """)
                .bind("id", id)
                .bind("name", name)
                .bind("price", price)
                .bind("stock", 100)
                .bind("createdAt", now)
                .bind("updatedAt", now)
                .fetch()
                .rowsUpdated()
                .then();
    }

    private Mono<Void> insertOrderWithId(Long id, String status, BigDecimal totalAmount) {
        OffsetDateTime now = OffsetDateTime.now();

        return databaseClient.sql("""
                    INSERT INTO orders (
                        id,
                        user_id,
                        status,
                        total_amount,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        :id,
                        :userId,
                        :status,
                        :totalAmount,
                        :createdAt,
                        :updatedAt
                    )
                    """)
                .bind("id", id)
                .bind("userId", "report-user-" + id)
                .bind("status", status)
                .bind("totalAmount", totalAmount)
                .bind("createdAt", now)
                .bind("updatedAt", now)
                .fetch()
                .rowsUpdated()
                .then();
    }

    private Mono<Void> insertOrderItem(
            Long orderId,
            Long productId,
            int quantity,
            BigDecimal price
    ) {
        return databaseClient.sql("""
                    INSERT INTO order_items (
                        order_id,
                        product_id,
                        quantity,
                        price
                    )
                    VALUES (
                        :orderId,
                        :productId,
                        :quantity,
                        :price
                    )
                    """)
                .bind("orderId", orderId)
                .bind("productId", productId)
                .bind("quantity", quantity)
                .bind("price", price)
                .fetch()
                .rowsUpdated()
                .then();
    }
}