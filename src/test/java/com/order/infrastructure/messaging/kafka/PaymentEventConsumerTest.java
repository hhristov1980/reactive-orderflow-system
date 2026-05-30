package com.order.infrastructure.messaging.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.order.application.service.OrderService;
import com.order.application.service.ShipmentService;
import com.order.domain.dto.request.CreateOrderRequest;
import com.order.domain.dto.response.OrderResponse;
import com.order.domain.dto.response.PagedResponse;
import com.order.domain.dto.response.ShipmentResponse;
import com.order.domain.enums.OrderSortField;
import com.order.domain.enums.ShipmentStatus;
import com.order.domain.enums.SortDirection;
import com.order.domain.event.PaymentCompletedEvent;
import com.order.exception.ShipmentAlreadyExistsException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PaymentEventConsumerTest {

    private final ObjectMapper objectMapper =
            new ObjectMapper().registerModule(new JavaTimeModule());

    private final FakeShipmentService shipmentService =
            new FakeShipmentService();

    private final OrderService orderService =
            new NoopOrderService();

    private final PaymentEventConsumer consumer =
            new PaymentEventConsumer(
                    objectMapper,
                    shipmentService,
                    orderService
            );

    @Test
    void consumesPaymentCompletedAfterShipmentIsCreated() throws Exception {
        PaymentCompletedEvent event = paymentCompletedEvent();

        shipmentService.nextCreateResult = Mono.just(shipmentResponse());

        assertDoesNotThrow(() ->
                consumer.consumePaymentCompleted(toJson(event))
        );

        assertEquals(event, shipmentService.receivedEvent);
    }

    @Test
    void treatsDuplicatePaymentCompletedAsProcessed() throws Exception {
        PaymentCompletedEvent event = paymentCompletedEvent();

        shipmentService.nextCreateResult =
                Mono.error(new ShipmentAlreadyExistsException(event.orderId()));

        assertDoesNotThrow(() ->
                consumer.consumePaymentCompleted(toJson(event))
        );

        assertEquals(event, shipmentService.receivedEvent);
    }

    @Test
    void propagatesUnexpectedShipmentFailureForKafkaRetry() throws Exception {
        PaymentCompletedEvent event = paymentCompletedEvent();

        shipmentService.nextCreateResult =
                Mono.error(new IllegalStateException("database unavailable"));

        assertThrows(
                IllegalStateException.class,
                () -> consumer.consumePaymentCompleted(toJson(event))
        );

        assertEquals(event, shipmentService.receivedEvent);
    }

    private String toJson(Object value) throws Exception {
        return objectMapper.writeValueAsString(value);
    }

    private PaymentCompletedEvent paymentCompletedEvent() {
        return new PaymentCompletedEvent(
                17L,
                31L,
                new BigDecimal("24.90"),
                "txn-31",
                OffsetDateTime.parse("2026-05-30T16:53:00Z")
        );
    }

    private ShipmentResponse shipmentResponse() {
        OffsetDateTime now = OffsetDateTime.parse("2026-05-30T16:53:05Z");

        return new ShipmentResponse(
                12L,
                31L,
                ShipmentStatus.CREATED,
                "TRACK-31",
                "DHL",
                now,
                now,
                null,
                null
        );
    }

    private static final class FakeShipmentService implements ShipmentService {

        private PaymentCompletedEvent receivedEvent;

        private Mono<ShipmentResponse> nextCreateResult =
                Mono.error(new AssertionError("createFromPaymentCompleted not configured"));

        @Override
        public Mono<ShipmentResponse> createFromPaymentCompleted(PaymentCompletedEvent event) {
            receivedEvent = event;
            return nextCreateResult;
        }

        @Override
        public Mono<ShipmentResponse> getById(Long id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Mono<ShipmentResponse> getByOrderId(Long orderId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Mono<ShipmentResponse> markAsShipped(Long id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Mono<ShipmentResponse> markAsDelivered(Long id) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class NoopOrderService implements OrderService {

        @Override
        public Mono<OrderResponse> create(CreateOrderRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Mono<PagedResponse<OrderResponse>> getAll(
                int page,
                int size,
                OrderSortField sortBy,
                SortDirection direction
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Mono<OrderResponse> getById(Long id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Mono<OrderResponse> cancel(Long id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Mono<OrderResponse> confirm(Long id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Mono<Void> confirmFromInventory(Long orderId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Mono<Void> failFromInventory(Long orderId, String reason) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Mono<Void> cancelFromPaymentFailure(Long orderId, String reason) {
            throw new UnsupportedOperationException();
        }
    }
}
