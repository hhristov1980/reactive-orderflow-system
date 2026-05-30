package com.order.infrastructure.messaging.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.order.application.service.InventoryService;
import com.order.application.service.OutboxService;
import com.order.domain.dto.response.InventoryResponse;
import com.order.domain.dto.response.PagedResponse;
import com.order.domain.enums.InventorySortField;
import com.order.domain.enums.SortDirection;
import com.order.domain.event.InventoryFailedEvent;
import com.order.domain.event.InventoryReleasedEvent;
import com.order.domain.event.InventoryReservedEvent;
import com.order.domain.event.OrderCancelledEvent;
import com.order.domain.event.OrderCreatedEvent;
import com.order.domain.event.OrderItemEvent;
import com.order.infrastructure.config.properties.OrderKafkaProperties;
import com.order.infrastructure.observability.KafkaEventMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class InventoryOrderEventConsumerTest {

    private final ObjectMapper objectMapper =
            new ObjectMapper().registerModule(new JavaTimeModule());

    private final FakeInventoryService inventoryService =
            new FakeInventoryService();

    private final CapturingOutboxService outboxService =
            new CapturingOutboxService();

    private final OrderKafkaProperties kafkaProperties =
            kafkaProperties();

    private final SimpleMeterRegistry meterRegistry =
            new SimpleMeterRegistry();

    private final InventoryOrderEventConsumer consumer =
            new InventoryOrderEventConsumer(
                    objectMapper,
                    inventoryService,
                    outboxService,
                    kafkaProperties,
                    new KafkaEventMetrics(meterRegistry)
            );

    @Test
    void convertsInventoryReservationFailureToInventoryFailedOutboxEvent() throws Exception {
        OrderCreatedEvent event = orderCreatedEvent();

        inventoryService.nextReserveResult =
                Mono.error(new IllegalStateException("insufficient inventory"));

        assertDoesNotThrow(() ->
                consumer.consumeOrderCreated(toJson(event))
        );

        assertEquals(event, inventoryService.receivedEvent);
        assertEquals("INVENTORY", outboxService.aggregateType);
        assertEquals(event.orderId(), outboxService.aggregateId);
        assertEquals("INVENTORY_FAILED", outboxService.eventType);
        assertEquals("inventory.failed", outboxService.topic);
        assertEquals(event.orderId().toString(), outboxService.eventKey);

        InventoryFailedEvent failedEvent =
                assertInstanceOf(
                        InventoryFailedEvent.class,
                        outboxService.payload
                );

        assertEquals(event.orderId(), failedEvent.orderId());
        assertEquals("insufficient inventory", failedEvent.reason());
        assertInventoryFailureCounter(1.0);
        assertConsumerCounter("success", "none", 1.0);
    }

    private String toJson(Object value) throws Exception {
        return objectMapper.writeValueAsString(value);
    }

    private OrderCreatedEvent orderCreatedEvent() {
        return new OrderCreatedEvent(
                31L,
                4L,
                new BigDecimal("24.90"),
                List.of(
                        new OrderItemEvent(
                                1L,
                                1,
                                new BigDecimal("24.90")
                        )
                ),
                OffsetDateTime.parse("2026-05-30T16:52:14Z")
        );
    }

    private OrderKafkaProperties kafkaProperties() {
        OrderKafkaProperties properties =
                new OrderKafkaProperties();

        properties.getTopics().setInventoryFailed("inventory.failed");
        properties.getTopics().setOrderCreated("order.created");

        return properties;
    }

    private void assertInventoryFailureCounter(double expectedCount) {
        double count = meterRegistry.get("orderflow.inventory.reservation.failures")
                .tag("exception", "IllegalStateException")
                .counter()
                .count();

        assertEquals(expectedCount, count);
    }

    private void assertConsumerCounter(
            String outcome,
            String exception,
            double expectedCount
    ) {
        double count = meterRegistry.get("orderflow.kafka.consumer.events")
                .tag("topic", "order.created")
                .tag("outcome", outcome)
                .tag("exception", exception)
                .counter()
                .count();

        assertEquals(expectedCount, count);
    }

    private static final class FakeInventoryService implements InventoryService {

        private OrderCreatedEvent receivedEvent;

        private Mono<InventoryReservedEvent> nextReserveResult =
                Mono.error(new AssertionError("reserve not configured"));

        @Override
        public Mono<InventoryResponse> getByProductId(Long productId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Mono<PagedResponse<InventoryResponse>> getAll(
                int page,
                int size,
                InventorySortField sortBy,
                SortDirection direction
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Mono<InventoryReservedEvent> reserve(OrderCreatedEvent event) {
            receivedEvent = event;
            return nextReserveResult;
        }

        @Override
        public Mono<InventoryReleasedEvent> release(OrderCancelledEvent event) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class CapturingOutboxService implements OutboxService {

        private String aggregateType;

        private Long aggregateId;

        private String eventType;

        private String topic;

        private String eventKey;

        private Object payload;

        @Override
        public Mono<Void> saveEvent(
                String aggregateType,
                Long aggregateId,
                String eventType,
                String topic,
                String eventKey,
                Object payload
        ) {
            this.aggregateType = aggregateType;
            this.aggregateId = aggregateId;
            this.eventType = eventType;
            this.topic = topic;
            this.eventKey = eventKey;
            this.payload = payload;
            return Mono.empty();
        }

        @Override
        public Mono<Integer> publishPublishableEvents(int maxRetries) {
            throw new UnsupportedOperationException();
        }
    }
}
