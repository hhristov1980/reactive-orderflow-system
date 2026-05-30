package com.order.infrastructure.messaging.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.order.application.service.PaymentService;
import com.order.domain.dto.response.PaymentResponse;
import com.order.domain.enums.PaymentStatus;
import com.order.domain.event.OrderConfirmedEvent;
import com.order.exception.PaymentAlreadyExistsException;
import com.order.infrastructure.config.properties.OrderKafkaProperties;
import com.order.infrastructure.observability.KafkaEventMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PaymentOrderConfirmedConsumerTest {

    private final ObjectMapper objectMapper =
            new ObjectMapper().registerModule(new JavaTimeModule());

    private final FakePaymentService paymentService =
            new FakePaymentService();

    private final OrderKafkaProperties kafkaProperties =
            kafkaProperties();

    private final SimpleMeterRegistry meterRegistry =
            new SimpleMeterRegistry();

    private final PaymentOrderConfirmedConsumer consumer =
            new PaymentOrderConfirmedConsumer(
                    objectMapper,
                    paymentService,
                    kafkaProperties,
                    new KafkaEventMetrics(meterRegistry)
            );

    @Test
    void consumesOrderConfirmedAfterPaymentIsCreated() throws Exception {
        OrderConfirmedEvent event = orderConfirmedEvent();

        paymentService.nextCreateResult = Mono.just(paymentResponse());

        assertDoesNotThrow(() ->
                consumer.consumeOrderConfirmed(toJson(event))
        );

        assertEquals(event, paymentService.receivedEvent);
        assertCounter("success", "none", 1.0);
    }

    @Test
    void treatsDuplicateOrderConfirmedAsProcessed() throws Exception {
        OrderConfirmedEvent event = orderConfirmedEvent();

        paymentService.nextCreateResult =
                Mono.error(new PaymentAlreadyExistsException(event.orderId()));

        assertDoesNotThrow(() ->
                consumer.consumeOrderConfirmed(toJson(event))
        );

        assertEquals(event, paymentService.receivedEvent);
        assertCounter("duplicate", "none", 1.0);
    }

    @Test
    void propagatesUnexpectedFailureForKafkaRetry() throws Exception {
        OrderConfirmedEvent event = orderConfirmedEvent();

        paymentService.nextCreateResult =
                Mono.error(new IllegalStateException("database unavailable"));

        assertThrows(
                IllegalStateException.class,
                () -> consumer.consumeOrderConfirmed(toJson(event))
        );

        assertEquals(event, paymentService.receivedEvent);
        assertCounter("failure", "IllegalStateException", 1.0);
    }

    private String toJson(Object value) throws Exception {
        return objectMapper.writeValueAsString(value);
    }

    private OrderConfirmedEvent orderConfirmedEvent() {
        return new OrderConfirmedEvent(
                31L,
                4L,
                new BigDecimal("24.90"),
                OffsetDateTime.parse("2026-05-30T16:52:21.149793Z")
        );
    }

    private PaymentResponse paymentResponse() {
        OffsetDateTime now = OffsetDateTime.parse("2026-05-30T16:52:26Z");

        return new PaymentResponse(
                17L,
                31L,
                PaymentStatus.PENDING,
                new BigDecimal("24.90"),
                "MOCK_PAYMENT_PROVIDER",
                null,
                null,
                now,
                now,
                null,
                null,
                null
        );
    }

    private OrderKafkaProperties kafkaProperties() {
        OrderKafkaProperties properties = new OrderKafkaProperties();

        properties.getTopics().setOrderConfirmed("order.confirmed");

        return properties;
    }

    private void assertCounter(
            String outcome,
            String exception,
            double expectedCount
    ) {
        double count = meterRegistry.get("orderflow.kafka.consumer.events")
                .tag("topic", "order.confirmed")
                .tag("outcome", outcome)
                .tag("exception", exception)
                .counter()
                .count();

        assertEquals(expectedCount, count);
    }

    private static final class FakePaymentService implements PaymentService {

        private OrderConfirmedEvent receivedEvent;

        private Mono<PaymentResponse> nextCreateResult =
                Mono.error(new AssertionError("createFromOrderConfirmed not configured"));

        @Override
        public Mono<PaymentResponse> createFromOrderConfirmed(OrderConfirmedEvent event) {
            receivedEvent = event;
            return nextCreateResult;
        }

        @Override
        public Mono<PaymentResponse> getById(Long id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Mono<PaymentResponse> getByOrderId(Long orderId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Mono<PaymentResponse> complete(Long id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Mono<PaymentResponse> fail(Long id, String reason) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Mono<Integer> expireOverduePayments(OffsetDateTime cutoff) {
            throw new UnsupportedOperationException();
        }
    }
}
