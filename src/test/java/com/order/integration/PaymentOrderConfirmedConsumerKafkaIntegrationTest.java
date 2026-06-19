package com.order.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.order.domain.entity.OutboxEvent;
import com.order.domain.entity.Payment;
import com.order.domain.enums.OutboxStatus;
import com.order.domain.enums.PaymentStatus;
import com.order.domain.event.OrderConfirmedEvent;
import com.order.infrastructure.repository.OutboxEventRepository;
import com.order.infrastructure.repository.PaymentRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@TestPropertySource(properties = {
        "spring.docker.compose.enabled=false",

        "orderflow.scheduler.outbox.enabled=false",
        "orderflow.scheduler.unpaid-payments.enabled=false",

        "orderflow.kafka.topics.order-confirmed=order.confirmed.it",
        "orderflow.kafka.topics.payment-created=payment.created.it",

        "orderflow.kafka.consumer-groups.payment=payment-consumer-it"
})
class PaymentOrderConfirmedConsumerKafkaIntegrationTest
        extends AbstractPostgresTestcontainersTest {

    private static final KafkaContainer KAFKA =
            new KafkaContainer(
                    DockerImageName.parse("apache/kafka-native:3.8.0")
            );

    static {
        KAFKA.start();
    }

    @DynamicPropertySource
    static void registerKafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("orderflow.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Test
    void shouldConsumeOrderConfirmedAndCreatePendingPaymentWithOutboxEvent()
            throws Exception {
        Long orderId = Math.abs(UUID.randomUUID().getMostSignificantBits());
        Long userId = 1001L;

        OrderConfirmedEvent event = new OrderConfirmedEvent(
                orderId,
                userId,
                new BigDecimal("149.90"),
                OffsetDateTime.now()
        );

        String payload = objectMapper.writeValueAsString(event);

        cleanDatabase().block(Duration.ofSeconds(5));

        kafkaTemplate.send(
                        "order.confirmed.it",
                        orderId.toString(),
                        payload
                )
                .get();

        Payment payment = awaitPayment(orderId);

        assertThat(payment.getOrderId()).isEqualTo(orderId);
        assertThat(payment.getAmount()).isEqualByComparingTo(new BigDecimal("149.90"));
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.PENDING);
        assertThat(payment.getProvider()).isEqualTo("MOCK_PAYMENT_PROVIDER");
        assertThat(payment.getTransactionId()).isNull();
        assertThat(payment.getFailureReason()).isNull();
        assertThat(payment.getPaidAt()).isNull();
        assertThat(payment.getFailedAt()).isNull();
        assertThat(payment.getExpiredAt()).isNull();

        OutboxEvent outboxEvent = awaitPaymentCreatedOutboxEvent(payment.getId());

        assertThat(outboxEvent.getAggregateType()).isEqualTo("PAYMENT");
        assertThat(outboxEvent.getAggregateId()).isEqualTo(payment.getId());
        assertThat(outboxEvent.getEventType()).isEqualTo("PAYMENT_CREATED");
        assertThat(outboxEvent.getTopic()).isEqualTo("payment.created.it");
        assertThat(outboxEvent.getEventKey()).isEqualTo(orderId.toString());
        assertThat(outboxEvent.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(outboxEvent.getRetryCount()).isZero();
        assertThat(outboxEvent.getPayload()).contains("\"orderId\":" + orderId);
        assertThat(outboxEvent.getPayload()).contains("\"amount\":149.90");
    }

    private Mono<Void> cleanDatabase() {
        return outboxEventRepository.deleteAll()
                .then(paymentRepository.deleteAll());
    }

    private Payment awaitPayment(Long orderId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;

        while (System.currentTimeMillis() < deadline) {
            Optional<Payment> payment =
                    paymentRepository.findByOrderId(orderId)
                            .blockOptional(Duration.ofSeconds(1));

            if (payment.isPresent()) {
                return payment.get();
            }

            Thread.sleep(250);
        }

        throw new AssertionError(
                "Payment was not created for orderId=" + orderId
        );
    }

    private OutboxEvent awaitPaymentCreatedOutboxEvent(Long paymentId)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;

        while (System.currentTimeMillis() < deadline) {
            List<OutboxEvent> events =
                    outboxEventRepository.findAll()
                            .collectList()
                            .block(Duration.ofSeconds(1));

            if (events != null) {
                Optional<OutboxEvent> event =
                        events.stream()
                                .filter(outbox ->
                                        "PAYMENT".equals(outbox.getAggregateType())
                                                && paymentId.equals(outbox.getAggregateId())
                                                && "PAYMENT_CREATED".equals(outbox.getEventType())
                                )
                                .findFirst();

                if (event.isPresent()) {
                    return event.get();
                }
            }

            Thread.sleep(250);
        }

        throw new AssertionError(
                "PAYMENT_CREATED outbox event was not created for paymentId=" + paymentId
        );
    }
}