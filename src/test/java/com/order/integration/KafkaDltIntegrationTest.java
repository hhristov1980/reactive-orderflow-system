package com.order.integration;

import com.order.infrastructure.repository.OutboxEventRepository;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource(properties = {
        "spring.docker.compose.enabled=false",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.listener.missing-topics-fatal=false",

        "orderflow.scheduler.outbox.enabled=false",
        "orderflow.scheduler.unpaid-payments.enabled=false",

        "orderflow.kafka.topics.order-created=order.created.dlt.it",
        "orderflow.kafka.topics.order-confirmed=order.confirmed.dlt.it",
        "orderflow.kafka.topics.order-cancelled=order.cancelled.dlt.it",

        "orderflow.kafka.topics.inventory-reserved=inventory.reserved.dlt.it",
        "orderflow.kafka.topics.inventory-failed=inventory.failed.dlt.it",
        "orderflow.kafka.topics.inventory-released=inventory.released.dlt.it",

        "orderflow.kafka.topics.payment-created=payment.created.dlt.it",
        "orderflow.kafka.topics.payment-completed=payment.completed.dlt.it",
        "orderflow.kafka.topics.payment-failed=payment.failed.dlt.it",
        "orderflow.kafka.topics.payment-expired=payment.expired.dlt.it",

        "orderflow.kafka.topics.shipment-created=shipment.created.dlt.it",
        "orderflow.kafka.topics.shipment-shipped=shipment.shipped.dlt.it",
        "orderflow.kafka.topics.shipment-delivered=shipment.delivered.dlt.it"
})
class KafkaDltIntegrationTest extends AbstractPostgresTestcontainersTest {

    private static final String ORDER_CREATED_TOPIC = "order.created.dlt.it";
    private static final String ORDER_CREATED_DLT_TOPIC = ORDER_CREATED_TOPIC + ".DLT";
    private static final KafkaContainer KAFKA =
            new KafkaContainer("apache/kafka-native:3.8.0");
    private static final String TECHNICAL_FAILURE_TOPIC = "technical.failure.dlt.it";
    private static final String TECHNICAL_FAILURE_DLT_TOPIC = TECHNICAL_FAILURE_TOPIC + ".DLT";

    static {
        KAFKA.start();
    }

    @DynamicPropertySource
    static void registerKafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("orderflow.kafka.bootstrap-servers", KAFKA::getBootstrapServers);

        registry.add(
                "orderflow.kafka.consumer-groups.inventory",
                () -> "orderflow-inventory-dlt-it-" + UUID.randomUUID()
        );
        registry.add(
                "orderflow.kafka.consumer-groups.audit",
                () -> "orderflow-audit-dlt-it-" + UUID.randomUUID()
        );
        registry.add(
                "orderflow.kafka.consumer-groups.order",
                () -> "orderflow-order-dlt-it-" + UUID.randomUUID()
        );
        registry.add(
                "orderflow.kafka.consumer-groups.payment",
                () -> "orderflow-payment-dlt-it-" + UUID.randomUUID()
        );
        registry.add(
                "orderflow.kafka.consumer-groups.shipment",
                () -> "orderflow-shipment-dlt-it-" + UUID.randomUUID()
        );
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private RetryFailureListener retryFailureListener;

    @BeforeEach
    void setUp() throws InterruptedException {
        outboxEventRepository.deleteAll().block(Duration.ofSeconds(5));
        waitForKafkaListeners();
    }

    @Test
    void shouldSendInvalidOrderCreatedPayloadToDlt() throws Exception {
        String invalidPayload = "{ invalid-json-payload";

        kafkaTemplate.send(
                ORDER_CREATED_TOPIC,
                "invalid-order-created-key",
                invalidPayload
        ).get();

        kafkaTemplate.flush();

        ConsumerRecord<String, String> dltRecord =
                awaitDltRecord(ORDER_CREATED_DLT_TOPIC, invalidPayload);

        assertThat(dltRecord.topic()).isEqualTo(ORDER_CREATED_DLT_TOPIC);
        assertThat(dltRecord.key()).isEqualTo("invalid-order-created-key");
        assertThat(dltRecord.value()).isEqualTo(invalidPayload);

        Long outboxEventsCount = outboxEventRepository.count().block(Duration.ofSeconds(5));
        assertThat(outboxEventsCount).isZero();
    }

    @Test
    void shouldRetryRetryableConsumerFailureAndSendToDlt() throws Exception {
        String payload = "{\"message\":\"force retry\"}";

        retryFailureListener.reset();

        kafkaTemplate.send(
                TECHNICAL_FAILURE_TOPIC,
                "technical-failure-key",
                payload
        ).get();

        kafkaTemplate.flush();

        ConsumerRecord<String, String> dltRecord =
                awaitDltRecord(TECHNICAL_FAILURE_DLT_TOPIC, payload);

        assertThat(dltRecord.topic()).isEqualTo(TECHNICAL_FAILURE_DLT_TOPIC);
        assertThat(dltRecord.key()).isEqualTo("technical-failure-key");
        assertThat(dltRecord.value()).isEqualTo(payload);

        assertThat(retryFailureListener.attempts())
                .isGreaterThanOrEqualTo(4);
    }

    private ConsumerRecord<String, String> awaitDltRecord(
            String dltTopic,
            String expectedPayload
    ) {
        try (Consumer<String, String> consumer = createDltConsumer(dltTopic)) {
            long deadline = System.currentTimeMillis() + 15_000;

            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(500));

                List<ConsumerRecord<String, String>> matchingRecords =
                        StreamSupport.stream(records.spliterator(), false)
                                .filter(record -> dltTopic.equals(record.topic()))
                                .filter(record -> expectedPayload.equals(record.value()))
                                .toList();

                if (!matchingRecords.isEmpty()) {
                    return matchingRecords.getFirst();
                }
            }

            throw new AssertionError(
                    "Expected payload was not published to DLT topic: " + dltTopic
            );
        }
    }

    private Consumer<String, String> createDltConsumer(String dltTopic) {
        Map<String, Object> properties = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "dlt-test-consumer-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        );

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(dltTopic));

        return consumer;
    }

    private void waitForKafkaListeners() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;

        while (System.currentTimeMillis() < deadline) {
            boolean allContainersRunning =
                    kafkaListenerEndpointRegistry.getListenerContainers()
                            .stream()
                            .allMatch(MessageListenerContainer::isRunning);

            if (allContainersRunning) {
                Thread.sleep(1_000);
                return;
            }

            Thread.sleep(250);
        }

        throw new AssertionError("Kafka listener containers were not ready");
    }

    @TestConfiguration
    static class RetryFailureKafkaTestConfig {

        @Bean
        NewTopic technicalFailureTopic() {
            return TopicBuilder.name(TECHNICAL_FAILURE_TOPIC)
                    .partitions(1)
                    .replicas(1)
                    .build();
        }

        @Bean
        NewTopic technicalFailureDltTopic() {
            return TopicBuilder.name(TECHNICAL_FAILURE_DLT_TOPIC)
                    .partitions(1)
                    .replicas(1)
                    .build();
        }

        @Bean
        RetryFailureListener retryFailureListener() {
            return new RetryFailureListener();
        }
    }

    static class RetryFailureListener {

        private final AtomicInteger attempts = new AtomicInteger();

        void reset() {
            attempts.set(0);
        }

        int attempts() {
            return attempts.get();
        }

        @KafkaListener(
                topics = TECHNICAL_FAILURE_TOPIC,
                groupId = "technical-failure-dlt-it",
                containerFactory = "kafkaListenerContainerFactory"
        )
        void consume(String payload) {
            attempts.incrementAndGet();

            throw new IllegalStateException(
                    "Simulated retryable consumer failure"
            );
        }
    }
}