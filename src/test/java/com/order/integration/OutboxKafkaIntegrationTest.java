package com.order.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.order.application.service.impl.OutboxServiceImpl;
import com.order.domain.entity.OutboxEvent;
import com.order.domain.enums.OutboxStatus;
import com.order.infrastructure.repository.OutboxEventRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.r2dbc.test.autoconfigure.DataR2dbcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@DataR2dbcTest
@Import({
        OutboxServiceImpl.class,
        OutboxKafkaIntegrationTest.TestConfig.class
})
class OutboxKafkaIntegrationTest extends AbstractPostgresTestcontainersTest {

    private static final KafkaContainer KAFKA =
            new KafkaContainer(
                    DockerImageName.parse("apache/kafka-native:3.8.0")
            );

    static {
        KAFKA.start();
    }

    @Autowired
    private OutboxServiceImpl outboxService;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Test
    void shouldPublishPendingOutboxEventToKafkaAndMarkAsPublished() {
        String topic = "outbox.integration." + UUID.randomUUID();
        String key = "order-31";
        String payload = "{\"eventType\":\"ORDER_CREATED\",\"orderId\":31}";

        AtomicReference<Long> eventIdRef = new AtomicReference<>();

        StepVerifier.create(
                        cleanDatabase()
                                .then(outboxEventRepository.save(OutboxEvent.builder()
                                        .aggregateType("ORDER")
                                        .aggregateId(31L)
                                        .eventType("ORDER_CREATED")
                                        .topic(topic)
                                        .eventKey(key)
                                        .payload(payload)
                                        .status(OutboxStatus.PENDING)
                                        .retryCount(0)
                                        .createdAt(OffsetDateTime.now())
                                        .updatedAt(OffsetDateTime.now())
                                        .build()))
                                .doOnNext(savedEvent -> eventIdRef.set(savedEvent.getId()))
                                .then(outboxService.publishPublishableEvents(3))
                )
                .expectNext(1)
                .verifyComplete();

        String consumedValue = consumeSingleMessage(topic, key);

        assertThat(consumedValue).isEqualTo(payload);

        StepVerifier.create(outboxEventRepository.findById(eventIdRef.get()))
                .expectNextMatches(event ->
                        event.getStatus() == OutboxStatus.PUBLISHED
                                && event.getPublishedAt() != null
                                && event.getLastError() == null
                                && event.getRetryCount() == 0
                )
                .verifyComplete();
    }

    private Mono<Void> cleanDatabase() {
        return outboxEventRepository.deleteAll();
    }

    private static String consumeSingleMessage(
            String topic,
            String expectedKey
    ) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "outbox-kafka-test-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(topic));

            long deadline = System.currentTimeMillis() + 10_000;

            while (System.currentTimeMillis() < deadline) {
                var records = consumer.poll(Duration.ofMillis(500));

                for (var record : records) {
                    if (topic.equals(record.topic()) && expectedKey.equals(record.key())) {
                        return record.value();
                    }
                }
            }
        }

        throw new AssertionError("Expected Kafka message was not consumed from topic: " + topic);
    }

    @TestConfiguration
    static class TestConfig {

        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapper()
                    .registerModule(new JavaTimeModule());
        }

        @Bean
        KafkaTemplate<String, String> kafkaTemplate() {
            return new KafkaTemplate<>(
                    new DefaultKafkaProducerFactory<>(
                            Map.of(
                                    "bootstrap.servers", KAFKA.getBootstrapServers(),
                                    "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                                    "value.serializer", "org.apache.kafka.common.serialization.StringSerializer"
                            )
                    )
            );
        }
    }
}