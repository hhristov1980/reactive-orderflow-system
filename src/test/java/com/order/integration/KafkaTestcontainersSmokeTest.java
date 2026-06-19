package com.order.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaTestcontainersSmokeTest {

    private static final KafkaContainer KAFKA =
            new KafkaContainer(
                    DockerImageName.parse("apache/kafka-native:3.8.0")
            );

    static {
        KAFKA.start();
    }

    @Test
    void shouldProduceAndConsumeMessageWithKafkaTestcontainer() {
        String topic = "test.smoke";
        String key = "key-1";
        String value = "{\"message\":\"hello-kafka-testcontainers\"}";

        KafkaTemplate<String, String> kafkaTemplate =
                new KafkaTemplate<>(
                        new DefaultKafkaProducerFactory<>(
                                Map.of(
                                        "bootstrap.servers", KAFKA.getBootstrapServers(),
                                        "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                                        "value.serializer", "org.apache.kafka.common.serialization.StringSerializer"
                                )
                        )
                );

        kafkaTemplate.send(new ProducerRecord<>(topic, key, value)).join();

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "smoke-test-group");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer =
                     new KafkaConsumer<>(consumerProperties)) {

            consumer.subscribe(List.of(topic));

            ConsumerRecord<String, String> received = null;
            long deadline = System.currentTimeMillis() + 10_000;

            while (received == null && System.currentTimeMillis() < deadline) {
                var records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    if (topic.equals(record.topic()) && key.equals(record.key())) {
                        received = record;
                        break;
                    }
                }
            }

            assertThat(received).isNotNull();
            assertThat(received.key()).isEqualTo(key);
            assertThat(received.value()).isEqualTo(value);
        }
    }
}