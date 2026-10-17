package com.festivalkafka.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.festivalkafka.common.Topics;
import com.festivalkafka.events.common.EventEnvelope;
import com.festivalkafka.events.inventory.InventoryReserved;
import com.festivalkafka.events.payment.PaymentCaptured;
import com.festivalkafka.payment.domain.Payment;
import com.festivalkafka.payment.domain.PaymentStatus;
import com.festivalkafka.payment.repo.PaymentRepository;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Verifies the happy path: InventoryReserved in -> PaymentCaptured out, payments row persisted.
 * failure-rate is forced to 0.0 to make the SimulatedPaymentGateway deterministic.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = "payment.gateway.failure-rate=0.0")
@Testcontainers
class PaymentServiceIntegrationTest {

    private static final Network NET = Network.newNetwork();

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("paymentdb")
                    .withUsername("fkp")
                    .withPassword("fkp");

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
                    .withKraft()
                    .withNetwork(NET)
                    .withListener(() -> "kafka:19092")
                    .withNetworkAliases("kafka");

    @Container
    static final GenericContainer<?> SCHEMA_REGISTRY =
            new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.6.1"))
                    .withNetwork(NET)
                    .withNetworkAliases("schema-registry")
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                    .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_TOPIC", "_schemas")
                    .withExposedPorts(8081)
                    .dependsOn(KAFKA)
                    .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url",      POSTGRES::getJdbcUrl);
        r.add("spring.datasource.username", POSTGRES::getUsername);
        r.add("spring.datasource.password", POSTGRES::getPassword);
        r.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        r.add("spring.kafka.properties.schema.registry.url", () -> schemaRegistryUrl());
    }

    private static String schemaRegistryUrl() {
        return "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081);
    }

    @Autowired PaymentRepository payments;

    @Test
    void inventoryReserved_withZeroFailureRate_capturesAndPublishes() {
        UUID orderId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();
        long amount = 25_000L;

        InventoryReserved event = InventoryReserved.newBuilder()
                .setEnvelope(EventEnvelope.newBuilder()
                        .setEventId(UUID.randomUUID().toString())
                        .setOrderId(orderId.toString())
                        .setEventType("inventory.reserved")
                        .setOccurredAt(Instant.now())
                        .setSchemaVersion(1)
                        .setTraceId(null)
                        .build())
                .setReservationId(reservationId.toString())
                .setAmountCents(amount)
                .setCurrency("INR")
                .build();

        produce(Topics.INVENTORY_RESERVED, orderId.toString(), event);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Optional<Payment> p = payments.findByOrderId(orderId);
            assertThat(p).isPresent();
            assertThat(p.get().status()).isEqualTo(PaymentStatus.CAPTURED);
            assertThat(p.get().amountCents()).isEqualTo(amount);
            assertThat(p.get().currency()).isEqualTo("INR");
        });

        ConsumerRecord<String, Object> rec = pollSingle(Topics.PAYMENT_CAPTURED);
        PaymentCaptured captured = (PaymentCaptured) rec.value();
        assertThat(captured.getEnvelope().getOrderId()).isEqualTo(orderId.toString());
        assertThat(captured.getAmountCapturedCents()).isEqualTo(amount);
        assertThat(captured.getCurrency()).isEqualTo("INR");
    }

    private void produce(String topic, String key, Object event) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", schemaRegistryUrl());
        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, key, event));
            producer.flush();
        }
    }

    private ConsumerRecord<String, Object> pollSingle(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl());
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        AtomicReference<ConsumerRecord<String, Object>> capture = new AtomicReference<>();
        try (KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            await().atMost(Duration.ofSeconds(15)).until(() -> {
                var records = consumer.poll(Duration.ofSeconds(1));
                if (!records.isEmpty()) {
                    capture.set(records.iterator().next());
                    return true;
                }
                return false;
            });
        }
        return capture.get();
    }
}
