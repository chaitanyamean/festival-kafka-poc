package com.festivalkafka.analytics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.festivalkafka.analytics.repo.LifecycleEventRepository;
import com.festivalkafka.analytics.repo.SagaCountsRepository;
import com.festivalkafka.common.Topics;
import com.festivalkafka.events.common.EventEnvelope;
import com.festivalkafka.events.payment.PaymentCaptured;
import com.festivalkafka.events.shipping.ShipmentCreated;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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
 * Publishes a PaymentCaptured and a ShipmentCreated and asserts both are recorded
 * in lifecycle_events and reflected in saga_counts.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
class AnalyticsServiceIntegrationTest {

    private static final Network NET = Network.newNetwork();

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("analyticsdb")
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

    @Autowired LifecycleEventRepository events;
    @Autowired SagaCountsRepository counts;

    @Test
    void twoEventsOfDifferentTypes_areCountedAndFactPersisted() {
        UUID orderId = UUID.randomUUID();

        PaymentCaptured pc = PaymentCaptured.newBuilder()
                .setEnvelope(envelope(orderId, "payment.captured"))
                .setPaymentId(UUID.randomUUID().toString())
                .setAmountCapturedCents(10_000L)
                .setCurrency("INR")
                .build();

        ShipmentCreated sc = ShipmentCreated.newBuilder()
                .setEnvelope(envelope(orderId, "shipment.created"))
                .setShipmentId(UUID.randomUUID().toString())
                .setCarrier("BLUEDART")
                .setTrackingNumber("BLUE-DEADBEEF")
                .build();

        produce(Topics.PAYMENT_CAPTURED, orderId.toString(), pc);
        produce(Topics.SHIPMENT_CREATED, orderId.toString(), sc);

        await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
            assertThat(events.countByEventType("payment.captured")).isGreaterThanOrEqualTo(1);
            assertThat(events.countByEventType("shipment.created")).isGreaterThanOrEqualTo(1);
            long captured = counts.findAll().stream()
                    .filter(s -> s.eventType().equals("payment.captured"))
                    .findFirst().orElseThrow().eventCount();
            long shipped = counts.findAll().stream()
                    .filter(s -> s.eventType().equals("shipment.created"))
                    .findFirst().orElseThrow().eventCount();
            assertThat(captured).isGreaterThanOrEqualTo(1);
            assertThat(shipped).isGreaterThanOrEqualTo(1);
        });
    }

    private EventEnvelope envelope(UUID orderId, String eventType) {
        return EventEnvelope.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setOrderId(orderId.toString())
                .setEventType(eventType)
                .setOccurredAt(Instant.now())
                .setSchemaVersion(1)
                .setTraceId(null)
                .build();
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
}
