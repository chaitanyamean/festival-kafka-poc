package com.festivalkafka.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.festivalkafka.common.Topics;
import com.festivalkafka.events.common.EventEnvelope;
import com.festivalkafka.events.common.OrderItem;
import com.festivalkafka.events.order.OrderCreated;
import com.festivalkafka.events.shipping.ShipmentCreated;
import com.festivalkafka.notification.repo.NotificationLogRepository;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
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
 * Produces an OrderCreated and a ShipmentCreated — two different Avro types on two
 * different topics — and verifies both land in notifications_log. Exercises the
 * pattern-matching dispatch end-to-end.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
class NotificationServiceIntegrationTest {

    private static final Network NET = Network.newNetwork();

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("notificationdb")
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

    @Autowired NotificationLogRepository notifications;

    @Test
    void lifecycleEvents_areDispatchedToNotificationsLog() {
        UUID orderId = UUID.randomUUID();

        OrderCreated oc = OrderCreated.newBuilder()
                .setEnvelope(envelope(orderId, "order.created"))
                .setCustomerId(UUID.randomUUID().toString())
                .setItems(List.of(OrderItem.newBuilder()
                        .setSkuId("SKU-FESTIVAL-001")
                        .setQuantity(1)
                        .setUnitPriceCents(10_000L)
                        .build()))
                .setTotalAmountCents(10_000L)
                .setCurrency("INR")
                .build();

        ShipmentCreated sc = ShipmentCreated.newBuilder()
                .setEnvelope(envelope(orderId, "shipment.created"))
                .setShipmentId(UUID.randomUUID().toString())
                .setCarrier("BLUEDART")
                .setTrackingNumber("BLUE-DEADBEEF")
                .build();

        produce(Topics.ORDER_CREATED,    orderId.toString(), oc);
        produce(Topics.SHIPMENT_CREATED, orderId.toString(), sc);

        await().atMost(Duration.ofSeconds(25)).untilAsserted(() ->
                assertThat(notifications.countByOrderId(orderId)).isEqualTo(2));
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
