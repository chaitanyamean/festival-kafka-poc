package com.festivalkafka.inventory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.festivalkafka.common.Topics;
import com.festivalkafka.events.common.EventEnvelope;
import com.festivalkafka.events.common.OrderItem;
import com.festivalkafka.events.inventory.InventoryFailed;
import com.festivalkafka.events.inventory.InventoryReserved;
import com.festivalkafka.events.order.OrderCancelled;
import com.festivalkafka.events.order.OrderCreated;
import com.festivalkafka.events.payment.PaymentFailed;
import com.festivalkafka.inventory.domain.Reservation;
import com.festivalkafka.inventory.domain.ReservationStatus;
import com.festivalkafka.inventory.repo.ReservationRepository;
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
 * Produces OrderCreated events into the broker and asserts the service's reaction via:
 *   1. The reservations table (via {@link ReservationRepository})
 *   2. The downstream Kafka topics
 *
 * The seed SKU "SKU-00001" is created by Flyway migration V2 before tests run (1000 SKUs seeded).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
class InventoryServiceIntegrationTest {

    private static final Network NET = Network.newNetwork();

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("inventorydb")
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

    @Autowired ReservationRepository reservations;

    @Test
    void orderCreated_withAvailableSku_reservesAndPublishes() {
        UUID orderId = UUID.randomUUID();
        OrderCreated event = buildOrderCreated(orderId, "SKU-00001", 3, 10_000L);

        produce(Topics.ORDER_CREATED, orderId.toString(), event);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Optional<Reservation> r = reservations.findByOrderId(orderId);
            assertThat(r).isPresent();
            assertThat(r.get().status()).isEqualTo(ReservationStatus.RESERVED);
        });

        ConsumerRecord<String, Object> rec = pollSingle(Topics.INVENTORY_RESERVED);
        assertThat(rec.value()).isInstanceOf(InventoryReserved.class);
        InventoryReserved ir = (InventoryReserved) rec.value();
        assertThat(ir.getEnvelope().getOrderId()).isEqualTo(orderId.toString());
        assertThat(ir.getEnvelope().getEventType()).isEqualTo("inventory.reserved");
        assertThat(UUID.fromString(ir.getReservationId())).isNotNull();
    }

    @Test
    void orderCreated_withUnknownSku_failsAndPublishesCancellation() {
        UUID orderId = UUID.randomUUID();
        OrderCreated event = buildOrderCreated(orderId, "SKU-DOES-NOT-EXIST", 1, 10_000L);

        produce(Topics.ORDER_CREATED, orderId.toString(), event);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            // No reservation row persisted on failure
            assertThat(reservations.findByOrderId(orderId)).isEmpty();
        });

        ConsumerRecord<String, Object> failRec = pollSingle(Topics.INVENTORY_FAILED);
        InventoryFailed failed = (InventoryFailed) failRec.value();
        assertThat(failed.getEnvelope().getOrderId()).isEqualTo(orderId.toString());
        assertThat(failed.getReason()).isEqualTo("OUT_OF_STOCK");
        assertThat(failed.getFailedSkus()).containsExactly("SKU-DOES-NOT-EXIST");

        ConsumerRecord<String, Object> cancelRec = pollSingle(Topics.ORDER_CANCELLED);
        OrderCancelled cancelled = (OrderCancelled) cancelRec.value();
        assertThat(cancelled.getEnvelope().getOrderId()).isEqualTo(orderId.toString());
        assertThat(cancelled.getReason()).isEqualTo("INVENTORY_FAILED");
    }

    @Test
    void paymentFailed_releasesReservationAndPublishesCancellation() {
        // Step 1 — set up a reservation by driving an order through the happy path.
        UUID orderId = UUID.randomUUID();
        OrderCreated created = buildOrderCreated(orderId, "SKU-00001", 2, 7_500L);
        produce(Topics.ORDER_CREATED, orderId.toString(), created);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Optional<Reservation> r = reservations.findByOrderId(orderId);
            assertThat(r).isPresent();
            assertThat(r.get().status()).isEqualTo(ReservationStatus.RESERVED);
        });

        // Step 2 — publish payment.failed for the same orderId. inventory-service
        // should release the reservation and emit order.cancelled.
        PaymentFailed payFailed = PaymentFailed.newBuilder()
                .setEnvelope(com.festivalkafka.events.common.EventEnvelope.newBuilder()
                        .setEventId(UUID.randomUUID().toString())
                        .setOrderId(orderId.toString())
                        .setEventType("payment.failed")
                        .setOccurredAt(Instant.now())
                        .setSchemaVersion(1)
                        .setTraceId(null)
                        .build())
                .setReason("CARD_DECLINED")
                .build();
        produce(Topics.PAYMENT_FAILED, orderId.toString(), payFailed);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Optional<Reservation> r = reservations.findByOrderId(orderId);
            assertThat(r).isPresent();
            assertThat(r.get().status()).isEqualTo(ReservationStatus.RELEASED);
        });

        // There will be TWO messages on order.cancelled across both tests in the
        // same shared broker: this test's payment-failed cancellation and the
        // previous test's inventory-failed cancellation. Poll until we see one
        // whose reason matches this path.
        OrderCancelled cancelled = pollUntilReason(Topics.ORDER_CANCELLED, orderId, "PAYMENT_FAILED");
        assertThat(cancelled.getEnvelope().getOrderId()).isEqualTo(orderId.toString());
        assertThat(cancelled.getReason()).isEqualTo("PAYMENT_FAILED");
    }

    private OrderCancelled pollUntilReason(String topic, UUID orderId, String reason) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl());
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        try (KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                var records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, Object> r : records) {
                    if (r.value() instanceof OrderCancelled oc
                            && orderId.toString().equals(oc.getEnvelope().getOrderId())
                            && reason.equals(oc.getReason())) {
                        return oc;
                    }
                }
            }
            throw new AssertionError("no OrderCancelled with reason=" + reason + " for orderId=" + orderId);
        }
    }

    // ---------- helpers ----------

    private OrderCreated buildOrderCreated(UUID orderId, String skuId, int qty, long priceCents) {
        EventEnvelope env = EventEnvelope.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setOrderId(orderId.toString())
                .setEventType("order.created")
                .setOccurredAt(Instant.now())
                .setSchemaVersion(1)
                .setTraceId(null)
                .build();
        OrderItem item = OrderItem.newBuilder()
                .setSkuId(skuId)
                .setQuantity(qty)
                .setUnitPriceCents(priceCents)
                .build();
        return OrderCreated.newBuilder()
                .setEnvelope(env)
                .setCustomerId(UUID.randomUUID().toString())
                .setItems(List.of(item))
                .setTotalAmountCents(priceCents * qty)
                .setCurrency("INR")
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
