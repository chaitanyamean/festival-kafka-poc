package com.festivalkafka.orderapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.festivalkafka.common.Topics;
import com.festivalkafka.events.order.OrderCreated;
import com.festivalkafka.orderapi.domain.Order;
import com.festivalkafka.orderapi.domain.OrderStatus;
import com.festivalkafka.orderapi.repo.OrderRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Boots Kafka (KRaft), Confluent Schema Registry, and Postgres in Testcontainers;
 * wires the Spring app at them; exercises POST /orders end to end and asserts both
 * the DB row and the produced Kafka message.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@Testcontainers
class OrderApiIntegrationTest {

    private static final Network NET = Network.newNetwork();

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("orderdb")
                    .withUsername("fkp")
                    .withPassword("fkp");

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
                    .withKraft()
                    .withNetwork(NET)
                    .withListener(() -> "kafka:19092")   // stable advertised address for in-network peers
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
        r.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        r.add("spring.datasource.username", POSTGRES::getUsername);
        r.add("spring.datasource.password", POSTGRES::getPassword);
        r.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        r.add("spring.kafka.properties.schema.registry.url", () -> schemaRegistryUrl());
    }

    private static String schemaRegistryUrl() {
        return "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081);
    }

    @Autowired MockMvc mvc;
    @Autowired OrderRepository orderRepository;
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void postOrder_persistsRowAndPublishesEvent() throws Exception {
        UUID customerId = UUID.randomUUID();
        String body = mapper.writeValueAsString(Map.of(
                "customerId", customerId.toString(),
                "items", List.of(Map.of(
                        "skuId", "SKU-DEMO-1",
                        "quantity", 2,
                        "unitPriceCents", 12_500L)),
                "city", "Bengaluru",
                "paymentMethod", "UPI"));

        String resp = mvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isAccepted())
                .andReturn().getResponse().getContentAsString();

        JsonNode j = mapper.readTree(resp);
        UUID orderId = UUID.fromString(j.get("orderId").asText());
        assertThat(j.get("totalAmountCents").asLong()).isEqualTo(25_000L);
        assertThat(j.get("status").asText()).isEqualTo("PENDING");
        assertThat(j.get("currency").asText()).isEqualTo("INR");

        // DB assertion
        Optional<Order> fromDb = orderRepository.findById(orderId);
        assertThat(fromDb).isPresent();
        assertThat(fromDb.get().status()).isEqualTo(OrderStatus.PENDING);
        assertThat(fromDb.get().totalAmountCents()).isEqualTo(25_000L);
        assertThat(fromDb.get().customerId()).isEqualTo(customerId);

        // Kafka assertion
        ConsumerRecord<String, OrderCreated> rec = pollSingle(Topics.ORDER_CREATED);
        assertThat(rec.key()).isEqualTo(orderId.toString());
        assertThat(rec.value().getCustomerId()).isEqualTo(customerId.toString());
        assertThat(rec.value().getTotalAmountCents()).isEqualTo(25_000L);
        assertThat(rec.value().getCurrency()).isEqualTo("INR");
        assertThat(rec.value().getCity()).isEqualTo("Bengaluru");
        assertThat(rec.value().getPaymentMethod().name()).isEqualTo("UPI");
        assertThat(rec.value().getItems()).hasSize(1);
        assertThat(rec.value().getItems().get(0).getSkuId()).isEqualTo("SKU-DEMO-1");
        assertThat(rec.value().getItems().get(0).getQuantity()).isEqualTo(2);
        assertThat(rec.value().getItems().get(0).getUnitPriceCents()).isEqualTo(12_500L);

        // Envelope assertions (ADR-0001)
        assertThat(rec.value().getEnvelope().getEventType()).isEqualTo("order.created");
        assertThat(rec.value().getEnvelope().getOrderId()).isEqualTo(orderId.toString());
        assertThat(rec.value().getEnvelope().getSchemaVersion()).isEqualTo(1);
        assertThat(UUID.fromString(rec.value().getEnvelope().getEventId())).isNotNull();
    }

    @Test
    void postOrder_withEmptyItems_returns400() throws Exception {
        String body = mapper.writeValueAsString(Map.of(
                "customerId", UUID.randomUUID().toString(),
                "items", List.of(),
                "city", "Mumbai",
                "paymentMethod", "UPI"));

        mvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest());
    }

    private ConsumerRecord<String, OrderCreated> pollSingle(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl());
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        try (KafkaConsumer<String, OrderCreated> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                var records = consumer.poll(Duration.ofSeconds(1));
                if (!records.isEmpty()) {
                    return records.iterator().next();
                }
            }
            throw new AssertionError("No record received on " + topic + " within 10s");
        }
    }
}
