package com.festivalkafka.orderapi.kafka;

import com.festivalkafka.common.EventHeaders;
import com.festivalkafka.common.Topics;
import com.festivalkafka.events.common.EventEnvelope;
import com.festivalkafka.events.common.OrderItem;
import com.festivalkafka.events.common.PaymentMethod;
import com.festivalkafka.events.order.OrderCreated;
import com.festivalkafka.orderapi.api.dto.ItemRequest;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(OrderEventPublisher.class);
    private static final String EVENT_TYPE = "order.created";
    private static final int SCHEMA_VERSION = 1;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Clock clock;

    public OrderEventPublisher(KafkaTemplate<String, Object> kafkaTemplate, Clock clock) {
        this.kafkaTemplate = kafkaTemplate;
        this.clock = clock;
    }

    public void publishOrderCreated(
            UUID orderId,
            UUID customerId,
            long totalCents,
            String currency,
            List<ItemRequest> items,
            String city,
            String paymentMethod) {

        UUID eventId = UUID.randomUUID();

        EventEnvelope envelope = EventEnvelope.newBuilder()
                .setEventId(eventId.toString())
                .setOrderId(orderId.toString())
                .setEventType(EVENT_TYPE)
                .setOccurredAt(Instant.now(clock))
                .setSchemaVersion(SCHEMA_VERSION)
                .setTraceId(null)                       // populated starting Phase 3
                .build();

        List<OrderItem> avroItems = items.stream()
                .map(i -> OrderItem.newBuilder()
                        .setSkuId(i.skuId())
                        .setQuantity(i.quantity())
                        .setUnitPriceCents(i.unitPriceCents())
                        .build())
                .toList();

        OrderCreated event = OrderCreated.newBuilder()
                .setEnvelope(envelope)
                .setCustomerId(customerId.toString())
                .setItems(avroItems)
                .setTotalAmountCents(totalCents)
                .setCurrency(currency)
                .setCity(city)
                .setPaymentMethod(parsePaymentMethod(paymentMethod))
                .build();

        ProducerRecord<String, Object> record =
                new ProducerRecord<>(Topics.ORDER_CREATED, orderId.toString(), event);
        record.headers()
                .add(EventHeaders.EVENT_ID, eventId.toString().getBytes(StandardCharsets.UTF_8))
                .add(EventHeaders.EVENT_TYPE, EVENT_TYPE.getBytes(StandardCharsets.UTF_8))
                .add(EventHeaders.SCHEMA_VERSION, Integer.toString(SCHEMA_VERSION).getBytes(StandardCharsets.UTF_8));

        kafkaTemplate.send(record).whenComplete((result, ex) -> {
            if (ex != null) {
                // Phase 1: log-and-drop. Phase 4 outbox makes this impossible by moving
                // the publish responsibility out of this process entirely.
                log.error("failed to publish {} for orderId={} eventId={}", EVENT_TYPE, orderId, eventId, ex);
            }
        });
    }

    private static PaymentMethod parsePaymentMethod(String s) {
        if (s == null || s.isBlank()) return PaymentMethod.UPI;
        try {
            return PaymentMethod.valueOf(s.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return PaymentMethod.UPI;
        }
    }
}
