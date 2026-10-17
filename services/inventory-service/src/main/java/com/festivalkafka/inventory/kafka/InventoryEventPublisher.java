package com.festivalkafka.inventory.kafka;

import com.festivalkafka.common.EventHeaders;
import com.festivalkafka.common.Topics;
import com.festivalkafka.events.common.EventEnvelope;
import com.festivalkafka.events.inventory.InventoryFailed;
import com.festivalkafka.events.inventory.InventoryReserved;
import com.festivalkafka.events.order.OrderCancelled;
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
public class InventoryEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventPublisher.class);
    private static final int SCHEMA_VERSION = 1;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Clock clock;

    public InventoryEventPublisher(KafkaTemplate<String, Object> kafkaTemplate, Clock clock) {
        this.kafkaTemplate = kafkaTemplate;
        this.clock = clock;
    }

    public void publishInventoryReserved(UUID orderId, UUID reservationId, long amountCents, String currency) {
        UUID eventId = UUID.randomUUID();
        var event = InventoryReserved.newBuilder()
                .setEnvelope(envelope(eventId, orderId, "inventory.reserved"))
                .setReservationId(reservationId.toString())
                .setAmountCents(amountCents)
                .setCurrency(currency)
                .build();
        send(Topics.INVENTORY_RESERVED, orderId, eventId, "inventory.reserved", event);
    }

    public void publishInventoryFailed(UUID orderId, String reason, List<String> failedSkus) {
        UUID eventId = UUID.randomUUID();
        var event = InventoryFailed.newBuilder()
                .setEnvelope(envelope(eventId, orderId, "inventory.failed"))
                .setReason(reason)
                .setFailedSkus(List.copyOf(failedSkus))
                .build();
        send(Topics.INVENTORY_FAILED, orderId, eventId, "inventory.failed", event);
    }

    public void publishOrderCancelled(UUID orderId, String reason) {
        UUID eventId = UUID.randomUUID();
        var event = OrderCancelled.newBuilder()
                .setEnvelope(envelope(eventId, orderId, "order.cancelled"))
                .setReason(reason)
                .build();
        send(Topics.ORDER_CANCELLED, orderId, eventId, "order.cancelled", event);
    }

    private EventEnvelope envelope(UUID eventId, UUID orderId, String eventType) {
        return EventEnvelope.newBuilder()
                .setEventId(eventId.toString())
                .setOrderId(orderId.toString())
                .setEventType(eventType)
                .setOccurredAt(Instant.now(clock))
                .setSchemaVersion(SCHEMA_VERSION)
                .setTraceId(null)
                .build();
    }

    private void send(String topic, UUID orderId, UUID eventId, String eventType, Object event) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, orderId.toString(), event);
        record.headers()
                .add(EventHeaders.EVENT_ID,       eventId.toString().getBytes(StandardCharsets.UTF_8))
                .add(EventHeaders.EVENT_TYPE,     eventType.getBytes(StandardCharsets.UTF_8))
                .add(EventHeaders.SCHEMA_VERSION, Integer.toString(SCHEMA_VERSION).getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record).whenComplete((r, ex) -> {
            if (ex != null) {
                log.error("failed to publish {} for orderId={} eventId={}", eventType, orderId, eventId, ex);
            }
        });
    }
}
