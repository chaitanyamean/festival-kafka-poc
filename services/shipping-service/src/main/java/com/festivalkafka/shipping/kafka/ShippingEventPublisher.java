package com.festivalkafka.shipping.kafka;

import com.festivalkafka.common.EventHeaders;
import com.festivalkafka.common.Topics;
import com.festivalkafka.events.common.EventEnvelope;
import com.festivalkafka.events.shipping.ShipmentCreated;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class ShippingEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(ShippingEventPublisher.class);
    private static final int SCHEMA_VERSION = 1;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Clock clock;

    public ShippingEventPublisher(KafkaTemplate<String, Object> kafkaTemplate, Clock clock) {
        this.kafkaTemplate = kafkaTemplate;
        this.clock = clock;
    }

    public void publishShipmentCreated(UUID orderId, UUID shipmentId, String carrier, String trackingNumber) {
        UUID eventId = UUID.randomUUID();
        EventEnvelope env = EventEnvelope.newBuilder()
                .setEventId(eventId.toString())
                .setOrderId(orderId.toString())
                .setEventType("shipment.created")
                .setOccurredAt(Instant.now(clock))
                .setSchemaVersion(SCHEMA_VERSION)
                .setTraceId(null)
                .build();
        ShipmentCreated event = ShipmentCreated.newBuilder()
                .setEnvelope(env)
                .setShipmentId(shipmentId.toString())
                .setCarrier(carrier)
                .setTrackingNumber(trackingNumber)
                .build();

        ProducerRecord<String, Object> record =
                new ProducerRecord<>(Topics.SHIPMENT_CREATED, orderId.toString(), event);
        record.headers()
                .add(EventHeaders.EVENT_ID,       eventId.toString().getBytes(StandardCharsets.UTF_8))
                .add(EventHeaders.EVENT_TYPE,     "shipment.created".getBytes(StandardCharsets.UTF_8))
                .add(EventHeaders.SCHEMA_VERSION, Integer.toString(SCHEMA_VERSION).getBytes(StandardCharsets.UTF_8));

        kafkaTemplate.send(record).whenComplete((r, ex) -> {
            if (ex != null) {
                log.error("failed to publish shipment.created for orderId={} eventId={}", orderId, eventId, ex);
            }
        });
    }
}
