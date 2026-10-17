package com.festivalkafka.payment.kafka;

import com.festivalkafka.common.EventHeaders;
import com.festivalkafka.common.Topics;
import com.festivalkafka.events.common.EventEnvelope;
import com.festivalkafka.events.payment.PaymentCaptured;
import com.festivalkafka.events.payment.PaymentFailed;
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
public class PaymentEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventPublisher.class);
    private static final int SCHEMA_VERSION = 1;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Clock clock;

    public PaymentEventPublisher(KafkaTemplate<String, Object> kafkaTemplate, Clock clock) {
        this.kafkaTemplate = kafkaTemplate;
        this.clock = clock;
    }

    public void publishPaymentCaptured(UUID orderId, UUID paymentId, long amountCents, String currency) {
        UUID eventId = UUID.randomUUID();
        var event = PaymentCaptured.newBuilder()
                .setEnvelope(envelope(eventId, orderId, "payment.captured"))
                .setPaymentId(paymentId.toString())
                .setAmountCapturedCents(amountCents)
                .setCurrency(currency)
                .build();
        send(Topics.PAYMENT_CAPTURED, orderId, eventId, "payment.captured", event);
    }

    public void publishPaymentFailed(UUID orderId, String reason) {
        UUID eventId = UUID.randomUUID();
        var event = PaymentFailed.newBuilder()
                .setEnvelope(envelope(eventId, orderId, "payment.failed"))
                .setReason(reason)
                .build();
        send(Topics.PAYMENT_FAILED, orderId, eventId, "payment.failed", event);
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
