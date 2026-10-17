package com.festivalkafka.payment.kafka;

import com.festivalkafka.common.Topics;
import com.festivalkafka.events.common.EventEnvelope;
import com.festivalkafka.events.inventory.InventoryReserved;
import com.festivalkafka.payment.service.PaymentOutcome;
import com.festivalkafka.payment.service.PaymentService;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Entry point into payment-service. Routes outcomes to the publisher:
 *   DUPLICATE -> no-op
 *   CAPTURED  -> publish payment.captured
 *   FAILED    -> publish payment.failed (inventory-service consumes it and releases
 *                the reservation, then publishes order.cancelled)
 */
@Component
public class InventoryReservedListener {

    private final PaymentService paymentService;
    private final PaymentEventPublisher publisher;

    public InventoryReservedListener(PaymentService paymentService,
                                       PaymentEventPublisher publisher) {
        this.paymentService = paymentService;
        this.publisher = publisher;
    }

    @KafkaListener(topics = Topics.INVENTORY_RESERVED, groupId = "${CONSUMER_GROUP:payment-service}")
    public void onInventoryReserved(ConsumerRecord<String, InventoryReserved> record) {
        InventoryReserved event = record.value();
        EventEnvelope env = event.getEnvelope();
        UUID eventId = UUID.fromString(env.getEventId());
        UUID orderId = UUID.fromString(env.getOrderId());

        PaymentOutcome outcome = paymentService.capturePayment(
                orderId, eventId, env.getEventType(),
                event.getAmountCents(), event.getCurrency());

        switch (outcome.kind()) {
            case DUPLICATE -> { /* nothing to publish */ }
            case CAPTURED  -> publisher.publishPaymentCaptured(
                    orderId, outcome.paymentId(), outcome.amountCents(), outcome.currency());
            case FAILED    -> publisher.publishPaymentFailed(orderId, outcome.failureReason());
        }
    }
}
