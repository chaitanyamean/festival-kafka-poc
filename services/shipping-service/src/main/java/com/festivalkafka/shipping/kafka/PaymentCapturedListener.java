package com.festivalkafka.shipping.kafka;

import com.festivalkafka.common.Topics;
import com.festivalkafka.events.common.EventEnvelope;
import com.festivalkafka.events.payment.PaymentCaptured;
import com.festivalkafka.shipping.service.ShipmentOutcome;
import com.festivalkafka.shipping.service.ShippingService;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Batch listener (Phase 3). The poll result is handed to
 * {@link ShippingService#createShipmentsBatch(List)} so the two DB writes
 * (dedupe + shipment insert) collapse to one round-trip each regardless of
 * batch size. Shipment-created events are then published in a tight loop; the
 * producer's linger.ms coalesces them on the wire.
 */
@Component
public class PaymentCapturedListener {

    private final ShippingService shippingService;
    private final ShippingEventPublisher publisher;

    public PaymentCapturedListener(ShippingService shippingService, ShippingEventPublisher publisher) {
        this.shippingService = shippingService;
        this.publisher = publisher;
    }

    @KafkaListener(
            topics = Topics.PAYMENT_CAPTURED,
            groupId = "${CONSUMER_GROUP:shipping-service}",
            batch = "true",
            containerFactory = "batchKafkaListenerContainerFactory")
    public void onPaymentCapturedBatch(List<ConsumerRecord<String, PaymentCaptured>> records) {
        if (records.isEmpty()) return;

        List<ShippingService.Event> events = new ArrayList<>(records.size());
        for (ConsumerRecord<String, PaymentCaptured> r : records) {
            PaymentCaptured value = r.value();
            EventEnvelope env = value.getEnvelope();
            events.add(new ShippingService.Event(
                    UUID.fromString(env.getOrderId()),
                    UUID.fromString(env.getEventId()),
                    env.getEventType()));
        }

        List<ShipmentOutcome> outcomes = shippingService.createShipmentsBatch(events);
        for (int i = 0; i < outcomes.size(); i++) {
            ShipmentOutcome outcome = outcomes.get(i);
            if (outcome.kind() == ShipmentOutcome.Kind.CREATED) {
                publisher.publishShipmentCreated(
                        events.get(i).orderId(),
                        outcome.shipmentId(),
                        outcome.carrier(),
                        outcome.trackingNumber());
            }
        }
    }
}
