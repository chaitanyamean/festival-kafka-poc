package com.festivalkafka.shipping.kafka;

import com.festivalkafka.common.Topics;
import com.festivalkafka.events.common.EventEnvelope;
import com.festivalkafka.events.payment.PaymentCaptured;
import com.festivalkafka.shipping.service.ShipmentOutcome;
import com.festivalkafka.shipping.service.ShippingService;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentCapturedListener {

    private final ShippingService shippingService;
    private final ShippingEventPublisher publisher;

    public PaymentCapturedListener(ShippingService shippingService, ShippingEventPublisher publisher) {
        this.shippingService = shippingService;
        this.publisher = publisher;
    }

    @KafkaListener(topics = Topics.PAYMENT_CAPTURED, groupId = "${CONSUMER_GROUP:shipping-service}")
    public void onPaymentCaptured(ConsumerRecord<String, PaymentCaptured> record) {
        PaymentCaptured event = record.value();
        EventEnvelope env = event.getEnvelope();
        UUID eventId = UUID.fromString(env.getEventId());
        UUID orderId = UUID.fromString(env.getOrderId());

        ShipmentOutcome outcome = shippingService.createShipment(orderId, eventId, env.getEventType());
        if (outcome.kind() == ShipmentOutcome.Kind.CREATED) {
            publisher.publishShipmentCreated(
                    orderId, outcome.shipmentId(), outcome.carrier(), outcome.trackingNumber());
        }
    }
}
