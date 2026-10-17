package com.festivalkafka.inventory.kafka;

import com.festivalkafka.common.Topics;
import com.festivalkafka.events.common.EventEnvelope;
import com.festivalkafka.events.payment.PaymentFailed;
import com.festivalkafka.inventory.service.ReleaseOutcome;
import com.festivalkafka.inventory.service.ReservationService;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Compensation path: on payment.failed, release the reservation and publish
 * order.cancelled. We only publish on an actual RELEASED transition — DUPLICATE,
 * NO_RESERVATION, and ALREADY_RELEASED mean the saga has already settled or will be
 * settled by another worker.
 */
@Component
public class PaymentFailedListener {

    private final ReservationService reservationService;
    private final InventoryEventPublisher publisher;

    public PaymentFailedListener(ReservationService reservationService,
                                   InventoryEventPublisher publisher) {
        this.reservationService = reservationService;
        this.publisher = publisher;
    }

    @KafkaListener(topics = Topics.PAYMENT_FAILED, groupId = "${CONSUMER_GROUP:inventory-service}")
    public void onPaymentFailed(ConsumerRecord<String, PaymentFailed> record) {
        PaymentFailed event = record.value();
        EventEnvelope env = event.getEnvelope();
        UUID eventId = UUID.fromString(env.getEventId());
        UUID orderId = UUID.fromString(env.getOrderId());

        ReleaseOutcome outcome = reservationService.releaseForOrder(orderId, eventId, env.getEventType());
        if (outcome.kind() == ReleaseOutcome.Kind.RELEASED) {
            publisher.publishOrderCancelled(orderId, "PAYMENT_FAILED");
        }
    }
}
