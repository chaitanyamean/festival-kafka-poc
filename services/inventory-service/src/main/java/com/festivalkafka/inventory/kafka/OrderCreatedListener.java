package com.festivalkafka.inventory.kafka;

import com.festivalkafka.common.Topics;
import com.festivalkafka.events.common.EventEnvelope;
import com.festivalkafka.events.order.OrderCreated;
import com.festivalkafka.inventory.service.ReservationOutcome;
import com.festivalkafka.inventory.service.ReservationService;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Entry point from the saga's first topic. Routes outcomes to the publisher:
 *   DUPLICATE  -> no-op
 *   RESERVED   -> publish inventory.reserved
 *   FAILED     -> publish inventory.failed + order.cancelled
 */
@Component
public class OrderCreatedListener {

    private final ReservationService reservationService;
    private final InventoryEventPublisher publisher;

    public OrderCreatedListener(ReservationService reservationService,
                                 InventoryEventPublisher publisher) {
        this.reservationService = reservationService;
        this.publisher = publisher;
    }

    @KafkaListener(topics = Topics.ORDER_CREATED, groupId = "${CONSUMER_GROUP:inventory-service}")
    public void onOrderCreated(ConsumerRecord<String, OrderCreated> record) {
        OrderCreated event = record.value();
        EventEnvelope env = event.getEnvelope();
        UUID eventId = UUID.fromString(env.getEventId());
        UUID orderId = UUID.fromString(env.getOrderId());

        ReservationOutcome outcome = reservationService.tryReserve(
                orderId, eventId, env.getEventType(), event.getItems());

        switch (outcome.kind()) {
            case DUPLICATE -> { /* nothing to publish */ }
            case RESERVED  -> publisher.publishInventoryReserved(
                    orderId, outcome.reservationId(), event.getTotalAmountCents(), event.getCurrency());
            case FAILED    -> {
                publisher.publishInventoryFailed(orderId, "OUT_OF_STOCK", outcome.failedSkus());
                publisher.publishOrderCancelled(orderId, "INVENTORY_FAILED");
            }
        }
    }
}
