package com.festivalkafka.inventory.kafka;

import com.festivalkafka.common.Topics;
import com.festivalkafka.events.common.EventEnvelope;
import com.festivalkafka.events.order.OrderCreated;
import com.festivalkafka.inventory.service.ReservationOutcome;
import com.festivalkafka.inventory.service.ReservationService;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Phase 3 batch listener. Dedupe + reservation INSERT + reservation_items INSERT
 * each collapse to one DB round-trip per poll; stock decrement still runs
 * per record because of SKU contention.
 *
 * <p>Outcome routing is unchanged from Phase 2:
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

    @KafkaListener(
            topics = Topics.ORDER_CREATED,
            groupId = "${CONSUMER_GROUP:inventory-service}",
            batch = "true",
            containerFactory = "batchKafkaListenerContainerFactory")
    public void onOrderCreatedBatch(List<ConsumerRecord<String, OrderCreated>> records) {
        if (records.isEmpty()) return;

        List<ReservationService.ReservationInput> inputs = new ArrayList<>(records.size());
        List<OrderCreated> values = new ArrayList<>(records.size());
        for (ConsumerRecord<String, OrderCreated> r : records) {
            OrderCreated value = r.value();
            EventEnvelope env = value.getEnvelope();
            inputs.add(new ReservationService.ReservationInput(
                    UUID.fromString(env.getOrderId()),
                    UUID.fromString(env.getEventId()),
                    env.getEventType(),
                    value.getItems()));
            values.add(value);
        }

        List<ReservationOutcome> outcomes = reservationService.tryReserveBatch(inputs);
        for (int i = 0; i < outcomes.size(); i++) {
            ReservationOutcome outcome = outcomes.get(i);
            OrderCreated value = values.get(i);
            UUID orderId = inputs.get(i).orderId();
            switch (outcome.kind()) {
                case DUPLICATE -> { /* nothing to publish */ }
                case RESERVED  -> publisher.publishInventoryReserved(
                        orderId, outcome.reservationId(),
                        value.getTotalAmountCents(), value.getCurrency());
                case FAILED    -> {
                    publisher.publishInventoryFailed(orderId, "OUT_OF_STOCK", outcome.failedSkus());
                    publisher.publishOrderCancelled(orderId, "INVENTORY_FAILED");
                }
            }
        }
    }
}
