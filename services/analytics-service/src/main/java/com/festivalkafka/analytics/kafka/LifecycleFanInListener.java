package com.festivalkafka.analytics.kafka;

import com.festivalkafka.analytics.service.AnalyticsService;
import com.festivalkafka.common.Topics;
import com.festivalkafka.events.common.EventEnvelope;
import com.festivalkafka.events.inventory.InventoryFailed;
import com.festivalkafka.events.inventory.InventoryReserved;
import com.festivalkafka.events.order.OrderCancelled;
import com.festivalkafka.events.order.OrderCreated;
import com.festivalkafka.events.payment.PaymentCaptured;
import com.festivalkafka.events.payment.PaymentFailed;
import com.festivalkafka.events.shipping.ShipmentCreated;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Subscribes to every lifecycle topic in its own consumer group. All seven event
 * types reduce to the same shape — (eventId, orderId, eventType, occurredAt) — so
 * the dispatch is just envelope extraction via pattern-matching switch.
 */
@Component
public class LifecycleFanInListener {

    private static final Logger log = LoggerFactory.getLogger(LifecycleFanInListener.class);

    private final AnalyticsService analytics;

    public LifecycleFanInListener(AnalyticsService analytics) {
        this.analytics = analytics;
    }

    @KafkaListener(
            topics = {
                Topics.ORDER_CREATED,
                Topics.INVENTORY_RESERVED,
                Topics.INVENTORY_FAILED,
                Topics.PAYMENT_CAPTURED,
                Topics.PAYMENT_FAILED,
                Topics.SHIPMENT_CREATED,
                Topics.ORDER_CANCELLED
            },
            groupId = "${CONSUMER_GROUP:analytics-service}")
    public void onLifecycleEvent(ConsumerRecord<String, Object> record) {
        Object value = record.value();
        EventEnvelope env;
        long gmvDeltaCents = 0L;

        switch (value) {
            case OrderCreated oc        -> {
                env = oc.getEnvelope();
                // Only OrderCreated contributes to GMV. Every other event in the
                // saga references the same order; counting again would double-count.
                gmvDeltaCents = oc.getTotalAmountCents();
            }
            case InventoryReserved ir   -> env = ir.getEnvelope();
            case InventoryFailed inf    -> env = inf.getEnvelope();
            case PaymentCaptured pc     -> env = pc.getEnvelope();
            case PaymentFailed pf       -> env = pf.getEnvelope();
            case ShipmentCreated sc     -> env = sc.getEnvelope();
            case OrderCancelled oc      -> env = oc.getEnvelope();
            default -> {
                log.warn("unknown event type on topic={} class={}",
                        record.topic(),
                        value == null ? "null" : value.getClass().getName());
                return;
            }
        }

        analytics.record(
                UUID.fromString(env.getEventId()),
                UUID.fromString(env.getOrderId()),
                env.getEventType(),
                env.getOccurredAt(),
                gmvDeltaCents);
    }
}
