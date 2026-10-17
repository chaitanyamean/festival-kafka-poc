package com.festivalkafka.notification.kafka;

import com.festivalkafka.common.Topics;
import com.festivalkafka.events.common.EventEnvelope;
import com.festivalkafka.events.inventory.InventoryFailed;
import com.festivalkafka.events.inventory.InventoryReserved;
import com.festivalkafka.events.order.OrderCancelled;
import com.festivalkafka.events.order.OrderCreated;
import com.festivalkafka.events.payment.PaymentCaptured;
import com.festivalkafka.events.payment.PaymentFailed;
import com.festivalkafka.events.shipping.ShipmentCreated;
import com.festivalkafka.notification.domain.NotificationChannel;
import com.festivalkafka.notification.service.NotificationService;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Subscribes to every lifecycle topic in its own consumer group so notifications
 * are independent of saga progression. Uses Java 21 pattern-matching switch over
 * the Avro SpecificRecord subtype to build a channel + message per event, then
 * delegates to {@link NotificationService} for dedupe + persist.
 */
@Component
public class LifecycleFanInListener {

    private static final Logger log = LoggerFactory.getLogger(LifecycleFanInListener.class);

    private final NotificationService notifications;

    public LifecycleFanInListener(NotificationService notifications) {
        this.notifications = notifications;
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
            groupId = "${CONSUMER_GROUP:notification-service}")
    public void onLifecycleEvent(ConsumerRecord<String, Object> record) {
        Object value = record.value();
        EventEnvelope env;
        NotificationChannel channel;
        String message;

        switch (value) {
            case OrderCreated oc -> {
                env = oc.getEnvelope();
                channel = NotificationChannel.EMAIL;
                message = "Order placed. Total: " + oc.getTotalAmountCents() + " " + oc.getCurrency();
            }
            case InventoryReserved ir -> {
                env = ir.getEnvelope();
                channel = NotificationChannel.PUSH;
                message = "Items reserved (reservation " + ir.getReservationId() + ")";
            }
            case InventoryFailed inf -> {
                env = inf.getEnvelope();
                channel = NotificationChannel.EMAIL;
                message = "Order could not be fulfilled: " + inf.getReason();
            }
            case PaymentCaptured pc -> {
                env = pc.getEnvelope();
                channel = NotificationChannel.PUSH;
                message = "Payment captured: " + pc.getAmountCapturedCents() + " " + pc.getCurrency();
            }
            case PaymentFailed pf -> {
                env = pf.getEnvelope();
                channel = NotificationChannel.SMS;
                message = "Payment failed: " + pf.getReason();
            }
            case ShipmentCreated sc -> {
                env = sc.getEnvelope();
                channel = NotificationChannel.EMAIL;
                message = "Shipped via " + sc.getCarrier() + ", tracking " + sc.getTrackingNumber();
            }
            case OrderCancelled oc -> {
                env = oc.getEnvelope();
                channel = NotificationChannel.EMAIL;
                message = "Order cancelled: " + oc.getReason();
            }
            default -> {
                log.warn("unknown event type on topic={} class={}",
                        record.topic(),
                        value == null ? "null" : value.getClass().getName());
                return;
            }
        }

        notifications.record(
                UUID.fromString(env.getEventId()),
                UUID.fromString(env.getOrderId()),
                env.getEventType(),
                channel,
                message);
    }
}
