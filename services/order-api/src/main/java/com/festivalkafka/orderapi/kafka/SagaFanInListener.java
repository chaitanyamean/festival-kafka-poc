package com.festivalkafka.orderapi.kafka;

import com.festivalkafka.common.Topics;
import com.festivalkafka.events.common.EventEnvelope;
import com.festivalkafka.events.order.OrderCancelled;
import com.festivalkafka.events.shipping.ShipmentCreated;
import com.festivalkafka.orderapi.service.OrderService;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes the two events that transition an order to a terminal state so the
 * REST read model stays accurate.
 *
 * <p>Both handlers delegate to {@link OrderService} which runs the dedupe + status
 * update in a single transaction (see {@link OrderService#markCompletedIdempotent}).
 */
@Component
public class SagaFanInListener {

    private final OrderService orderService;

    public SagaFanInListener(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(topics = Topics.SHIPMENT_CREATED, groupId = "${CONSUMER_GROUP:order-api}")
    public void onShipmentCreated(ConsumerRecord<String, ShipmentCreated> record) {
        EventEnvelope env = record.value().getEnvelope();
        orderService.markConfirmedIdempotent(
                UUID.fromString(env.getOrderId()),
                UUID.fromString(env.getEventId()),
                env.getEventType());
    }

    @KafkaListener(topics = Topics.ORDER_CANCELLED, groupId = "${CONSUMER_GROUP:order-api}")
    public void onOrderCancelled(ConsumerRecord<String, OrderCancelled> record) {
        OrderCancelled value = record.value();
        EventEnvelope env = value.getEnvelope();
        orderService.markCancelledIdempotent(
                UUID.fromString(env.getOrderId()),
                UUID.fromString(env.getEventId()),
                env.getEventType(),
                String.valueOf(value.getReason()));
    }
}
