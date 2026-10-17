package com.festivalkafka.orderapi.api.dto;

import com.festivalkafka.orderapi.domain.Order;
import com.festivalkafka.orderapi.domain.OrderStatus;
import java.time.OffsetDateTime;
import java.util.UUID;

public record OrderResponse(
        UUID orderId,
        UUID customerId,
        long totalAmountCents,
        String currency,
        OrderStatus status,
        OffsetDateTime createdAt) {

    public static OrderResponse from(Order o) {
        return new OrderResponse(
                o.orderId(),
                o.customerId(),
                o.totalAmountCents(),
                o.currency(),
                o.status(),
                o.createdAt());
    }
}
