package com.festivalkafka.orderapi.domain;

import java.time.OffsetDateTime;
import java.util.UUID;

public record Order(
        UUID orderId,
        UUID customerId,
        long totalAmountCents,
        String currency,
        OrderStatus status,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt) {
}
