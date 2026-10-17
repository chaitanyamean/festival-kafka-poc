package com.festivalkafka.shipping.domain;

import java.time.OffsetDateTime;
import java.util.UUID;

public record Shipment(
        UUID shipmentId,
        UUID orderId,
        String carrier,
        String trackingNumber,
        OffsetDateTime createdAt) {
}
