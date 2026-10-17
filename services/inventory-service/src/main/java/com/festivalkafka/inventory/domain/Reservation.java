package com.festivalkafka.inventory.domain;

import java.time.OffsetDateTime;
import java.util.UUID;

public record Reservation(
        UUID reservationId,
        UUID orderId,
        ReservationStatus status,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt) {
}
