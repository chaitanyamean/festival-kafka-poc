package com.festivalkafka.payment.domain;

import java.time.OffsetDateTime;
import java.util.UUID;

public record Payment(
        UUID paymentId,
        UUID orderId,
        long amountCents,
        String currency,
        PaymentStatus status,
        String failureReason,
        OffsetDateTime createdAt) {
}
