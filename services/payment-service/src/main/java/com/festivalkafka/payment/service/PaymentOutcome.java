package com.festivalkafka.payment.service;

import java.util.UUID;

/**
 * Outcome of {@link PaymentService#capturePayment}. The listener uses {@code kind}
 * to decide which event (if any) to publish.
 */
public record PaymentOutcome(
        Kind kind,
        UUID paymentId,
        long amountCents,
        String currency,
        String failureReason) {

    public enum Kind { DUPLICATE, CAPTURED, FAILED }

    public static PaymentOutcome duplicate() {
        return new PaymentOutcome(Kind.DUPLICATE, null, 0L, null, null);
    }

    public static PaymentOutcome captured(UUID paymentId, long amountCents, String currency) {
        return new PaymentOutcome(Kind.CAPTURED, paymentId, amountCents, currency, null);
    }

    public static PaymentOutcome failed(String reason) {
        return new PaymentOutcome(Kind.FAILED, null, 0L, null, reason);
    }
}
