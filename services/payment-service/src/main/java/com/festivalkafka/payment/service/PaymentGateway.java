package com.festivalkafka.payment.service;

import java.util.UUID;

/**
 * Payment gateway abstraction. Phase 1 ships a simulated implementation driven by a
 * failure-rate config. A real gateway client would replace this behind the same contract.
 */
public interface PaymentGateway {

    /** Attempts to capture a payment synchronously. Never throws for domain failures. */
    CaptureResult capture(UUID orderId, long amountCents, String currency);

    record CaptureResult(boolean success, UUID paymentId, String failureReason) {

        public static CaptureResult captured(UUID paymentId) {
            return new CaptureResult(true, paymentId, null);
        }

        public static CaptureResult failed(String reason) {
            return new CaptureResult(false, null, reason);
        }
    }
}
