package com.festivalkafka.payment.service;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Simulated payment gateway. Returns success with probability (1 - failureRate) and
 * failure otherwise. The failure reason is sampled from a small fixed set so analytics
 * can group by it.
 */
@Component
public class SimulatedPaymentGateway implements PaymentGateway {

    private static final Logger log = LoggerFactory.getLogger(SimulatedPaymentGateway.class);
    private static final List<String> FAILURE_REASONS =
            List.of("INSUFFICIENT_FUNDS", "CARD_DECLINED", "GATEWAY_TIMEOUT", "RISK_FLAGGED");

    private final double failureRate;
    private final long processingDelayMs;

    public SimulatedPaymentGateway(
            @Value("${payment.gateway.failure-rate:0.01}") double failureRate,
            @Value("${payment.gateway.processing-delay-ms:0}") long processingDelayMs) {
        if (failureRate < 0.0 || failureRate > 1.0) {
            throw new IllegalArgumentException("failureRate must be in [0.0, 1.0], got " + failureRate);
        }
        this.failureRate = failureRate;
        this.processingDelayMs = processingDelayMs;
        log.info("SimulatedPaymentGateway configured: failureRate={} processingDelayMs={}",
                failureRate, processingDelayMs);
    }

    @Override
    public CaptureResult capture(UUID orderId, long amountCents, String currency) {
        if (processingDelayMs > 0) {
            try {
                Thread.sleep(processingDelayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return CaptureResult.failed("INTERRUPTED");
            }
        }
        if (ThreadLocalRandom.current().nextDouble() < failureRate) {
            String reason = FAILURE_REASONS.get(ThreadLocalRandom.current().nextInt(FAILURE_REASONS.size()));
            return CaptureResult.failed(reason);
        }
        return CaptureResult.captured(UUID.randomUUID());
    }
}
