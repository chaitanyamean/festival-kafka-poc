package com.festivalkafka.payment.service;

import com.festivalkafka.payment.domain.PaymentStatus;
import com.festivalkafka.payment.repo.PaymentRepository;
import com.festivalkafka.payment.repo.ProcessedEventRepository;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Phase 1 compromise: the gateway call runs inside the transaction so dedupe + capture
 * decision + payments row are atomic. For the simulated gateway this is harmless (no
 * network). A real gateway integration must move the gateway call out of the DB
 * transaction and reconcile asynchronously — out of scope for Phase 1.
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository payments;
    private final ProcessedEventRepository processed;
    private final PaymentGateway gateway;

    public PaymentService(PaymentRepository payments,
                           ProcessedEventRepository processed,
                           PaymentGateway gateway) {
        this.payments = payments;
        this.processed = processed;
        this.gateway = gateway;
    }

    @Transactional
    public PaymentOutcome capturePayment(UUID orderId,
                                           UUID eventId,
                                           String eventType,
                                           long amountCents,
                                           String currency) {
        if (!processed.recordIfAbsent(eventId, eventType)) {
            log.debug("duplicate inventory.reserved event {} skipped", eventId);
            return PaymentOutcome.duplicate();
        }

        PaymentGateway.CaptureResult result = gateway.capture(orderId, amountCents, currency);
        UUID paymentId = result.success() ? result.paymentId() : UUID.randomUUID();

        if (result.success()) {
            payments.insert(paymentId, orderId, amountCents, currency, PaymentStatus.CAPTURED, null);
            log.debug("captured payment {} for orderId={}", paymentId, orderId);
            return PaymentOutcome.captured(paymentId, amountCents, currency);
        }

        payments.insert(paymentId, orderId, amountCents, currency, PaymentStatus.FAILED, result.failureReason());
        log.info("payment failed for orderId={} reason={}", orderId, result.failureReason());
        return PaymentOutcome.failed(result.failureReason());
    }
}
