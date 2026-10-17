package com.festivalkafka.inventory.service;

import com.festivalkafka.events.common.OrderItem;
import com.festivalkafka.inventory.domain.Reservation;
import com.festivalkafka.inventory.domain.ReservationItem;
import com.festivalkafka.inventory.domain.ReservationStatus;
import com.festivalkafka.inventory.repo.ProcessedEventRepository;
import com.festivalkafka.inventory.repo.ReservationRepository;
import com.festivalkafka.inventory.repo.SkuRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Core inventory use cases, all run inside a single transaction so that dedupe + stock
 * mutations + reservation state transitions are atomic. The listener layer calls these
 * then performs Kafka publishing OUTSIDE the transaction — Phase 4 outbox will absorb
 * that split (see ADR-0004).
 */
@Service
public class ReservationService {

    private static final Logger log = LoggerFactory.getLogger(ReservationService.class);

    private final SkuRepository skus;
    private final ReservationRepository reservations;
    private final ProcessedEventRepository processed;

    public ReservationService(SkuRepository skus,
                              ReservationRepository reservations,
                              ProcessedEventRepository processed) {
        this.skus = skus;
        this.reservations = reservations;
        this.processed = processed;
    }

    /**
     * Attempt to reserve stock for an order. Partial-failure handling: if any item
     * cannot be decremented, compensate by incrementing back the ones that were. This
     * keeps the operation at-least-once-safe without SELECT FOR UPDATE.
     */
    @Transactional
    public ReservationOutcome tryReserve(UUID orderId, UUID eventId, String eventType,
                                          List<OrderItem> items) {
        if (!processed.recordIfAbsent(eventId, eventType)) {
            log.debug("duplicate order.created event {} skipped", eventId);
            return ReservationOutcome.duplicate();
        }

        List<String> failedSkus = new ArrayList<>();
        List<ReservationItem> applied = new ArrayList<>();

        for (OrderItem item : items) {
            int rows = skus.decrementAvailable(item.getSkuId(), item.getQuantity());
            if (rows == 1) {
                applied.add(new ReservationItem(item.getSkuId(), item.getQuantity()));
            } else {
                failedSkus.add(item.getSkuId());
            }
        }

        if (!failedSkus.isEmpty()) {
            // Compensate the partial decrements so the ledger is consistent on commit.
            for (ReservationItem a : applied) {
                skus.incrementAvailable(a.skuId(), a.quantity());
            }
            log.info("inventory reservation failed for orderId={} failedSkus={}", orderId, failedSkus);
            return ReservationOutcome.failed(failedSkus);
        }

        UUID reservationId = UUID.randomUUID();
        reservations.insertReservation(reservationId, orderId, ReservationStatus.RESERVED);
        reservations.insertItems(reservationId, applied);
        log.debug("reservation {} created for orderId={}", reservationId, orderId);
        return ReservationOutcome.reserved(reservationId);
    }

    /**
     * Release the reservation held for an order. Called by the payment.failed listener;
     * returns outcome so the listener can decide to publish order.cancelled.
     */
    @Transactional
    public ReleaseOutcome releaseForOrder(UUID orderId, UUID eventId, String eventType) {
        if (!processed.recordIfAbsent(eventId, eventType)) {
            log.debug("duplicate payment.failed event {} skipped", eventId);
            return ReleaseOutcome.duplicate();
        }

        Optional<Reservation> maybe = reservations.findByOrderId(orderId);
        if (maybe.isEmpty()) {
            log.debug("no reservation for orderId={}; nothing to release", orderId);
            return ReleaseOutcome.noReservation();
        }

        Reservation r = maybe.get();
        if (r.status() == ReservationStatus.RELEASED) {
            log.debug("reservation {} already released", r.reservationId());
            return ReleaseOutcome.alreadyReleased();
        }

        int moved = reservations.transitionToReleased(r.reservationId());
        if (moved == 0) {
            // Concurrent release raced us; another worker owns the stock-restore and the
            // order.cancelled publish. We treat this as a dedupe-equivalent outcome.
            log.debug("reservation {} transition raced; nothing to do", r.reservationId());
            return ReleaseOutcome.alreadyReleased();
        }

        for (ReservationItem item : reservations.findItems(r.reservationId())) {
            skus.incrementAvailable(item.skuId(), item.quantity());
        }
        log.info("released reservation {} for orderId={}", r.reservationId(), orderId);
        return ReleaseOutcome.released();
    }
}
