package com.festivalkafka.shipping.service;

import com.festivalkafka.shipping.repo.ProcessedEventRepository;
import com.festivalkafka.shipping.repo.ShipmentRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Terminal saga step. Phase 1 has no failure mode here — every payment.captured
 * yields a shipment.created. A real shipping integration would acquire a label from
 * a carrier API; we fake both carrier and tracking number for the walkthrough.
 *
 * <p>Phase 3 added {@link #createShipmentsBatch(List)} that groups the DB writes
 * for a Kafka poll into two round-trips (processed_events + shipments) regardless
 * of batch size. The single-record form is preserved for the integration test and
 * for anyone who flips the listener back to record-mode.
 */
@Service
public class ShippingService {

    private static final Logger log = LoggerFactory.getLogger(ShippingService.class);

    private static final List<String> CARRIERS =
            List.of("BLUEDART", "DELHIVERY", "DTDC", "FEDEX", "ECOM_EXPRESS");

    private final ShipmentRepository shipments;
    private final ProcessedEventRepository processed;

    public ShippingService(ShipmentRepository shipments, ProcessedEventRepository processed) {
        this.shipments = shipments;
        this.processed = processed;
    }

    @Transactional
    public ShipmentOutcome createShipment(UUID orderId, UUID eventId, String eventType) {
        if (!processed.recordIfAbsent(eventId, eventType)) {
            log.debug("duplicate payment.captured event {} skipped", eventId);
            return ShipmentOutcome.duplicate();
        }
        ShipmentDraft draft = draft(orderId);
        shipments.insert(draft.shipmentId(), orderId, draft.carrier(), draft.trackingNumber());
        return ShipmentOutcome.created(draft.shipmentId(), draft.carrier(), draft.trackingNumber());
    }

    /**
     * Batch form: one dedupe round-trip + one shipments-insert round-trip, regardless
     * of {@code events.size()}. The returned list is index-aligned with the input;
     * duplicates (whose event was already in processed_events) carry
     * {@link ShipmentOutcome.Kind#DUPLICATE}.
     */
    @Transactional
    public List<ShipmentOutcome> createShipmentsBatch(List<Event> events) {
        if (events.isEmpty()) return List.of();

        List<ProcessedEventRepository.Entry> entries = events.stream()
                .map(e -> new ProcessedEventRepository.Entry(e.eventId(), e.eventType()))
                .toList();
        Set<UUID> newEventIds = processed.batchRecordIfAbsent(entries);

        List<ShipmentOutcome> outcomes = new ArrayList<>(events.size());
        List<ShipmentRepository.Row> rowsToInsert = new ArrayList<>(newEventIds.size());

        for (Event e : events) {
            if (!newEventIds.contains(e.eventId())) {
                outcomes.add(ShipmentOutcome.duplicate());
                continue;
            }
            ShipmentDraft draft = draft(e.orderId());
            rowsToInsert.add(new ShipmentRepository.Row(
                    draft.shipmentId(), e.orderId(), draft.carrier(), draft.trackingNumber()));
            outcomes.add(ShipmentOutcome.created(draft.shipmentId(), draft.carrier(), draft.trackingNumber()));
        }

        shipments.batchInsert(rowsToInsert);
        log.debug("shipping batch: in={} dup={} created={}",
                events.size(), events.size() - newEventIds.size(), newEventIds.size());
        return outcomes;
    }

    private ShipmentDraft draft(UUID orderId) {
        String carrier = CARRIERS.get(ThreadLocalRandom.current().nextInt(CARRIERS.size()));
        String trackingNumber = carrier.substring(0, Math.min(carrier.length(), 4)) + "-"
                + Long.toHexString(ThreadLocalRandom.current().nextLong()).toUpperCase();
        return new ShipmentDraft(UUID.randomUUID(), carrier, trackingNumber);
    }

    private record ShipmentDraft(UUID shipmentId, String carrier, String trackingNumber) {}

    /** Input tuple for the batch form; carries only what the dedupe + insert need. */
    public record Event(UUID orderId, UUID eventId, String eventType) {}
}
