package com.festivalkafka.shipping.service;

import com.festivalkafka.shipping.repo.ProcessedEventRepository;
import com.festivalkafka.shipping.repo.ShipmentRepository;
import java.util.List;
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

        String carrier = CARRIERS.get(ThreadLocalRandom.current().nextInt(CARRIERS.size()));
        String trackingNumber = carrier.substring(0, Math.min(carrier.length(), 4)) + "-"
                + Long.toHexString(ThreadLocalRandom.current().nextLong()).toUpperCase();
        UUID shipmentId = UUID.randomUUID();

        shipments.insert(shipmentId, orderId, carrier, trackingNumber);
        log.debug("shipment {} created for orderId={} via {}", shipmentId, orderId, carrier);
        return ShipmentOutcome.created(shipmentId, carrier, trackingNumber);
    }
}
