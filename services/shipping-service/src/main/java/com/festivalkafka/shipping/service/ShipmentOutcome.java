package com.festivalkafka.shipping.service;

import java.util.UUID;

public record ShipmentOutcome(Kind kind, UUID shipmentId, String carrier, String trackingNumber) {

    public enum Kind { DUPLICATE, CREATED }

    public static ShipmentOutcome duplicate() {
        return new ShipmentOutcome(Kind.DUPLICATE, null, null, null);
    }

    public static ShipmentOutcome created(UUID shipmentId, String carrier, String trackingNumber) {
        return new ShipmentOutcome(Kind.CREATED, shipmentId, carrier, trackingNumber);
    }
}
