package com.festivalkafka.inventory.service;

import java.util.List;
import java.util.UUID;

/**
 * Outcome of {@link ReservationService#tryReserve}. Three states so the listener can
 * decide what to publish without re-querying the DB.
 */
public record ReservationOutcome(Kind kind, UUID reservationId, List<String> failedSkus) {

    public enum Kind { DUPLICATE, RESERVED, FAILED }

    public static ReservationOutcome duplicate() {
        return new ReservationOutcome(Kind.DUPLICATE, null, List.of());
    }

    public static ReservationOutcome reserved(UUID reservationId) {
        return new ReservationOutcome(Kind.RESERVED, reservationId, List.of());
    }

    public static ReservationOutcome failed(List<String> failedSkus) {
        return new ReservationOutcome(Kind.FAILED, null, List.copyOf(failedSkus));
    }
}
