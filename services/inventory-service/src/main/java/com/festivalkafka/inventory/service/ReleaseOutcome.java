package com.festivalkafka.inventory.service;

/**
 * Outcome of {@link ReservationService#releaseForOrder}. Only {@link Kind#RELEASED}
 * triggers an outbound order.cancelled publish; the other states indicate the state
 * machine has already settled.
 */
public record ReleaseOutcome(Kind kind) {

    public enum Kind {
        DUPLICATE,            // same payment.failed event already processed
        RELEASED,             // reservation existed and was flipped RESERVED -> RELEASED
        NO_RESERVATION,       // no reservation for this orderId; inventory.failed path already handled it
        ALREADY_RELEASED      // reservation exists but was already RELEASED (defensive)
    }

    public static ReleaseOutcome duplicate()         { return new ReleaseOutcome(Kind.DUPLICATE); }
    public static ReleaseOutcome released()          { return new ReleaseOutcome(Kind.RELEASED); }
    public static ReleaseOutcome noReservation()     { return new ReleaseOutcome(Kind.NO_RESERVATION); }
    public static ReleaseOutcome alreadyReleased()   { return new ReleaseOutcome(Kind.ALREADY_RELEASED); }
}
