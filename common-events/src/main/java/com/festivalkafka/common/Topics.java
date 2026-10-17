package com.festivalkafka.common;

/**
 * Kafka topic names for the order lifecycle saga.
 *
 * <p>All lifecycle topics use {@code orderId} as the partition key so that every event for a given
 * order lands on the same partition and is processed in order by any single-threaded consumer within
 * a group.
 *
 * <p>Topic names are stable across phases. Partition count and replication factor change per phase
 * (see docs/PHASES.md and the ADRs in docs/DECISIONS.md).
 */
public final class Topics {

    private Topics() {}

    public static final String ORDER_CREATED       = "order.created";
    public static final String INVENTORY_RESERVED  = "inventory.reserved";
    public static final String INVENTORY_FAILED    = "inventory.failed";
    public static final String PAYMENT_CAPTURED    = "payment.captured";
    public static final String PAYMENT_FAILED      = "payment.failed";
    public static final String SHIPMENT_CREATED    = "shipment.created";
    public static final String ORDER_CANCELLED     = "order.cancelled";

    /** All lifecycle topics. Used by notification-service and analytics-service. */
    public static final String[] ALL_LIFECYCLE_TOPICS = {
        ORDER_CREATED,
        INVENTORY_RESERVED,
        INVENTORY_FAILED,
        PAYMENT_CAPTURED,
        PAYMENT_FAILED,
        SHIPMENT_CREATED,
        ORDER_CANCELLED
    };

    /** Dead-letter topic suffix (used from Phase 2 onwards). */
    public static final String DLT_SUFFIX = ".dlt";
}
