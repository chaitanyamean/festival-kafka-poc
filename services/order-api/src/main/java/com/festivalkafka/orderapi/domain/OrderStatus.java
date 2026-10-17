package com.festivalkafka.orderapi.domain;

public enum OrderStatus {
    /** Order accepted, saga in flight. */
    PENDING,
    /** Terminal: shipment.created observed. */
    CONFIRMED,
    /** Terminal: inventory.failed OR payment.failed path. */
    CANCELLED
}
