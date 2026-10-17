package com.festivalkafka.common;

/**
 * Standard Kafka message header names used across all services.
 *
 * <p>{@link #EVENT_ID} and {@link #EVENT_TYPE} are mirrored from the Avro {@code EventEnvelope} into
 * headers so that consumers can route or dedupe without deserializing the payload. {@link
 * #TRACEPARENT} carries W3C trace context from Phase 3 onwards.
 */
public final class EventHeaders {

    private EventHeaders() {}

    /** Mirrors envelope.eventId. Used by consumers for idempotent dedupe. */
    public static final String EVENT_ID = "event-id";

    /** Mirrors envelope.eventType, e.g. "order.created". */
    public static final String EVENT_TYPE = "event-type";

    /** W3C traceparent (OpenTelemetry). Populated starting in Phase 3. */
    public static final String TRACEPARENT = "traceparent";

    /** Mirrors envelope.schemaVersion as a string. */
    public static final String SCHEMA_VERSION = "schema-version";
}
