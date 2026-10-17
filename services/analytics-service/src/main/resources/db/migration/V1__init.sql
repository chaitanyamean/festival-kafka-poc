-- analytics-service / analyticsdb initial schema.
--
-- lifecycle_events : fact table, one row per unique eventId
-- saga_counts      : small aggregation table (one row per event_type)
--
-- Dedupe uses lifecycle_events.event_id as the primary key. ON CONFLICT DO NOTHING
-- on insert tells us when a duplicate arrived; the aggregation update only runs on
-- a fresh insert. No separate processed_events table — the fact table IS the dedupe.

CREATE TABLE lifecycle_events (
    event_id      UUID         PRIMARY KEY,
    order_id      UUID         NOT NULL,
    event_type    VARCHAR(64)  NOT NULL,
    occurred_at   TIMESTAMPTZ  NOT NULL,
    recorded_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_lifecycle_events_order_id    ON lifecycle_events (order_id);
CREATE INDEX idx_lifecycle_events_event_type  ON lifecycle_events (event_type);
CREATE INDEX idx_lifecycle_events_occurred_at ON lifecycle_events (occurred_at);

CREATE TABLE saga_counts (
    event_type   VARCHAR(64)  PRIMARY KEY,
    event_count  BIGINT       NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Pre-seed one row per known event type so saga_counts always has 7 rows for the
-- demo query to pretty-print.
INSERT INTO saga_counts (event_type, event_count) VALUES
    ('order.created',      0),
    ('inventory.reserved', 0),
    ('inventory.failed',   0),
    ('payment.captured',   0),
    ('payment.failed',     0),
    ('shipment.created',   0),
    ('order.cancelled',    0);
