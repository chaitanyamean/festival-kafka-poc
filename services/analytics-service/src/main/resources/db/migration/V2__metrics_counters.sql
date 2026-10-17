-- Phase 1 extended analytics: besides saga_counts (which bins by event type),
-- track a flat `metrics_counters` table keyed by metric name. The initial
-- counter is `gmv_cents_total`, incremented on every OrderCreated by the
-- order's totalAmountCents. Future counters (returns, refunds) land here with
-- their own well-known keys.
--
-- orders/min and failure_rate are DERIVABLE at query time from saga_counts +
-- lifecycle_events; they intentionally don't get their own counter rows.

CREATE TABLE metrics_counters (
    name        VARCHAR(64)  PRIMARY KEY,
    value       BIGINT       NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

INSERT INTO metrics_counters (name, value) VALUES ('gmv_cents_total', 0);
