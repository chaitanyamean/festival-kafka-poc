-- payment-service / paymentdb initial schema.
--
-- payments         : one row per capture attempt (CAPTURED or FAILED, both recorded)
-- processed_events : dedupe table (same shape across every service)

CREATE TABLE payments (
    payment_id      UUID         PRIMARY KEY,
    order_id        UUID         NOT NULL UNIQUE,
    amount_cents    BIGINT       NOT NULL CHECK (amount_cents > 0),
    currency        VARCHAR(3)   NOT NULL,
    status          VARCHAR(32)  NOT NULL,         -- CAPTURED | FAILED
    failure_reason  VARCHAR(64),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_payments_order_id ON payments (order_id);
CREATE INDEX idx_payments_status   ON payments (status);

CREATE TABLE processed_events (
    event_id      UUID         PRIMARY KEY,
    event_type    VARCHAR(64)  NOT NULL,
    processed_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_processed_events_at ON processed_events (processed_at);
