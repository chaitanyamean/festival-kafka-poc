-- order-api / orderdb initial schema.
--
-- Writer tables: orders, order_items. One order row + N item rows per POST /orders.
-- Idempotency: processed_events deduplicates inbound order.cancelled / shipment.created events.

CREATE TABLE orders (
    order_id            UUID         PRIMARY KEY,
    customer_id         UUID         NOT NULL,
    total_amount_cents  BIGINT       NOT NULL CHECK (total_amount_cents > 0),
    currency            VARCHAR(3)   NOT NULL DEFAULT 'INR',
    status              VARCHAR(32)  NOT NULL,   -- CREATED | COMPLETED | CANCELLED
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_orders_status      ON orders (status);
CREATE INDEX idx_orders_created_at  ON orders (created_at);

CREATE TABLE order_items (
    order_id          UUID         NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    sku_id            VARCHAR(64)  NOT NULL,
    quantity          INTEGER      NOT NULL CHECK (quantity > 0),
    unit_price_cents  BIGINT       NOT NULL CHECK (unit_price_cents > 0),
    PRIMARY KEY (order_id, sku_id)
);

-- Idempotent consumer dedupe table. Reused unchanged by all 6 services.
CREATE TABLE processed_events (
    event_id      UUID         PRIMARY KEY,
    event_type    VARCHAR(64)  NOT NULL,
    processed_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_processed_events_at ON processed_events (processed_at);
