-- inventory-service / inventorydb initial schema.
--
-- skus          : stock catalog, with available + reserved counters
-- reservations  : one row per successful order reservation
-- reservation_items : the per-SKU breakdown of each reservation (needed to roll back on release)
-- processed_events  : dedupe table (identical shape across all services)

CREATE TABLE skus (
    sku_id      VARCHAR(64)  PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    available   INTEGER      NOT NULL CHECK (available >= 0),
    reserved    INTEGER      NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE reservations (
    reservation_id  UUID         PRIMARY KEY,
    order_id        UUID         NOT NULL UNIQUE,
    status          VARCHAR(32)  NOT NULL,         -- RESERVED | RELEASED
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_reservations_order_id ON reservations (order_id);
CREATE INDEX idx_reservations_status   ON reservations (status);

CREATE TABLE reservation_items (
    reservation_id  UUID         NOT NULL REFERENCES reservations(reservation_id) ON DELETE CASCADE,
    sku_id          VARCHAR(64)  NOT NULL,
    quantity        INTEGER      NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (reservation_id, sku_id)
);

CREATE TABLE processed_events (
    event_id      UUID         PRIMARY KEY,
    event_type    VARCHAR(64)  NOT NULL,
    processed_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_processed_events_at ON processed_events (processed_at);
