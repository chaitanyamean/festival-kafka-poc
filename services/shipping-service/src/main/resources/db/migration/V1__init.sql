-- shipping-service / shippingdb initial schema.
--
-- shipments        : terminal shipment record, one per completed order
-- processed_events : dedupe table (same shape across every service)

CREATE TABLE shipments (
    shipment_id      UUID         PRIMARY KEY,
    order_id         UUID         NOT NULL UNIQUE,
    carrier          VARCHAR(32)  NOT NULL,
    tracking_number  VARCHAR(64)  NOT NULL,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_shipments_order_id ON shipments (order_id);
CREATE INDEX idx_shipments_carrier  ON shipments (carrier);

CREATE TABLE processed_events (
    event_id      UUID         PRIMARY KEY,
    event_type    VARCHAR(64)  NOT NULL,
    processed_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_processed_events_at ON processed_events (processed_at);
