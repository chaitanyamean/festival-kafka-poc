-- notification-service / notificationdb initial schema.
--
-- notifications_log : one row per processed lifecycle event
-- processed_events  : dedupe table (same shape across every service)

CREATE TABLE notifications_log (
    id                      BIGSERIAL    PRIMARY KEY,
    event_id                UUID         NOT NULL,
    order_id                UUID         NOT NULL,
    event_type              VARCHAR(64)  NOT NULL,
    notification_channel    VARCHAR(16)  NOT NULL,     -- EMAIL | SMS | PUSH
    message                 TEXT         NOT NULL,
    created_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_notifications_log_order_id   ON notifications_log (order_id);
CREATE INDEX idx_notifications_log_event_type ON notifications_log (event_type);
CREATE INDEX idx_notifications_log_created_at ON notifications_log (created_at);

CREATE TABLE processed_events (
    event_id      UUID         PRIMARY KEY,
    event_type    VARCHAR(64)  NOT NULL,
    processed_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_processed_events_at ON processed_events (processed_at);
