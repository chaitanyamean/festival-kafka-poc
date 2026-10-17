package com.festivalkafka.analytics.repo;

import java.time.Instant;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class LifecycleEventRepository {

    private final JdbcTemplate jdbc;

    public LifecycleEventRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Inserts a fact row if the event_id has not been seen before. The PK is the
     * dedupe key — a duplicate returns 0 rows affected and the caller skips the
     * downstream count increment.
     */
    public boolean insertIfAbsent(UUID eventId, UUID orderId, String eventType, Instant occurredAt) {
        int rows = jdbc.update(
                """
                INSERT INTO lifecycle_events (event_id, order_id, event_type, occurred_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (event_id) DO NOTHING
                """,
                eventId,
                orderId,
                eventType,
                java.sql.Timestamp.from(occurredAt));
        return rows == 1;
    }

    public int countByEventType(String eventType) {
        return jdbc.queryForObject(
                "SELECT COUNT(*) FROM lifecycle_events WHERE event_type = ?",
                Integer.class,
                eventType);
    }
}
