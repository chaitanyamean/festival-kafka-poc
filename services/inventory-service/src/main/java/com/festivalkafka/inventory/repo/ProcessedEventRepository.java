package com.festivalkafka.inventory.repo;

import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Dedupe store (identical shape across every service). Callers run this inside the
 * same transaction as the business update so a roll-back of the business update also
 * rolls back the dedupe row.
 */
@Repository
public class ProcessedEventRepository {

    private final JdbcTemplate jdbc;

    public ProcessedEventRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public boolean recordIfAbsent(UUID eventId, String eventType) {
        int rows = jdbc.update(
                "INSERT INTO processed_events (event_id, event_type) VALUES (?, ?) ON CONFLICT (event_id) DO NOTHING",
                eventId, eventType);
        return rows == 1;
    }
}
