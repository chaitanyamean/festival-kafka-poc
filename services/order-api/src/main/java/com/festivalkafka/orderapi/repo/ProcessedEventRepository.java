package com.festivalkafka.orderapi.repo;

import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Dedupe store for inbound Kafka events. Pattern reused by every service.
 *
 * <p>Caller is expected to run {@link #recordIfAbsent(UUID, String)} inside the same
 * transaction as the business update it protects: if business update fails and the
 * tx rolls back, the dedupe row is rolled back too, so the next delivery of the same
 * event is re-processed instead of silently swallowed.
 */
@Repository
public class ProcessedEventRepository {

    private final JdbcTemplate jdbc;

    public ProcessedEventRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** @return true if the event is newly recorded; false if it was already processed. */
    public boolean recordIfAbsent(UUID eventId, String eventType) {
        int rows = jdbc.update(
                "INSERT INTO processed_events (event_id, event_type) VALUES (?, ?) ON CONFLICT (event_id) DO NOTHING",
                eventId, eventType);
        return rows == 1;
    }
}
