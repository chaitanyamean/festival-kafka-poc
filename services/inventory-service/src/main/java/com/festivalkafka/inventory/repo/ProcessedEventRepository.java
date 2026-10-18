package com.festivalkafka.inventory.repo;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Dedupe store (identical shape across every service). Callers run this inside the
 * same transaction as the business update so a roll-back of the business update also
 * rolls back the dedupe row.
 *
 * <p>Phase 3 added {@link #batchRecordIfAbsent(List)}: one INSERT round-trip for the
 * whole poll result, returning the subset of event IDs that were newly recorded.
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

    public Set<UUID> batchRecordIfAbsent(List<Entry> entries) {
        if (entries == null || entries.isEmpty()) return Collections.emptySet();
        String placeholders = String.join(",", Collections.nCopies(entries.size(), "(?,?)"));
        String sql = "INSERT INTO processed_events (event_id, event_type) VALUES " + placeholders
                + " ON CONFLICT (event_id) DO NOTHING RETURNING event_id";
        Object[] args = new Object[entries.size() * 2];
        for (int i = 0; i < entries.size(); i++) {
            args[2 * i]     = entries.get(i).eventId();
            args[2 * i + 1] = entries.get(i).eventType();
        }
        return new HashSet<>(jdbc.queryForList(sql, UUID.class, args));
    }

    public record Entry(UUID eventId, String eventType) {}
}
