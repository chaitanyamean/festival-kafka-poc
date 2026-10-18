package com.festivalkafka.shipping.repo;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

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

    /**
     * Phase 3 batch form: one INSERT round-trip for N candidates, returning the set
     * of event IDs that were newly recorded (i.e. the ones that must be processed).
     * Duplicates are silently dropped by the ON CONFLICT clause and omitted from the
     * returned set.
     */
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
