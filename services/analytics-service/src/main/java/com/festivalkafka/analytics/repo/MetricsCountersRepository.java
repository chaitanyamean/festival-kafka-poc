package com.festivalkafka.analytics.repo;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Flat key/value counter store (`metrics_counters` table). UPSERT keeps counters
 * current without a separate aggregation job.
 */
@Repository
public class MetricsCountersRepository {

    public static final String GMV_CENTS_TOTAL = "gmv_cents_total";

    private final JdbcTemplate jdbc;

    public MetricsCountersRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void incrementBy(String name, long delta) {
        jdbc.update(
                """
                INSERT INTO metrics_counters (name, value, updated_at)
                VALUES (?, ?, NOW())
                ON CONFLICT (name) DO UPDATE
                   SET value = metrics_counters.value + EXCLUDED.value,
                       updated_at = NOW()
                """,
                name, delta);
    }

    public long valueOf(String name) {
        Long v = jdbc.query(
                "SELECT value FROM metrics_counters WHERE name = ?",
                rs -> rs.next() ? rs.getLong(1) : 0L,
                name);
        return v == null ? 0L : v;
    }
}
