package com.festivalkafka.analytics.repo;

import com.festivalkafka.analytics.domain.SagaCount;
import java.time.OffsetDateTime;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class SagaCountsRepository {

    private final JdbcTemplate jdbc;

    public SagaCountsRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * UPSERT: either increment the existing counter or create a new row at 1. Keeps
     * saga_counts current for the demo's completion-rate query without a separate
     * aggregation job.
     */
    public void increment(String eventType) {
        jdbc.update(
                """
                INSERT INTO saga_counts (event_type, event_count, updated_at)
                VALUES (?, 1, NOW())
                ON CONFLICT (event_type) DO UPDATE
                   SET event_count = saga_counts.event_count + 1,
                       updated_at  = NOW()
                """,
                eventType);
    }

    public List<SagaCount> findAll() {
        return jdbc.query(
                "SELECT event_type, event_count, updated_at FROM saga_counts ORDER BY event_type",
                sagaCountMapper());
    }

    private RowMapper<SagaCount> sagaCountMapper() {
        return (rs, i) -> new SagaCount(
                rs.getString("event_type"),
                rs.getLong("event_count"),
                rs.getObject("updated_at", OffsetDateTime.class));
    }
}
