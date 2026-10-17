package com.festivalkafka.notification.repo;

import com.festivalkafka.notification.domain.NotificationChannel;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class NotificationLogRepository {

    private final JdbcTemplate jdbc;

    public NotificationLogRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(UUID eventId,
                        UUID orderId,
                        String eventType,
                        NotificationChannel channel,
                        String message) {
        jdbc.update(
                """
                INSERT INTO notifications_log
                    (event_id, order_id, event_type, notification_channel, message)
                VALUES (?, ?, ?, ?, ?)
                """,
                eventId, orderId, eventType, channel.name(), message);
    }

    public int countByOrderId(UUID orderId) {
        return jdbc.queryForObject(
                "SELECT COUNT(*) FROM notifications_log WHERE order_id = ?",
                Integer.class,
                orderId);
    }
}
