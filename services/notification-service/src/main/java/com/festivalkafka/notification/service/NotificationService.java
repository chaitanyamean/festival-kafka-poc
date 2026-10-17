package com.festivalkafka.notification.service;

import com.festivalkafka.notification.domain.NotificationChannel;
import com.festivalkafka.notification.repo.NotificationLogRepository;
import com.festivalkafka.notification.repo.ProcessedEventRepository;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Phase 1 notification sink: dedupes by eventId then records a short message in the
 * notifications_log. "Sending" is simulated — a real integration would push to an
 * email/SMS/push gateway here.
 */
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final NotificationLogRepository notifications;
    private final ProcessedEventRepository processed;

    public NotificationService(NotificationLogRepository notifications,
                                ProcessedEventRepository processed) {
        this.notifications = notifications;
        this.processed = processed;
    }

    @Transactional
    public void record(UUID eventId,
                        UUID orderId,
                        String eventType,
                        NotificationChannel channel,
                        String message) {
        if (!processed.recordIfAbsent(eventId, eventType)) {
            log.debug("duplicate event {} ({}) skipped", eventId, eventType);
            return;
        }
        notifications.insert(eventId, orderId, eventType, channel, message);
        log.debug("notification queued via {}: {}", channel, message);
    }
}
