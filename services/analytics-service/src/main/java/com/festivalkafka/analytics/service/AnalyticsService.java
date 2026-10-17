package com.festivalkafka.analytics.service;

import com.festivalkafka.analytics.repo.LifecycleEventRepository;
import com.festivalkafka.analytics.repo.MetricsCountersRepository;
import com.festivalkafka.analytics.repo.SagaCountsRepository;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Writes the fact row, bumps the per-event-type aggregation counter, and (for
 * OrderCreated) adds the order total to the GMV counter. All atomic within one
 * transaction: a duplicate PK on lifecycle_events short-circuits before any
 * counter is touched.
 */
@Service
public class AnalyticsService {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsService.class);

    private final LifecycleEventRepository events;
    private final SagaCountsRepository counts;
    private final MetricsCountersRepository metrics;

    public AnalyticsService(LifecycleEventRepository events,
                             SagaCountsRepository counts,
                             MetricsCountersRepository metrics) {
        this.events = events;
        this.counts = counts;
        this.metrics = metrics;
    }

    /**
     * Record a lifecycle event. {@code gmvDeltaCents} is applied to the
     * {@code gmv_cents_total} counter only when non-zero (OrderCreated path).
     */
    @Transactional
    public void record(UUID eventId, UUID orderId, String eventType, Instant occurredAt, long gmvDeltaCents) {
        boolean inserted = events.insertIfAbsent(eventId, orderId, eventType, occurredAt);
        if (!inserted) {
            log.debug("duplicate event {} ({}) skipped", eventId, eventType);
            return;
        }
        counts.increment(eventType);
        if (gmvDeltaCents > 0L) {
            metrics.incrementBy(MetricsCountersRepository.GMV_CENTS_TOTAL, gmvDeltaCents);
        }
    }
}
