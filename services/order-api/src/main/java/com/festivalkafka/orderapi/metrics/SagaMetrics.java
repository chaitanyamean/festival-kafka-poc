package com.festivalkafka.orderapi.metrics;

import com.festivalkafka.orderapi.domain.Order;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Objects;
import org.springframework.stereotype.Component;

/**
 * Application-level Micrometer metrics for Phase 2 observability.
 *
 * <ul>
 *   <li>{@code fkp.saga.outcome} counter — one bump per terminal transition.
 *       Tags: {@code outcome=confirmed|cancelled}, {@code reason=<short>}.</li>
 *   <li>{@code fkp.order.e2e.latency} timer — created_at → first terminal event.
 *       Tags: {@code outcome=confirmed|cancelled}.</li>
 * </ul>
 *
 * The emitter is central so dashboards can rely on consistent tag names.
 */
@Component
public class SagaMetrics {

    public static final String CONFIRMED = "confirmed";
    public static final String CANCELLED = "cancelled";
    public static final String REASON_SHIPPED = "shipment_created";

    private final MeterRegistry registry;
    private final Clock clock;

    public SagaMetrics(MeterRegistry registry, Clock clock) {
        this.registry = registry;
        this.clock = clock;
    }

    public void recordConfirmed(Order order) {
        recordOutcome(order, CONFIRMED, REASON_SHIPPED);
    }

    public void recordCancelled(Order order, String reason) {
        recordOutcome(order, CANCELLED, normalize(reason));
    }

    private void recordOutcome(Order order, String outcome, String reason) {
        Objects.requireNonNull(order, "order");

        Counter.builder("fkp.saga.outcome")
                .description("Terminal transitions per outcome and reason")
                .tag("outcome", outcome)
                .tag("reason", reason)
                .register(registry)
                .increment();

        OffsetDateTime createdAt = order.createdAt();
        if (createdAt != null) {
            Duration latency = Duration.between(createdAt, OffsetDateTime.now(clock));
            if (!latency.isNegative()) {
                Timer.builder("fkp.order.e2e.latency")
                        .description("End-to-end latency from order.created to first terminal event")
                        .tag("outcome", outcome)
                        .publishPercentileHistogram()
                        .register(registry)
                        .record(latency);
            }
        }
    }

    /** Normalize free-form reason strings into bounded, dashboard-friendly tag values. */
    static String normalize(String reason) {
        if (reason == null || reason.isBlank()) return "unknown";
        String r = reason.trim().toLowerCase();
        // Known reasons emitted by inventory-service / payment-service.
        if (r.contains("inventory") && r.contains("fail")) return "inventory_failed";
        if (r.contains("payment")   && r.contains("fail")) return "payment_failed";
        if (r.contains("sku"))                             return "inventory_failed";
        // Last-resort: alpha-num + underscore, cap at 32 chars to avoid high-cardinality churn.
        String cleaned = r.replaceAll("[^a-z0-9]+", "_").replaceAll("^_+|_+$", "");
        if (cleaned.isEmpty()) return "unknown";
        return cleaned.substring(0, Math.min(cleaned.length(), 32));
    }
}
