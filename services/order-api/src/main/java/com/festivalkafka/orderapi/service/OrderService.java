package com.festivalkafka.orderapi.service;

import com.festivalkafka.orderapi.api.dto.CreateOrderRequest;
import com.festivalkafka.orderapi.api.dto.OrderResponse;
import com.festivalkafka.orderapi.domain.Order;
import com.festivalkafka.orderapi.domain.OrderStatus;
import com.festivalkafka.orderapi.kafka.OrderEventPublisher;
import com.festivalkafka.orderapi.metrics.SagaMetrics;
import com.festivalkafka.orderapi.repo.OrderRepository;
import com.festivalkafka.orderapi.repo.ProcessedEventRepository;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Core use cases for the order-api service.
 *
 * <p>{@link #create(CreateOrderRequest)} is intentionally NOT @Transactional: the DB
 * write runs in {@link OrderRepository#insertNewOrder}'s own transaction and commits
 * before the Kafka publish is attempted. This is the dual-write gap called out in
 * ADR-0004; Phase 4 closes it with an outbox + relay.
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);
    private static final String DEFAULT_CURRENCY = "INR";

    private final OrderRepository orders;
    private final ProcessedEventRepository processed;
    private final OrderEventPublisher publisher;
    private final SagaMetrics sagaMetrics;
    private final Clock clock;

    public OrderService(
            OrderRepository orders,
            ProcessedEventRepository processed,
            OrderEventPublisher publisher,
            SagaMetrics sagaMetrics,
            Clock clock) {
        this.orders = orders;
        this.processed = processed;
        this.publisher = publisher;
        this.sagaMetrics = sagaMetrics;
        this.clock = clock;
    }

    public OrderResponse create(CreateOrderRequest req) {
        UUID orderId = UUID.randomUUID();
        long total = req.items().stream()
                .mapToLong(i -> Math.multiplyExact(i.unitPriceCents(), (long) i.quantity()))
                .sum();
        OffsetDateTime createdAt = OffsetDateTime.now(clock);

        // KNOWN-FLAW: dual write, fixed in phase 4.
        // The DB commit below is independent from the Kafka publish that follows.
        // A crash between the two loses the event. Phase 4 introduces a
        // transactional outbox + relay to close this gap.
        orders.insertNewOrder(
                orderId, req.customerId(), total, DEFAULT_CURRENCY,
                OrderStatus.PENDING, createdAt, req.items());

        publisher.publishOrderCreated(
                orderId, req.customerId(), total, DEFAULT_CURRENCY,
                req.items(), req.city(), req.paymentMethod());

        return new OrderResponse(
                orderId, req.customerId(), total, DEFAULT_CURRENCY, OrderStatus.PENDING, createdAt);
    }

    public Optional<Order> find(UUID orderId) {
        return orders.findById(orderId);
    }

    /**
     * Idempotent terminal transition for the happy path (PENDING → CONFIRMED).
     * Dedupe row and status update share a single transaction; a failure in either
     * rolls both back so redelivery replays the event.
     */
    @Transactional
    public void markConfirmedIdempotent(UUID orderId, UUID eventId, String eventType) {
        if (!processed.recordIfAbsent(eventId, eventType)) {
            log.debug("duplicate event {} ({}), skipping", eventId, eventType);
            return;
        }
        orders.transitionFromPendingReturning(orderId, OrderStatus.CONFIRMED)
                .ifPresentOrElse(
                        sagaMetrics::recordConfirmed,
                        () -> log.debug("order {} not in PENDING or missing; no transition applied", orderId));
    }

    /** Idempotent terminal transition for the failure path (PENDING → CANCELLED). */
    @Transactional
    public void markCancelledIdempotent(UUID orderId, UUID eventId, String eventType, String reason) {
        if (!processed.recordIfAbsent(eventId, eventType)) {
            log.debug("duplicate event {} ({}), skipping", eventId, eventType);
            return;
        }
        orders.transitionFromPendingReturning(orderId, OrderStatus.CANCELLED)
                .ifPresentOrElse(
                        o -> sagaMetrics.recordCancelled(o, reason),
                        () -> log.debug("order {} not in PENDING or missing; no transition applied", orderId));
    }
}
