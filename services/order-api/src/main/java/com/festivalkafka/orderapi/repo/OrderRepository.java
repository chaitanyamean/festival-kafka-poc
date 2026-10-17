package com.festivalkafka.orderapi.repo;

import com.festivalkafka.orderapi.api.dto.ItemRequest;
import com.festivalkafka.orderapi.domain.Order;
import com.festivalkafka.orderapi.domain.OrderStatus;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class OrderRepository {

    private final JdbcTemplate jdbc;

    public OrderRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Inserts the order row and all item rows atomically. Called from OrderService;
     * runs in a new transaction so Kafka publishing happens strictly after DB commit
     * (see ADR-0004 for the dual-write trade-off this leaves open).
     */
    @Transactional
    public void insertNewOrder(
            UUID orderId,
            UUID customerId,
            long totalCents,
            String currency,
            OrderStatus status,
            OffsetDateTime createdAt,
            List<ItemRequest> items) {

        jdbc.update(
                """
                INSERT INTO orders
                    (order_id, customer_id, total_amount_cents, currency, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                orderId, customerId, totalCents, currency, status.name(), createdAt, createdAt);

        List<Object[]> itemRows = items.stream()
                .map(i -> new Object[]{orderId, i.skuId(), i.quantity(), i.unitPriceCents()})
                .toList();

        jdbc.batchUpdate(
                "INSERT INTO order_items (order_id, sku_id, quantity, unit_price_cents) VALUES (?, ?, ?, ?)",
                itemRows);
    }

    public Optional<Order> findById(UUID orderId) {
        try {
            Order o = jdbc.queryForObject(
                    """
                    SELECT order_id, customer_id, total_amount_cents, currency, status, created_at, updated_at
                    FROM orders WHERE order_id = ?
                    """,
                    orderRowMapper(),
                    orderId);
            return Optional.ofNullable(o);
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    /**
     * Transitions status only from PENDING. Guarantees first terminal event wins;
     * a late-arriving competing terminal event is a no-op.
     */
    public int transitionFromPending(UUID orderId, OrderStatus newStatus) {
        return jdbc.update(
                "UPDATE orders SET status = ?, updated_at = NOW() WHERE order_id = ? AND status = 'PENDING'",
                newStatus.name(), orderId);
    }

    /**
     * Variant that returns the fresh Order after a successful PENDING→newStatus
     * transition (so the caller has createdAt for an end-to-end latency metric).
     * Returns Optional.empty when no row was updated (duplicate / already terminal).
     */
    public Optional<Order> transitionFromPendingReturning(UUID orderId, OrderStatus newStatus) {
        int updated = transitionFromPending(orderId, newStatus);
        if (updated == 0) {
            return Optional.empty();
        }
        return findById(orderId);
    }

    private RowMapper<Order> orderRowMapper() {
        return (rs, i) -> new Order(
                rs.getObject("order_id", UUID.class),
                rs.getObject("customer_id", UUID.class),
                rs.getLong("total_amount_cents"),
                rs.getString("currency"),
                OrderStatus.valueOf(rs.getString("status")),
                rs.getObject("created_at", OffsetDateTime.class),
                rs.getObject("updated_at", OffsetDateTime.class));
    }
}
