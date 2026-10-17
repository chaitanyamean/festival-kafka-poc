package com.festivalkafka.payment.repo;

import com.festivalkafka.payment.domain.Payment;
import com.festivalkafka.payment.domain.PaymentStatus;
import java.util.Optional;
import java.util.UUID;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class PaymentRepository {

    private final JdbcTemplate jdbc;

    public PaymentRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(UUID paymentId, UUID orderId, long amountCents, String currency,
                        PaymentStatus status, String failureReason) {
        jdbc.update(
                """
                INSERT INTO payments
                    (payment_id, order_id, amount_cents, currency, status, failure_reason)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                paymentId, orderId, amountCents, currency, status.name(), failureReason);
    }

    public Optional<Payment> findByOrderId(UUID orderId) {
        try {
            Payment p = jdbc.queryForObject(
                    """
                    SELECT payment_id, order_id, amount_cents, currency, status, failure_reason, created_at
                      FROM payments WHERE order_id = ?
                    """,
                    paymentMapper(),
                    orderId);
            return Optional.ofNullable(p);
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    private RowMapper<Payment> paymentMapper() {
        return (rs, i) -> new Payment(
                rs.getObject("payment_id", UUID.class),
                rs.getObject("order_id", UUID.class),
                rs.getLong("amount_cents"),
                rs.getString("currency"),
                PaymentStatus.valueOf(rs.getString("status")),
                rs.getString("failure_reason"),
                rs.getObject("created_at", java.time.OffsetDateTime.class));
    }
}
