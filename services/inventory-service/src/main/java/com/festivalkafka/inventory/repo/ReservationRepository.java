package com.festivalkafka.inventory.repo;

import com.festivalkafka.inventory.domain.Reservation;
import com.festivalkafka.inventory.domain.ReservationItem;
import com.festivalkafka.inventory.domain.ReservationStatus;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class ReservationRepository {

    private final JdbcTemplate jdbc;

    public ReservationRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insertReservation(UUID reservationId, UUID orderId, ReservationStatus status) {
        jdbc.update(
                "INSERT INTO reservations (reservation_id, order_id, status) VALUES (?, ?, ?)",
                reservationId, orderId, status.name());
    }

    public void insertItems(UUID reservationId, List<ReservationItem> items) {
        List<Object[]> rows = items.stream()
                .map(i -> new Object[]{reservationId, i.skuId(), i.quantity()})
                .toList();
        jdbc.batchUpdate(
                "INSERT INTO reservation_items (reservation_id, sku_id, quantity) VALUES (?, ?, ?)",
                rows);
    }

    public Optional<Reservation> findByOrderId(UUID orderId) {
        try {
            Reservation r = jdbc.queryForObject(
                    """
                    SELECT reservation_id, order_id, status, created_at, updated_at
                      FROM reservations WHERE order_id = ?
                    """,
                    reservationMapper(),
                    orderId);
            return Optional.ofNullable(r);
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public List<ReservationItem> findItems(UUID reservationId) {
        return jdbc.query(
                "SELECT sku_id, quantity FROM reservation_items WHERE reservation_id = ?",
                (rs, i) -> new ReservationItem(rs.getString("sku_id"), rs.getInt("quantity")),
                reservationId);
    }

    /**
     * Guarded transition RESERVED -> RELEASED. Idempotent: a second call for the same
     * reservation returns 0 rows affected and the caller skips the stock-restore step.
     */
    public int transitionToReleased(UUID reservationId) {
        return jdbc.update(
                "UPDATE reservations SET status = 'RELEASED', updated_at = NOW() WHERE reservation_id = ? AND status = 'RESERVED'",
                reservationId);
    }

    private RowMapper<Reservation> reservationMapper() {
        return (rs, i) -> new Reservation(
                rs.getObject("reservation_id", UUID.class),
                rs.getObject("order_id", UUID.class),
                ReservationStatus.valueOf(rs.getString("status")),
                rs.getObject("created_at", java.time.OffsetDateTime.class),
                rs.getObject("updated_at", java.time.OffsetDateTime.class));
    }
}
