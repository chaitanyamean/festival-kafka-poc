package com.festivalkafka.shipping.repo;

import com.festivalkafka.shipping.domain.Shipment;
import java.util.Optional;
import java.util.UUID;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class ShipmentRepository {

    private final JdbcTemplate jdbc;

    public ShipmentRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(UUID shipmentId, UUID orderId, String carrier, String trackingNumber) {
        jdbc.update(
                """
                INSERT INTO shipments (shipment_id, order_id, carrier, tracking_number)
                VALUES (?, ?, ?, ?)
                """,
                shipmentId, orderId, carrier, trackingNumber);
    }

    /**
     * Phase 3 batch insert. Delegates to the JDBC driver's batch pipeline (one
     * network round-trip for N rows, assuming reWriteBatchedInserts is on in the
     * JDBC URL — see docker-compose.yml). ORDER MATTERS ONLY as far as the
     * underlying driver preserves per-call positions for exception reporting.
     */
    public void batchInsert(java.util.List<Row> rows) {
        if (rows == null || rows.isEmpty()) return;
        jdbc.batchUpdate(
                "INSERT INTO shipments (shipment_id, order_id, carrier, tracking_number) VALUES (?, ?, ?, ?)",
                rows.stream()
                        .map(r -> new Object[]{r.shipmentId(), r.orderId(), r.carrier(), r.trackingNumber()})
                        .toList());
    }

    public record Row(UUID shipmentId, UUID orderId, String carrier, String trackingNumber) {}

    public Optional<Shipment> findByOrderId(UUID orderId) {
        try {
            Shipment s = jdbc.queryForObject(
                    """
                    SELECT shipment_id, order_id, carrier, tracking_number, created_at
                      FROM shipments WHERE order_id = ?
                    """,
                    shipmentMapper(),
                    orderId);
            return Optional.ofNullable(s);
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    private RowMapper<Shipment> shipmentMapper() {
        return (rs, i) -> new Shipment(
                rs.getObject("shipment_id", UUID.class),
                rs.getObject("order_id", UUID.class),
                rs.getString("carrier"),
                rs.getString("tracking_number"),
                rs.getObject("created_at", java.time.OffsetDateTime.class));
    }
}
