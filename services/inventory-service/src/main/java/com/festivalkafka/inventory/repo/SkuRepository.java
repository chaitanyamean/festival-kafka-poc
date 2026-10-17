package com.festivalkafka.inventory.repo;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Stock mutations. All methods are lock-free conditional updates: callers inspect the
 * returned row count to detect "insufficient stock" rather than throwing. Phase 5 will
 * move the hot path for popular SKUs behind Redis atomic DECRBY.
 */
@Repository
public class SkuRepository {

    private final JdbcTemplate jdbc;

    public SkuRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Atomically decrements available stock iff enough is on hand. Also bumps the
     * reserved counter by the same amount so the aggregate skus.available + skus.reserved
     * stays an invariant of the catalog.
     *
     * @return 1 if the decrement was applied, 0 if the SKU is missing or insufficient.
     */
    public int decrementAvailable(String skuId, int qty) {
        return jdbc.update(
                """
                UPDATE skus
                   SET available = available - ?,
                       reserved  = reserved  + ?,
                       updated_at = NOW()
                 WHERE sku_id = ?
                   AND available >= ?
                """,
                qty, qty, skuId, qty);
    }

    /**
     * Reverse of {@link #decrementAvailable}: used both to compensate a partial-failure
     * reservation attempt and to release stock on a confirmed cancellation.
     */
    public int incrementAvailable(String skuId, int qty) {
        return jdbc.update(
                """
                UPDATE skus
                   SET available = available + ?,
                       reserved  = GREATEST(reserved - ?, 0),
                       updated_at = NOW()
                 WHERE sku_id = ?
                """,
                qty, qty, skuId);
    }
}
