-- Seed data for Phase 1 demo + integration tests.
--
-- 1000 SKUs with ids SKU-00001..SKU-01000. Each starts with 10_000 units of
-- available stock (plenty for a 100 orders/s × 60 s demo = 6k orders × up to
-- 3 items/order = ~18k item units distributed across 1000 SKUs).
--
-- The load-generator picks SKUs uniformly from this range.

INSERT INTO skus (sku_id, name, available)
SELECT
    'SKU-' || LPAD(gs::text, 5, '0')      AS sku_id,
    'Festival widget ' || gs             AS name,
    10000                                 AS available
FROM generate_series(1, 1000) AS gs;
