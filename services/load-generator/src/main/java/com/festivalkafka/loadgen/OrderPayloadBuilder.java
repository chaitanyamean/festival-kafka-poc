package com.festivalkafka.loadgen;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.festivalkafka.loadgen.scenario.Scenario.PayloadPolicy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Builds a JSON body that matches order-api's {@code CreateOrderRequest}.
 *
 * <ul>
 *   <li>SKUs are drawn uniformly from {@code SKU-00001 .. SKU-<skuCount>}, matching
 *       the 1 000-row seed in inventory-service's V2 migration.</li>
 *   <li>With probability {@code badSkuRate}, one item in the order carries a SKU
 *       that will NOT exist in the catalog, forcing the inventory-fail compensation
 *       path to be exercised during the demo.</li>
 *   <li>City picked uniformly from a fixed list of Indian metros; payment method
 *       picked uniformly from the five supported methods.</li>
 * </ul>
 */
public final class OrderPayloadBuilder {

    private static final List<String> CITIES = List.of(
            "Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Chennai",
            "Kolkata", "Pune", "Ahmedabad", "Jaipur", "Lucknow",
            "Surat", "Nagpur", "Indore", "Thane", "Visakhapatnam");

    private static final List<String> PAYMENT_METHODS = List.of(
            "CARD", "UPI", "NETBANKING", "COD", "WALLET");

    private final ObjectMapper mapper = new ObjectMapper();
    private final int maxItemsPerOrder;
    private final int skuCount;
    private final double badSkuRate;
    private final PayloadPolicy payloadPolicy;

    public OrderPayloadBuilder(int maxItemsPerOrder, int skuCount, double badSkuRate) {
        this(maxItemsPerOrder, skuCount, badSkuRate, PayloadPolicy.none());
    }

    public OrderPayloadBuilder(int maxItemsPerOrder, int skuCount, double badSkuRate, PayloadPolicy payloadPolicy) {
        if (maxItemsPerOrder < 1) {
            throw new IllegalArgumentException("maxItemsPerOrder must be >= 1");
        }
        if (skuCount < 1) {
            throw new IllegalArgumentException("skuCount must be >= 1");
        }
        if (badSkuRate < 0.0 || badSkuRate > 1.0) {
            throw new IllegalArgumentException("badSkuRate must be in [0.0, 1.0]");
        }
        this.maxItemsPerOrder = maxItemsPerOrder;
        this.skuCount = skuCount;
        this.badSkuRate = badSkuRate;
        this.payloadPolicy = payloadPolicy != null ? payloadPolicy : PayloadPolicy.none();
    }

    public byte[] buildBody() {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        int itemCount = 1 + r.nextInt(maxItemsPerOrder);
        boolean badOrder = r.nextDouble() < badSkuRate;
        List<String> hotSkus = payloadPolicy.hotSkus();
        double hotShare      = payloadPolicy.hotShare();
        boolean biased       = !hotSkus.isEmpty() && hotShare > 0.0;

        List<Map<String, Object>> items = new ArrayList<>(itemCount);
        for (int i = 0; i < itemCount; i++) {
            String skuId;
            if (badOrder && i == 0) {
                // Guaranteed-missing SKU in the first item slot; the other items
                // (if any) stay valid so the test still looks like a real order.
                skuId = "SKU-BAD-" + r.nextInt(100000);
            } else if (biased && r.nextDouble() < hotShare) {
                skuId = hotSkus.get(r.nextInt(hotSkus.size()));
            } else {
                skuId = String.format("SKU-%05d", 1 + r.nextInt(skuCount));
            }
            items.add(Map.of(
                    "skuId",          skuId,
                    "quantity",       1 + r.nextInt(3),
                    "unitPriceCents", 5_000L + r.nextLong(25_000)
            ));
        }

        Map<String, Object> body = Map.of(
                "customerId",    UUID.randomUUID().toString(),
                "items",         items,
                "city",          CITIES.get(r.nextInt(CITIES.size())),
                "paymentMethod", PAYMENT_METHODS.get(r.nextInt(PAYMENT_METHODS.size()))
        );
        try {
            return mapper.writeValueAsBytes(body);
        } catch (Exception e) {
            throw new IllegalStateException("JSON serialization failed", e);
        }
    }
}
