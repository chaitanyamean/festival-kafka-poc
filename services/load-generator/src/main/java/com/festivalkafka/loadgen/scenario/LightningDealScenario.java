package com.festivalkafka.loadgen.scenario;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Steady {@code rps}, but {@code hotSkuShare} fraction of each order's items is
 * taken from a short "hot" SKU list rather than the uniform 1..skuCount pool.
 * Default {@code hotShare = 0.8}, i.e. 80% of items collide on 3 SKUs — this
 * exercises row-level contention in inventory-service's reservation path.
 */
public final class LightningDealScenario implements Scenario {

    private final int rps;
    private final int durationSec;
    private final List<String> hotSkus;
    private final double hotShare;

    public LightningDealScenario(int rps, int durationSec, List<String> hotSkus, double hotShare) {
        if (rps <= 0 || durationSec <= 0) {
            throw new IllegalArgumentException("rps and durationSec must be > 0");
        }
        if (hotSkus == null || hotSkus.isEmpty()) {
            throw new IllegalArgumentException("hotSkus must be non-empty");
        }
        if (hotShare < 0 || hotShare > 1) {
            throw new IllegalArgumentException("hotShare must be in [0,1]");
        }
        this.rps = rps;
        this.durationSec = durationSec;
        this.hotSkus = List.copyOf(hotSkus);
        this.hotShare = hotShare;
    }

    @Override public String name() { return "lightning-deal"; }
    @Override public double totalDurationSec() { return durationSec; }
    @Override public double rateAtSeconds(double tSec) { return rps; }
    @Override public PayloadPolicy payloadPolicy() { return new PayloadPolicy(hotSkus, hotShare); }

    @Override
    public Map<String, Object> params() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("rps", rps);
        m.put("durationSec", durationSec);
        m.put("hotSkus", hotSkus);
        m.put("hotShare", hotShare);
        return m;
    }
}
