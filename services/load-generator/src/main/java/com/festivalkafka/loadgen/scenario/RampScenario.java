package com.festivalkafka.loadgen.scenario;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Linear ramp from {@code fromRps} to {@code toRps} across {@code durationSec} seconds.
 * Interpolation is continuous (per-tick); no step quantisation.
 */
public final class RampScenario implements Scenario {

    private final int fromRps;
    private final int toRps;
    private final int durationSec;

    public RampScenario(int fromRps, int toRps, int durationSec) {
        if (fromRps <= 0 || toRps <= 0 || durationSec <= 0) {
            throw new IllegalArgumentException("fromRps, toRps and durationSec must be > 0");
        }
        this.fromRps = fromRps;
        this.toRps = toRps;
        this.durationSec = durationSec;
    }

    @Override public String name() { return "ramp"; }
    @Override public double totalDurationSec() { return durationSec; }

    @Override
    public double rateAtSeconds(double tSec) {
        double clamped = Math.min(Math.max(tSec, 0.0), durationSec);
        double frac = clamped / durationSec;
        return fromRps + (toRps - fromRps) * frac;
    }

    @Override public PayloadPolicy payloadPolicy() { return PayloadPolicy.none(); }

    @Override
    public Map<String, Object> params() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("fromRps", fromRps);
        m.put("toRps", toRps);
        m.put("durationSec", durationSec);
        return m;
    }
}
