package com.festivalkafka.loadgen.scenario;

import java.util.LinkedHashMap;
import java.util.Map;

/** Constant RPS for a configured duration. Equivalent to Phase 1 behaviour. */
public final class SteadyScenario implements Scenario {

    private final int rps;
    private final int durationSec;

    public SteadyScenario(int rps, int durationSec) {
        if (rps <= 0 || durationSec <= 0) {
            throw new IllegalArgumentException("rps and durationSec must be > 0");
        }
        this.rps = rps;
        this.durationSec = durationSec;
    }

    @Override public String name() { return "steady"; }
    @Override public double totalDurationSec() { return durationSec; }
    @Override public double rateAtSeconds(double tSec) { return rps; }
    @Override public PayloadPolicy payloadPolicy() { return PayloadPolicy.none(); }

    @Override
    public Map<String, Object> params() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("rps", rps);
        m.put("durationSec", durationSec);
        return m;
    }
}
