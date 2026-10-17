package com.festivalkafka.loadgen.scenario;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Steady {@code baselineRps} for {@code baselineDurationSec}, then an instant jump
 * to {@code baselineRps * spikeMultiplier} for {@code spikeDurationSec}, optionally
 * followed by a cooldown back at baseline.
 *
 * <p>Simulates an Amazon Great Indian Festival-style flash event: traffic sitting
 * at steady state, a scheduled sale starts, orders surge, and we observe how the
 * saga handles the sudden multiplication.
 */
public final class FestivalSpikeScenario implements Scenario {

    private final int baselineRps;
    private final double spikeMultiplier;
    private final int baselineDurationSec;
    private final int spikeDurationSec;
    private final int cooldownDurationSec;

    public FestivalSpikeScenario(
            int baselineRps,
            double spikeMultiplier,
            int baselineDurationSec,
            int spikeDurationSec,
            int cooldownDurationSec) {
        if (baselineRps <= 0 || baselineDurationSec <= 0 || spikeDurationSec <= 0) {
            throw new IllegalArgumentException("rps and both durations must be > 0");
        }
        if (spikeMultiplier <= 0) {
            throw new IllegalArgumentException("spikeMultiplier must be > 0");
        }
        if (cooldownDurationSec < 0) {
            throw new IllegalArgumentException("cooldownDurationSec must be >= 0");
        }
        this.baselineRps = baselineRps;
        this.spikeMultiplier = spikeMultiplier;
        this.baselineDurationSec = baselineDurationSec;
        this.spikeDurationSec = spikeDurationSec;
        this.cooldownDurationSec = cooldownDurationSec;
    }

    @Override public String name() { return "festival-spike"; }
    @Override public double totalDurationSec() { return baselineDurationSec + spikeDurationSec + cooldownDurationSec; }

    @Override
    public double rateAtSeconds(double tSec) {
        if (tSec < baselineDurationSec) return baselineRps;
        if (tSec < baselineDurationSec + spikeDurationSec) return baselineRps * spikeMultiplier;
        return baselineRps;
    }

    @Override public PayloadPolicy payloadPolicy() { return PayloadPolicy.none(); }

    @Override
    public Map<String, Object> params() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("baselineRps", baselineRps);
        m.put("spikeMultiplier", spikeMultiplier);
        m.put("baselineDurationSec", baselineDurationSec);
        m.put("spikeDurationSec", spikeDurationSec);
        m.put("cooldownDurationSec", cooldownDurationSec);
        return m;
    }
}
