package com.festivalkafka.loadgen.scenario;

import java.util.List;
import java.util.Map;

/**
 * Pacing + payload shaping policy for one load run.
 *
 * <p>The main loop in {@code LoadGenerator} calls {@link #rateAtSeconds(double)}
 * at each tick to decide how fast to emit the next request, and uses
 * {@link #payloadPolicy()} to let the payload builder know whether to bias SKU
 * selection (e.g. the "lightning-deal" scenario).
 *
 * <p>Scenarios are stateless w.r.t. the main loop; all elapsed-time logic lives
 * in {@code rateAtSeconds}.
 */
public interface Scenario {

    /** Short machine name for the scenario (goes into the JSON report). */
    String name();

    /** Total wall-clock duration of the run, in seconds. */
    double totalDurationSec();

    /**
     * Target emission rate at {@code tSec} seconds since start. Must stay &gt; 0
     * until {@link #totalDurationSec()}.
     */
    double rateAtSeconds(double tSec);

    /** Payload shaping policy: normal random SKUs, or biased toward a hot set. */
    PayloadPolicy payloadPolicy();

    /** Scenario-specific knobs, dumped verbatim into the JSON report for traceability. */
    Map<String, Object> params();

    /** Payload shaping. {@code hotSkus} empty = no bias (default). */
    record PayloadPolicy(List<String> hotSkus, double hotShare) {
        public static PayloadPolicy none() {
            return new PayloadPolicy(List.of(), 0.0);
        }
    }
}
