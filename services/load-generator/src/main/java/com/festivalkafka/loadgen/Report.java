package com.festivalkafka.loadgen;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.time.Instant;
import java.util.Map;

/**
 * End-of-run report. Serializable to pretty JSON for machine consumption; the
 * human-readable table on stdout is produced separately by {@link Metrics.Summary#format()}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "scenario", "scenarioParams", "host", "startedAt", "endedAt", "elapsedSec",
    "sent", "status", "achievedRps", "errorRatePct", "latencyMs"
})
public record Report(
        String scenario,
        Map<String, Object> scenarioParams,
        String host,
        Instant startedAt,
        Instant endedAt,
        double elapsedSec,
        long sent,
        Status status,
        double achievedRps,
        double errorRatePct,
        LatencyMs latencyMs) {

    public record Status(
            @JsonProperty("2xx") long twoXx,
            @JsonProperty("4xx") long fourXx,
            @JsonProperty("5xx") long fiveXx,
            @JsonProperty("errors") long errors) {}

    public record LatencyMs(double p50, double p95, double p99, double max) {}
}
