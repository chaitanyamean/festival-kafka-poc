package com.festivalkafka.loadgen;

import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Lock-free counters + latency samples for the load run. End-of-run summary sorts the
 * sampled latencies and reports p50/p95/p99/max. {@code ConcurrentLinkedQueue<Long>}
 * is fine for the Phase 1 target (≤ 2k rps × 60s = 120k samples ≈ ~1 MB).
 */
public final class Metrics {

    private final LongAdder sent        = new LongAdder();
    private final LongAdder success2xx  = new LongAdder();
    private final LongAdder client4xx   = new LongAdder();
    private final LongAdder server5xx   = new LongAdder();
    private final LongAdder errors      = new LongAdder();
    private final AtomicLong startNanos = new AtomicLong();
    private final AtomicLong endNanos   = new AtomicLong();
    private final ConcurrentLinkedQueue<Long> latenciesMicros = new ConcurrentLinkedQueue<>();

    public void markStart() { startNanos.set(System.nanoTime()); }
    public void markEnd()   { endNanos.set(System.nanoTime()); }

    public void onSend() { sent.increment(); }

    public void onResponse(int status, long latencyNanos) {
        if (status >= 200 && status < 300)      success2xx.increment();
        else if (status >= 400 && status < 500) client4xx.increment();
        else if (status >= 500)                 server5xx.increment();
        latenciesMicros.add(latencyNanos / 1_000);
    }

    public void onError() { errors.increment(); }

    public Summary summarize(int targetRps, int durationSec) {
        long elapsedNanos = endNanos.get() - startNanos.get();
        double elapsedSec = elapsedNanos / 1_000_000_000.0;
        long totalSent = sent.sum();
        long total2xx  = success2xx.sum();
        long total4xx  = client4xx.sum();
        long total5xx  = server5xx.sum();
        long totalErr  = errors.sum();

        long[] sortedMicros = latenciesMicros.stream()
                .mapToLong(Long::longValue)
                .sorted()
                .toArray();

        double p50 = percentileMs(sortedMicros, 50);
        double p95 = percentileMs(sortedMicros, 95);
        double p99 = percentileMs(sortedMicros, 99);
        double max = sortedMicros.length == 0 ? 0.0 : sortedMicros[sortedMicros.length - 1] / 1_000.0;

        double achievedRps = elapsedSec == 0 ? 0 : totalSent / elapsedSec;
        double errorRate   = totalSent == 0 ? 0 : (totalErr + total4xx + total5xx) * 100.0 / totalSent;

        return new Summary(targetRps, durationSec, elapsedSec, totalSent, total2xx,
                total4xx, total5xx, totalErr, achievedRps, errorRate, p50, p95, p99, max);
    }

    private static double percentileMs(long[] sortedMicros, int percentile) {
        if (sortedMicros.length == 0) return 0.0;
        int idx = Math.min(sortedMicros.length - 1, (int) Math.ceil(sortedMicros.length * percentile / 100.0) - 1);
        return sortedMicros[Math.max(idx, 0)] / 1_000.0;
    }

    public record Summary(
            int targetRps,
            int durationSec,
            double elapsedSec,
            long totalSent,
            long total2xx,
            long total4xx,
            long total5xx,
            long totalErrors,
            double achievedRps,
            double errorRatePercent,
            double p50Ms,
            double p95Ms,
            double p99Ms,
            double maxMs) {

        public String format() {
            return """
                    ================= Load generator summary =================
                     Target RPS       : %d
                     Duration (cfg)   : %ds
                     Elapsed (actual) : %.2fs
                     Sent             : %d
                     2xx              : %d  (%.2f%%)
                     4xx              : %d
                     5xx              : %d
                     Errors           : %d
                     Achieved RPS     : %.2f
                     Error rate       : %.2f%%
                     Latency ms       : p50=%.1f  p95=%.1f  p99=%.1f  max=%.1f
                    ==========================================================
                    """.formatted(
                    targetRps, durationSec, elapsedSec,
                    totalSent, total2xx,
                    totalSent == 0 ? 0.0 : total2xx * 100.0 / totalSent,
                    total4xx, total5xx, totalErrors,
                    achievedRps, errorRatePercent,
                    p50Ms, p95Ms, p99Ms, maxMs);
        }
    }
}
