package com.festivalkafka.loadgen;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.festivalkafka.loadgen.scenario.Scenario;
import com.festivalkafka.loadgen.scenario.ScenarioLoader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

/**
 * POSTs orders to {@code <host>/orders} following a {@link Scenario} that controls
 * both pacing (RPS over time) and payload shape (SKU bias). One virtual thread
 * per in-flight request; the main loop uses {@link LockSupport#parkNanos} to pace
 * emission at {@code scenario.rateAtSeconds(tSec)} on each tick.
 *
 * <p>CLI (common flags; scenario-specific flags are documented on each Scenario impl):
 * <pre>
 *   --host             order-api base URL              (default: http://localhost:8080)
 *   --scenario         steady|ramp|festival-spike|lightning-deal  (default: steady)
 *   --scenario-file    optional YAML with named profiles          (default: classpath:scenarios.yaml)
 *   --profile          named profile from --scenario-file         (overridden by scenario-specific CLI flags)
 *   --report-json      write JSON report to this host path        (default: none)
 *   --max-items        max items per order             (default: 3)
 *   --sku-count        upper bound of the SKU id range (default: 1000)
 *   --bad-sku-rate     probability [0..1] that an order carries one missing SKU (default: 0.0)
 *   --timeout-ms       per-request timeout             (default: 5000)
 * </pre>
 */
public final class LoadGenerator {

    public static void main(String[] args) throws Exception {
        Map<String, String> parsed = parseArgs(args);

        String host     = parsed.getOrDefault("--host", "http://localhost:8080");
        int maxItems    = Integer.parseInt(parsed.getOrDefault("--max-items",    "3"));
        int skuCount    = Integer.parseInt(parsed.getOrDefault("--sku-count",    "1000"));
        double badSku   = Double.parseDouble(parsed.getOrDefault("--bad-sku-rate", "0.0"));
        int timeoutMs   = Integer.parseInt(parsed.getOrDefault("--timeout-ms",   "5000"));
        String reportPath = parsed.get("--report-json");

        Scenario scenario = ScenarioLoader.resolve(parsed);

        URI target = URI.create(host.endsWith("/") ? host + "orders" : host + "/orders");

        HttpClient http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        OrderPayloadBuilder payloads =
                new OrderPayloadBuilder(maxItems, skuCount, badSku, scenario.payloadPolicy());
        Metrics metrics = new Metrics();

        System.out.printf(
                "Starting load: scenario=%s target=%s params=%s%n   defaults: maxItems=%d skuCount=%d badSkuRate=%.3f%n",
                scenario.name(), target, scenario.params(), maxItems, skuCount, badSku);

        double totalDurationSec = scenario.totalDurationSec();
        long startNanos    = System.nanoTime();
        long deadlineNanos = startNanos + (long) (totalDurationSec * 1_000_000_000L);
        Instant startedAt  = Instant.now();

        metrics.markStart();
        try (var executor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("load-", 0).factory())) {
            // Scheduler loop: at each iteration, recompute the current rate (which
            // may vary per scenario) and pace the next emission to 1/rate seconds
            // after the previous tick. This gives smooth ramps and clean steps.
            long nextTickNanos = startNanos;
            while (true) {
                long now = System.nanoTime();
                if (now >= deadlineNanos) break;

                long sleep = nextTickNanos - now;
                if (sleep > 0) LockSupport.parkNanos(sleep);

                double tSec = (nextTickNanos - startNanos) / 1_000_000_000.0;
                double rate = Math.max(scenario.rateAtSeconds(tSec), 1.0);
                long gapNanos = (long) (1_000_000_000.0 / rate);

                metrics.onSend();
                byte[] body = payloads.buildBody();
                executor.submit(() -> sendOne(http, target, body, timeoutMs, metrics));

                nextTickNanos += gapNanos;
            }
        }
        metrics.markEnd();
        Instant endedAt = Instant.now();

        Metrics.Summary summary = metrics.summarize((int) scenario.rateAtSeconds(0), (int) totalDurationSec);
        System.out.println();
        System.out.println(summary.format());

        if (reportPath != null) {
            writeJsonReport(reportPath, scenario, host, startedAt, endedAt, summary);
        }
    }

    private static void sendOne(HttpClient http, URI target, byte[] body, int timeoutMs, Metrics metrics) {
        long sendNanos = System.nanoTime();
        HttpRequest req = HttpRequest.newBuilder(target)
                .timeout(Duration.ofMillis(timeoutMs))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        try {
            HttpResponse<Void> resp = http.send(req, HttpResponse.BodyHandlers.discarding());
            metrics.onResponse(resp.statusCode(), System.nanoTime() - sendNanos);
        } catch (Exception e) {
            metrics.onError();
        }
    }

    private static void writeJsonReport(
            String path,
            Scenario scenario,
            String host,
            Instant startedAt,
            Instant endedAt,
            Metrics.Summary s) {
        try {
            ObjectMapper mapper = new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                    .enable(SerializationFeature.INDENT_OUTPUT);

            Report report = new Report(
                    scenario.name(),
                    scenario.params(),
                    host,
                    startedAt,
                    endedAt,
                    s.elapsedSec(),
                    s.totalSent(),
                    new Report.Status(s.total2xx(), s.total4xx(), s.total5xx(), s.totalErrors()),
                    s.achievedRps(),
                    s.errorRatePercent(),
                    new Report.LatencyMs(s.p50Ms(), s.p95Ms(), s.p99Ms(), s.maxMs()));

            byte[] json = mapper.writeValueAsBytes(report);
            Files.write(Path.of(path), json);
            System.out.printf(" JSON report: %s%n", path);
        } catch (Exception e) {
            System.err.printf("WARN: failed to write JSON report to %s: %s%n", path, e.getMessage());
        }
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> out = new HashMap<>();
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].startsWith("--")) {
                out.put(args[i], args[i + 1]);
                i++;
            }
        }
        return out;
    }
}
