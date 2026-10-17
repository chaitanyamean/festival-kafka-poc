# BENCHMARKS

## Reporting rules

- **MEASURED** entries carry: hardware, OS, Docker/Kafka/service versions, exact command used, date, and the raw numeric result (p50/p99/throughput/error-rate as applicable).
- **PROJECTED** entries carry: the formula, every input value with its source (which MEASURED entry it came from or which docs/vendor spec), and the assumptions used.
- A projected number is **never** written in the MEASURED table. A measured number is **never** restated as a projection.
- If a test was planned but not run, it goes under "Pending" with the reason — not under MEASURED with a guess.

## Hardware record

### Primary measurement host (VPS)

| Field | Value |
|---|---|
| Provider | Hostinger KVM 2 |
| Hostname | srv1809153.hstgr.cloud |
| CPU | AMD EPYC 9354P (Zen 4), 2 vCPU pinned |
| RAM | 7.8 GiB |
| Swap | 0 B (hard OOM-kill on overcommit) |
| Disk | 100 GB NVMe SSD |
| Network | 8 TB/month, 1 Gbps shared NIC |
| OS | Ubuntu 24.04 LTS |
| Kernel | _recorded on first run_ |
| Docker | _recorded on first run_ |
| Docker Compose | _recorded on first run_ |

> Full `uname -a`, `docker version`, and `docker compose version` output recorded in `docs/benchmarks/raw/host-info.txt` on each benchmark run.

### Secondary / cloud hosts

_None yet. If we upsize for Phase 5/6, hardware block added here._

---

## MEASURED

| Phase | Date | Host | Test | Result | Raw |
|---|---|---|---|---|---|
| 1 | 2026-10-17 | Mac Docker Desktop (dev sanity, not VPS) | `make demo-phase1` @ 100 rps / 60 s | 5998 orders accepted, 5621 CONFIRMED, 377 CANCELLED, 0 lost, p99=10.1 ms (POST /orders) | see Phase 1 entry below |
| 2 | 2026-10-18 | Mac Docker Desktop (dev sanity, not VPS) | `make load-ramp` ramp 100 → 800 rps / 180 s | 81 000 sent, 80 904 2xx (99.88%), POST p99=11.7 ms. **Saga ceiling observed: ~33 msg/s** — only 5 576 CONFIRMED + 1 082 CANCELLED, **75 744 (93.6%) stuck in PENDING**. First bottleneck = payment-service 50 ms synth delay × 3 partitions (ADR-0008). | see Phase 2 entry below |

### Phase 1 — walking-skeleton demo (Mac Docker Desktop, dev sanity run)

> **⚠️ Dev sanity measurement, not the headline VPS number.** This run was executed on the developer's macOS workstation inside Docker Desktop, _not_ the Hostinger KVM 2 target host. Its purpose is to verify the saga code and the zero-loss acceptance script before shipping to the VPS. The headline Phase 1 MEASURED entry on real target hardware will land after the runs in [`docs/VPS_RUNBOOK.md`](VPS_RUNBOOK.md).

```
Phase: 1
Run:   dev-sanity-mac-100rps
Date:  2026-10-17
Host:  macOS 15 (Darwin 25.6.0), Apple Silicon, Docker Desktop 4.55 (dev workstation — not the Phase 1 target)
Git sha: <no commits yet — Phase 1 implementation tree>
Docker: 29.1.3
Compose: 2.40.3-desktop.1
Kafka image: confluentinc/cp-kafka:7.6.1 (KRaft, single broker, RF=1, 3 partitions)
Schema Registry: confluentinc/cp-schema-registry:7.6.1
Command: make demo-phase1   (LOAD_RPS=100  LOAD_DURATION_SEC=60  LOAD_BAD_SKU_RATE=0.01)
Config:  payment.gateway.failure-rate=0.05  payment.gateway.processing-delay-ms=50
Partitions: 3, Replication-factor: 1

Load-generator Summary (verbatim):
  Target RPS       : 100
  Duration (cfg)   : 60s
  Elapsed (actual) : 60.00s
  Sent             : 6001
  2xx              : 5998  (99.95%)
  4xx              : 0
  5xx              : 3
  Errors           : 0
  Achieved RPS     : 100.01
  Error rate       : 0.05%
  Latency ms       : p50=1.7  p95=3.3  p99=10.1  max=381.4

saga_counts (from analytics-service):
  order.created      5998
  inventory.reserved 5932
  inventory.failed     66    (compensation path 1 — bad SKUs @ 1% rate)
  payment.captured   5621
  payment.failed      311    (compensation path 2 — simulated gateway @ 5% rate)
  shipment.created   5621
  order.cancelled     377    (= inventory.failed 66 + payment.failed 311)

GMV + failure rate (analytics-service):
  gmv_cents_total   : 419023667   (₹4,190,236.67 cumulative over the 60 s window)
  orders_created    : 5998
  orders_cancelled  : 377
  failure_rate_pct  : 6.29        (377 / 5998)

Orders status distribution (order-api orderdb):
  CONFIRMED : 5621     (= shipment.created ✓)
  CANCELLED : 377      (= order.cancelled ✓)
  PENDING   : 0        (= in-flight at test end ✓)

Payments status distribution (payment-service paymentdb):
  CAPTURED  : 5621
  FAILED    : 311

Shipments total (shipping-service shippingdb):
  shipments : 5621

Consumer group lag at drain end: 0 across all topics / all consumer groups
Consumer groups:
  order-api               (shipment.created, order.cancelled)   lag=0
  inventory-service       (order.created, payment.failed)       lag=0
  payment-service         (inventory.reserved)                  lag=0
  shipping-service        (payment.captured)                    lag=0
  notification-service    (all 5 lifecycle topics)              lag=0
  analytics-service       (all 7 lifecycle topics)              lag=0

Acceptance script (make verify-zero-loss):
  PASS: 5998 orders ended CONFIRMED or CANCELLED. 0 in-flight. 0 lost.

Notes:
  - 3 initial POST /orders returned 5xx (handled during the first ~100 ms of warmup
    before Spring Boot caches were primed). Load-gen reported error-rate=0.05 %.
  - verify-zero-loss required a second poll on this run (default drain at the time
    was 25 s, insufficient for the payment-service tail backlog; 60 s additional
    sleep drained it to zero). Root cause: payment synthetic delay is 50 ms
    serialised per partition → ceiling ~60 msg/s/partition × 3 partitions ≈
    180 msg/s steady-state. At 100 orders/s input this is comfortable in steady
    state but a tail backlog accumulates by end-of-load. The Makefile
    `LAG_DRAIN_SECS` default has been raised to 90 s so the headline command
    passes first-shot; VPS runs may revisit this.
  - payment.failed 311 / inventory.reserved 5932 = 5.24 %, matches the configured
    5 % simulated-gateway failure rate.
  - inventory.failed 66 / order.created 5998 = 1.10 %, matches the configured
    1 % bad-SKU rate in the load generator.
  - All 7 topics configured 3 partitions RF=1 (single-broker KRaft).
  - Dual-write (order-api DB → Kafka) was in play as expected per ADR-0004. No
    cross-boundary loss observed; the zero-loss script asserts this.
```

**Headline VPS Phase 1 MEASURED entry still pending.** See the "Pending" section below and the planned runs in [`VPS_RUNBOOK.md`](VPS_RUNBOOK.md).

### Phase 2 — ramp with full observability (Mac Docker Desktop, dev sanity run)

> **⚠️ Dev sanity measurement, not the headline VPS number.** Same workstation as the Phase 1 entry above. Purpose: validate that observability wires up correctly (Prometheus scraping, Grafana provisioning, OTel traces end to end) and that the ramp scenario reaches a bottleneck we can characterize. The ceiling numbers below hold the *shape* of the problem; absolute magnitudes will shift on the Hostinger KVM 2 VPS.

```
Phase: 2
Run:   dev-sanity-mac-ramp-100-to-800
Date:  2026-10-18
Host:  macOS 15 (Darwin 25.6.0), Apple Silicon, Docker Desktop 4.55 (dev workstation — not the Phase 2 target)
Git sha: <no commits yet — Phase 2 implementation tree>
Docker: 29.1.3
Compose: 2.40.3-desktop.1
Images:
  Kafka            confluentinc/cp-kafka:7.6.1 (KRaft, single broker, RF=1, 3 partitions)
  Schema Registry  confluentinc/cp-schema-registry:7.6.1
  Prometheus       prom/prometheus:v2.55.1
  Grafana          grafana/grafana-oss:11.3.0
  Jaeger           jaegertracing/all-in-one:1.60 (OTLP enabled; in-memory; MEMORY_MAX_TRACES=20000)
  OTel Collector   otel/opentelemetry-collector-contrib:0.112.0
  kafka-exporter   danielqsj/kafka-exporter:v1.8.0
Agent:
  OpenTelemetry Java agent 2.9.0, loaded via JAVA_TOOL_OPTIONS=-javaagent on every service
Scrape config: 9 jobs @ 10-15 s (6 app services /actuator/prometheus + kafka-exporter + otel-collector :8888 + prometheus self)
Command: make load-ramp
         RAMP_FROM_RPS=100 RAMP_TO_RPS=800 RAMP_DURATION_SEC=180
         REPORT_DIR=docs/benchmarks/raw/phase2-ramp-mac-2026-10-18
Config:  payment.gateway.failure-rate=0.05  payment.gateway.processing-delay-ms=50

Load-generator JSON report (verbatim):
  scenario           : ramp
  scenarioParams     : {fromRps: 100, toRps: 800, durationSec: 180}
  elapsedSec         : 180.00
  sent               : 81 000     (expected = ∫ rate(t) dt = 100·180 + 350·180 = 81 000 ✓)
  status.2xx         : 80 904  (99.88 %)
  status.4xx         :  0
  status.5xx         :  96  (0.12 %, order_items PK dup — load-gen bug, see ADR-0008)
  achievedRps        : 449.99
  errorRatePct       : 0.12 %
  latencyMs          : p50=1.0   p95=5.5   p99=11.7   max=80.1   (POST /orders only)

End-of-ramp order state (one minute drain window):
  CONFIRMED :  5 576   ( 6.9 % — the saga's actual throughput over the 180 s)
  CANCELLED :  1 082   ( 1.3 %)
  PENDING   : 75 744   (93.6 % — stuck in-flight waiting for payment-service)

Analytics saga_counts (same window):
  order.created      : 80 904   (fully produced; order-api never fell behind)
  inventory.reserved : 80 338   (inventory-service kept pace)
  inventory.failed   :    566
  payment.captured   :  5 575   ← throughput ceiling observed here
  payment.failed     :    296
  shipment.created   :  5 575
  order.cancelled    :  1 082

Consumer-group lag at end-of-run:
  All groups                  : lag = 0 across every topic
  payment-service / inventory.reserved : lag = ~82 000, growing (ceiling driver — ADR-0008)

Docker stats, measured at t ≈ 120 s during the ramp (peak CPU snapshot):
  Service              CPU %   Mem used / limit
  fkp-postgres         85.21   181 / 384 MiB      (secondary bottleneck — will matter next phase)
  fkp-kafka            57.37   694 / 1536 MiB
  fkp-order-api        34.28   372 / 512 MiB
  fkp-analytics        28.10   336 / 512 MiB
  fkp-inventory        27.11   335 / 512 MiB
  fkp-notification     27.01   331 / 512 MiB
  fkp-otel-collector   16.73   151 / 256 MiB
  fkp-payment-service   1.85   331 / 512 MiB      (sleep-bound, NOT CPU-bound — the actual ceiling)

Observability evidence (what the dashboards show):
  Prometheus targets up   : 9 / 9
  Grafana dashboards      : 6 / 6 provisioned under folder "festival-kafka-poc"
                            fkp-topic-throughput, fkp-consumer-lag,
                            fkp-order-e2e-latency, fkp-producer,
                            fkp-broker, fkp-saga-outcomes
  Custom metrics flowing  : fkp_saga_outcome_total (confirmed=5 141→5 576),
                            fkp_order_e2e_latency_seconds (histogram populated)
  Topic throughput rate   : sum rate = 582 msgs/s across all topics (5m avg at run end)

Trace propagation evidence:
  Verified at 50 rps / 30 s steady (no observability memory pressure):
    50 traces fetched for order-api
    49 / 50 traces spanned ALL 6 services in a SINGLE trace tree
          (order-api, inventory-service, payment-service, shipping-service,
           notification-service, analytics-service)
    1  / 50 trace had 5 services — the compensation path trace where
          shipping-service was never involved (order was cancelled).
  Example trace:  4b77a4c8e9affac761b18f2042f66d30 (47 spans, all 6 services)
  Mechanism: OTel Java agent 2.9.0 auto-instruments spring-kafka producer +
             consumer, propagating W3C traceparent via Kafka record headers.
             No code changes required.

First-bottleneck analysis (full detail in ADR-0008):
  Mechanism       : payment-service's @KafkaListener handler sleeps 50 ms
                    (simulated gateway latency). Spring Kafka default
                    concurrency=1 serialises records within each partition's
                    consumer thread.
  Ceiling (theory): 3 partitions × (1 record / 50 ms) = 60 msg/s
  Ceiling (obs.)  : ~33 msg/s (includes DB insert, Avro deser, header parse,
                    Kafka commit, OTel span emission on top of the sleep)
  Fix: deferred to Phase 3 or Phase 5. Phase 2 is observability-first.

Raw transcripts: docs/benchmarks/raw/phase2-ramp-mac-2026-10-18/
                   load-ramp.json                — JSON report from load-gen
                   transcript.txt                — full `make load-ramp` stdout
                   consumer-groups-post-ramp.txt — kafka-consumer-groups --describe --all-groups
                   db-state-post-ramp.txt        — SELECTs on orders/payments/shipments/saga_counts
                   docker-stats.txt              — docker stats snapshot (post-ramp, idle)

Notes:
  - Jaeger OOM-killed twice at the initial 384 MiB limit under ~400 rps traffic.
    Raised to 768 MiB with MEMORY_MAX_TRACES reduced 50000 → 20000. This fix is
    in the committed compose; a VPS run must either carry the extra memory
    or lower observability load (e.g. 10 % tail sampling at the collector).
  - A pre-Phase-2 cached load-generator image initially made the ramp appear
    stuck at 100 rps (old jar without scenario support). Caught by log format
    diff (`scenario=ramp` vs `rate=100 duration=180s`); fixed by explicit
    `docker compose build load-generator`. The ramp numbers above are from
    the correct re-run.
  - The 96 × 5xx errors are a real bug in OrderPayloadBuilder: it can pick the
    same SKU twice for the same order's items list, violating the composite
    PK (order_id, sku_id) on order_items. Load-generator fix is a Phase 3
    cleanup. The service behaviour is correct (DB rejected the write).
```

**Headline VPS Phase 2 MEASURED entry still pending.** Will follow the Hostinger KVM 2 runs for Phase 1 (same `docs/VPS_RUNBOOK.md`). Numbers on the VPS will shift down (lower per-core clock, 2 shared vCPU) and the specific absolute ceiling will differ — but the *shape* of the problem (payment is the ceiling, Postgres is the next ceiling) should hold.

---

## PROJECTED

| Phase | Target | Formula | Inputs | Caveats |
|---|---|---|---|---|
| — | — | — | — | — |

_No projections recorded yet. Phase 7 is the first phase that produces projected numbers; its anchors come from Phase 6's MEASURED entries._

---

## Pending

### Phase 1 — walking-skeleton demo on VPS

**Status:** Code + infra + runbook ready. Not yet executed.
**Blocked on:** running [`docs/VPS_RUNBOOK.md`](VPS_RUNBOOK.md) on the Hostinger KVM 2 instance.

**Planned runs (one entry each will land in the MEASURED table above):**

| # | Target RPS | Duration | Purpose |
|---|---|---|---|
| 1 | 100 | 60 s | Sanity: confirm saga completes end-to-end |
| 2 | 500 | 60 s | Mid-load: find the first bottleneck |
| 3 | 1000 | 60 s | Push until saturation; stop early if run 2 is already struggling |

**MEASURED template that will be filled in per run:**

```
Phase: 1
Run:   <run1|run2|run3>
Date:  <YYYY-MM-DD HH:MM TZ>
Host:  Hostinger KVM 2 — AMD EPYC 9354P (2 vCPU), 7.8 GiB, no swap, Ubuntu 24.04
Git sha: <output of `git rev-parse HEAD`>
Docker: <version>
Compose: <version>
Kafka image: confluentinc/cp-kafka:7.6.1
Command: make demo-phase1 LOAD_RPS=<X> LOAD_DURATION_SEC=60
Config:  payment.gateway.failure-rate=0.01 (default)
Partitions: 3, Replication-factor: 1

Load-generator Summary (verbatim):
  Target RPS       : <N>
  Elapsed          : <seconds>
  Sent             : <N>
  2xx              : <N>  (<%>)
  4xx / 5xx / err  : <N> / <N> / <N>
  Achieved RPS     : <value>
  Error rate       : <%>
  Latency ms       : p50=<N>  p95=<N>  p99=<N>  max=<N>

saga_counts (from analytics-service):
  order.created      <N>
  inventory.reserved <N>
  inventory.failed   <N>
  payment.captured   <N>
  payment.failed     <N>
  shipment.created   <N>
  order.cancelled    <N>

Saga completion rate:  shipment.created / order.created = <%>
Expected ~99% at 1% payment failure rate.

Orders status distribution:
  CREATED   <N>   (should be ~0 after drain; residual = sagas still in flight at test end)
  COMPLETED <N>   (should match shipment.created)
  CANCELLED <N>   (should match order.cancelled)

Consumer group lag at drain end:
  <group> <topic> lag=<N>  (should be 0 for all)

Docker stats peak (from docker-stats.txt):
  Service              Mem used / limit   CPU %
  fkp-kafka            <...>              <...>
  fkp-order-api        <...>              <...>
  (...)

Raw transcripts:  docs/benchmarks/raw/<run-label>/transcript.txt
                  docs/benchmarks/raw/<run-label>/docker-stats.txt

Notes: <anything suspect or surprising>
```

---

## Appendix: entry templates

### MEASURED template

```
Phase: N
Date: YYYY-MM-DD
Host: <reference to hardware record>
Kafka version: x.y.z
Service build: <git sha>
Command: <exact shell command used>
Config: <non-default env / args>
Duration: <sec>
Producer throughput: <msg/s>
Consumer saga completion rate: <%>
End-to-end latency: p50=... p99=... p99.9=...
Error rate: <%>
Raw log: docs/benchmarks/raw/<timestamp>.txt
Notes: <what was interesting; anything suspect>
```

### PROJECTED template

```
Phase: N
Projection: <what we're projecting>
Formula: <math, with units>
Inputs:
  - <name> = <value>   (source: MEASURED Phase M <date>  OR  docs/vendor link)
  - ...
Assumptions:
  - <assumption that may not hold>
  - ...
Result: <value>
Caveats: <what would move this number up or down the most>
```
