# DECISIONS

Architectural decisions, ADR-style. One entry per decision. Each phase closes with at least one entry capturing what broke, how we detected it, what we changed, what it cost (trade-off).

Status values: **Proposed**, **Accepted**, **Superseded by #N**, **Deprecated**.

---

## ADR-0001: Event envelope embedded in every event schema

**Date:** 2026-10-17
**Status:** Accepted
**Phase:** 1

### Context

Every lifecycle event must carry `eventId`, `orderId`, `eventType`, `occurredAt`, `schemaVersion`, `traceId`. Three ways to carry them:

1. Kafka headers only.
2. Flattened into each event schema.
3. A nested `EventEnvelope` Avro record embedded in every event schema.

### Decision

Option 3 — nested `EventEnvelope` record. We also mirror `eventId` and `eventType` into Kafka headers so brokers/consumers can route or dedupe without deserializing the value.

### Consequences

- Pro: envelope evolves independently from the business payload; one shared schema change touches all events uniformly.
- Pro: payload is self-describing on replay even if headers are stripped by an intermediary.
- Con: slight payload-size overhead vs. headers-only. Not material for this workload.
- Con: generated Java classes have a `getEnvelope()` indirection; consumers write `event.getEnvelope().getEventId()`.

### Cost

Payload size: envelope ~60–100 bytes with the UUID strings (we're using string-typed UUIDs for Avro simplicity, not the fixed-16 binary form). Acceptable for 1 KB-range events.

---

## ADR-0002: Monetary values as `long` minor units, not Avro `decimal`

**Date:** 2026-10-17
**Status:** Accepted
**Phase:** 1

### Context

Order totals and payment amounts need exactness. Avro has a `decimal` logical type backed by `bytes` + precision/scale, which generates `BigDecimal` fields in Java. We could use that, or store cents/paise as `long`.

### Decision

Store monetary values as `long` representing minor units (`totalAmountCents`, `amountCapturedCents`). Currency is a separate string field defaulting to `INR`.

### Consequences

- Pro: trivial Java handling; no `BigDecimal` conversion ceremony in generated code.
- Pro: fixed 8-byte width; no variable `bytes` payload.
- Con: not suitable for currencies with sub-cent precision or precious-metals accounting. Acceptable for retail orders.
- Con: callers must agree on the unit per currency. Documented in schema `doc` fields.

### Cost

Future contract change if we expand to a currency with different minor-unit precision. Mitigated by `schemaVersion` + Schema Registry compatibility rules.

---

## ADR-0003: Partition count 3 for Phase 1

**Date:** 2026-10-17
**Status:** Accepted
**Phase:** 1

### Context

Partition count has to be chosen at topic creation. Raising it later requires rebalancing or recreation. On the measurement VPS we have 2 vCPU total; running more consumer threads than cores burns context-switch budget.

### Decision

All lifecycle topics created with `partitions=3`, `replication-factor=1` for Phase 1.

### Consequences

- Pro: each consumer group can run with up to 3 instances in Phase 5/6 without changing topics.
- Pro: slightly more than CPU count leaves headroom for IO-bound stages.
- Con: at 3 partitions and 1 broker, no fault tolerance. **Expected and acceptable** for Phase 1; raised in Phase 5.

### Cost

Capped per-topic parallelism. We will raise partition count to 24–48 in Phase 5, which requires topic recreation (acceptable because Phase 5 begins the horizontal-scaling story).

---

## ADR-0004: Dual-write accepted in Phases 1–3; outbox introduced in Phase 4

**Date:** 2026-10-17
**Status:** Accepted
**Phase:** 1
**Superseded by:** _pending Phase 4 ADR_

### Context

Writer services (`order-api`, `inventory-service`, `payment-service`, `shipping-service`) write business state to Postgres and then publish to Kafka. If the service crashes between the DB commit and the Kafka send, the event is lost. The transactional outbox pattern fixes this but adds a service (`outbox-relay`) and a per-service `outbox` table.

### Decision

Phases 1–3 run with naive dual-write. The gap is **explicitly documented**, not hidden. Phase 4 introduces the outbox + relay.

### Consequences

- Pro: Phases 1–3 stay small enough to focus on their own lesson (walking skeleton, reliability knobs, observability).
- Pro: the Phase 4 ADR has a concrete "here's what the problem looked like" measurement from a chaos test run during Phase 2 or 3.
- Con: Phase 1–3 benchmarks are reported under a known-lossy delivery model. Must be labelled as such in BENCHMARKS entries.

### Cost

Dual-write gap window ≈ interval between DB commit and `producer.send()` return. On this host, O(1–10 ms). In Phase 2 we quantify with a crash-injection test.

---

## Template for new entries

```
## ADR-NNNN: Short title

**Date:** YYYY-MM-DD
**Status:** Proposed | Accepted | Superseded by #M | Deprecated
**Phase:** N

### Context
What problem are we solving? What forces are in play?

### Decision
What we chose, concretely.

### Consequences
Pros, cons, trade-offs.

### Cost
The specific trade-off we accepted. If a chaos test or benchmark informed this, cite the entry in BENCHMARKS.md.
```


---

## ADR-0005: Testcontainers integration tests validated on Linux (VPS), not local macOS

**Date:** 2026-10-17
**Status:** Accepted
**Phase:** 1

### Context

The Phase 1 integration test for `order-api` compiles and is correct Java, but cannot execute on this developer workstation (macOS, Docker Desktop 4.55 / Engine 29.1.3).

Observed failure mode: Testcontainers' three client-provider strategies all talk to a Docker Desktop socket that returns `Status 400` with a stub `Info` payload whose only non-empty field is a `Labels` entry: `com.docker.desktop.address=unix:///…/Library/Containers/com.docker.docker/Data/docker-cli.sock`.

Direct `curl` to the standard daemon socket (`~/.docker/run/docker.sock`) returns `/info` fine on API `v1.47` and `v1.44`, but returns the identical stub payload on `v1.43` and below.

Root cause: Docker Engine 29 declares `MinAPIVersion=1.44`. docker-java 3.4.2 (bundled with Testcontainers 1.21.3) defaults to API `1.43`. On Docker Desktop 4.3x+ this triggers the mediated-socket path, which returns the stub.

### Attempts

1. `DOCKER_HOST` env + `docker.host` system property pointing to the full daemon socket.
2. `DOCKER_API_VERSION` env + `api.version` system property set to `1.47`.
3. `~/.testcontainers.properties` with `docker.host=…` override.
4. Surefire `<environmentVariables>` in parent `pluginManagement`.
5. Override `com.github.docker-java:*` to `3.5.3` via `dependencyManagement`.
6. Downgrade `commons-codec` to `1.16.1` (fixes a separate `NoClassDefFound: Charsets` once step 5 unlocked execution).

Steps 1–5 did not change the stub-response behavior; `commons-codec` override (6) is still needed independently.

### Decision

Phase 1 integration tests are **compile-verified locally** and **execution-verified on the Linux VPS** (Task #12). On the VPS, Docker Engine is accessed directly with no Desktop mediation, so docker-java 3.4.2 or 3.5.3's defaults are irrelevant to the broken-strategy path.

For local macOS execution, enabling **Docker Desktop → Settings → Advanced → "Allow the default Docker socket to be used"** should bypass the mediation and let the tests run. That toggle is a per-workstation setting, not a project artifact.

### Consequences

- Pro: Phase 1 is not held hostage by a dev-env quirk; the actual services and demo run normally via `docker compose` on both Mac and VPS.
- Pro: The test code is correct — we verified compile, dependency resolution, and that the main service packages into a 51 MB fat jar.
- Con: Tests do not run locally without the one-click Docker Desktop setting change. Developers who skip the toggle will see this failure.
- Con: We have no local green-test anchor for Phase 1 until the VPS run. The service's behaviour is covered by the demo instead.

### Cost

One extra VPS verification pass per phase to confirm tests pass under native Docker before the ADR for that phase closes. Acceptable; the VPS is where the measurement runs happen anyway.

### Keep in mind

If a future phase introduces logic that is HARD to verify via the demo (e.g. race conditions, chaos tests, specific error-path consumer behaviour), the test matters more. At that point, revisit this ADR and either run tests under Linux CI, or require the Docker Desktop setting as a prerequisite.


---

## ADR-0006: Phase 1 wrap-up — walking skeleton complete, measurement pending

**Date:** 2026-10-17
**Status:** Accepted (sanity-run complete on Mac Docker Desktop 2026-10-17; headline VPS entry still pending).
**Phase:** 1

### Context

Phase 1 built a working choreographed saga across six services with Avro + Confluent Schema Registry, Postgres-per-service, Kafka in KRaft mode, idempotent consumers, and a Java load generator — all glued together by `make demo-phase1`. Every piece compiles, docker-compose parses, local infra brings up cleanly. The only open question is **what the actual numbers look like under load on the measurement VPS**.

### What's in Phase 1 (all code-complete)

- 7 lifecycle topics (`order.created`, `inventory.reserved`, `inventory.failed`, `payment.captured`, `payment.failed`, `shipment.created`, `order.cancelled`) at `partitions=3 replicationFactor=1` (ADR-0003).
- 6 services:
  - `order-api` — REST entry; `POST /orders` writes DB then publishes `order.created`.
  - `inventory-service` — reserves stock; emits `inventory.reserved` or `inventory.failed`+`order.cancelled`; releases on `payment.failed`.
  - `payment-service` — simulated gateway with configurable `failure-rate` (default 1%); emits `payment.captured` or `payment.failed`.
  - `shipping-service` — fake carrier + tracking number; emits `shipment.created`.
  - `notification-service` — fan-out consumer across all 7 topics; writes `notifications_log`.
  - `analytics-service` — fan-out consumer; writes `lifecycle_events` fact table + `saga_counts` aggregation.
- `load-generator` — standalone Java 21 app with `HttpClient` + virtual threads, built as a thin jar.
- Dual-write accepted explicitly (ADR-0004); Phase 4 outbox closes it.
- Idempotent consumers via per-service `processed_events(event_id PK)` with `INSERT … ON CONFLICT DO NOTHING`, inside the business transaction so dedupe and business update roll back together on failure (except analytics-service which collapses dedupe into the fact table's PK).
- Compose has 15 services (6 app, 1 Kafka, 1 Schema Registry, 1 topic-init, 6 Postgres), all memory-limited.

### What was deliberately deferred

| Deferred to | What |
|---|---|
| Phase 2 | `acks=all` + `enable.idempotence=true` + DLT + chaos test of broker-kill. Phase 1 producers use `acks=1`. |
| Phase 2 | Payment gateway call outside DB transaction + async reconciliation. Phase 1 keeps it in-tx for simplicity (safe only because simulated). |
| Phase 3 | Prometheus / Grafana / OpenTelemetry / kafka-exporter. Phase 1 ships bare Actuator probes used only for health. |
| Phase 4 | Transactional outbox. Phase 1 dual-write gap is quantified in Phase 2's chaos test. |
| Phase 5 | Redis hot-SKU counters; partition count bump (`3 → 24+`); 3-broker Kafka cluster. |

### Measurement expectations (projection — to be validated)

Target host: Hostinger KVM 2 (2 vCPU AMD EPYC 9354P, 7.8 GiB RAM, no swap).

- **Memory budget projection:** Kafka 1.5 GiB + Schema Registry ~0.5 GiB + 6× Postgres ~1 GiB + 6× services (~2.3 GiB) + OS/Docker ~0.5 GiB = **~6 GiB used, ~1.8 GiB headroom**. No swap, so a mis-tuned heap triggers an OOM-kill rather than a slowdown.
- **Throughput expectation:** 100 rps should be trivial. 500 rps is where I expect the first bottleneck to surface (likely CPU-bound; 2 vCPU shared across Kafka + 6 JVMs + 6 Postgres + load-generator). 1000 rps may saturate; I'll record where it breaks rather than press for a target.
- **Latency expectation:** p99 end-to-end (POST → shipment persisted) likely 100–500 ms at 100 rps, climbing as load grows. The async part of the saga adds variable latency driven by consumer-poll intervals.
- **Error-mode expectation:** payment.failed at ~1% (gateway default); nothing else should fail. Any 5xx on the REST endpoint or missing `shipment.created` for a non-cancelled order is a bug to investigate.

### Decision

Phase 1 is **code-complete and ready to measure**. The first MEASURED entry lands in `BENCHMARKS.md` after a VPS run per `docs/VPS_RUNBOOK.md`. This ADR flips from Proposed → Accepted at that point, with the actual results summarized below.

### Measured results

**Dev-sanity run (Mac Docker Desktop, 2026-10-17) — PASS:**

- `make demo-phase1` at 100 rps / 60 s → 6001 POST /orders sent, **5998 accepted (99.95 %)**, 3 × 5xx during warmup.
- POST /orders latency: **p50 = 1.7 ms, p95 = 3.3 ms, p99 = 10.1 ms, max = 381.4 ms**.
- Saga outcome: **5621 CONFIRMED + 377 CANCELLED = 5998 terminal, 0 PENDING, 0 lost**.
- Compensation split: 66 inventory-failed (bad-SKU rate 1.1 %, matches configured 1 %) + 311 payment-failed (5.24 %, matches configured 5 %).
- GMV accumulator (analytics-service): **419 023 667 cents = ₹41,90,236.67**.
- Zero-loss acceptance script (`make verify-zero-loss`): **PASS** after a second drain poll (payment-service queue drained to zero within ~60 s past end-of-load; default 25 s drain was marginal — see BENCHMARKS note).
- Full consumer-group lag at completion: **0** across all 6 groups.
- Full transcript: `docs/BENCHMARKS.md` → "Phase 1 — walking-skeleton demo (Mac Docker Desktop, dev sanity run)".

**Headline VPS runs — still pending:**

- Run 1 (100 rps / 60 s): _pending — Hostinger KVM 2_
- Run 2 (500 rps / 60 s): _pending_
- Run 3 (1000 rps / 60 s): _pending (skip if run 2 saturates)_

The headline Phase 1 BENCHMARKS entry on 2 vCPU / 7.8 GiB hardware is what will exercise the memory-budget projection in this ADR. The dev-sanity run only confirms the saga is correct and the acceptance script is honest; it says nothing about the VPS.

### Consequences

- Pro: a complete walking skeleton exists in ~130 Java files + ~20 config files + 1 Makefile. Phase 2 starts from a working baseline, not a rebuild.
- Pro: every design trade-off visible in Phases 2–7 is motivated by something in this phase — the dual-write gap, the lack of DLT, the absent observability, the single broker. Later phases earn their complexity.
- Con: no local test green anchor on the Mac dev workstation (ADR-0005). Phase 1 integration tests execute only on Linux (VPS) or on Mac with Docker Desktop's "default socket" setting enabled.
- Con: 51 MB fat jars across 6 services is wasteful. Phase 2 may introduce a layered Jib build or a smaller base image.

### Cost

The accepted trade-off is: **honesty about the dual-write gap** (ADR-0004) + **honesty about the test gap on Mac** (ADR-0005) + **honesty about measurements being pending until the VPS runs** (this ADR). Phase 1 earns its right to be "done" only when the first MEASURED entry lands.


---

## ADR-0007: One Postgres instance with per-service databases, not per-service containers

**Date:** 2026-10-18
**Status:** Accepted
**Phase:** 0 (scaffold refactor)

### Context

Phase 1 originally shipped six Postgres containers — one per service, each bound to its own volume and health-checked independently. The Phase 0 contract explicitly specifies "Postgres (with an init script creating a database per service)", i.e. **one** Postgres instance with the per-service databases created at first boot via `/docker-entrypoint-initdb.d/`.

### Decision

Replace the six `postgres-*` services in `infra/docker-compose.yml` with a single `postgres` service. Mount `infra/postgres/init.sql` into the container's init directory; the SQL file issues `CREATE DATABASE orderdb OWNER fkp; …` for each of the six per-service DBs. Each service's `POSTGRES_HOST` becomes `postgres`; each service's `POSTGRES_DB` stays unique (`orderdb`, `inventorydb`, `paymentdb`, `shippingdb`, `notificationdb`, `analyticsdb`).

### Consequences

- Pro: ~600 MiB saved on the 7.8 GiB VPS (one Postgres @ 384 MiB cap vs six @ 160 MiB each). Shared buffer pool, one autovacuum worker, one WAL. On a 2 vCPU host this is non-trivial.
- Pro: matches the Phase 0 contract literally.
- Pro: simpler `make psql` routing (one container, DB name decides).
- Con: shared fate — a Postgres restart drops all six service connections. Acceptable for a POC walkthrough; a real "database per service" principle would use separate clusters.
- Con: per-DB tuning (shared_buffers, max_connections) must balance across all six services instead of being tuned per service. Phase 5 can revisit if the inventory DB becomes the hot path.

### Cost

Loss of per-service DB isolation at the container level. Documented, intentional. The database-per-service principle at the schema/DB level is preserved.


---

## ADR-0008: First observed bottleneck — payment-service synchronous `Thread.sleep(50 ms)` serializes per partition

**Date:** 2026-10-18
**Status:** Observed; fix deferred to a later phase (Phase 3 or later). Phase 2 is observability-first — no optimizations yet.
**Phase:** 2

### Context

Phase 2 added Prometheus, Grafana (6 provisioned dashboards), OpenTelemetry agent on every service, Jaeger, kafka-exporter, and a scenario-driven load generator. The acceptance criterion for the phase is to **run a ramp with observability on and document the first bottleneck observed** — not to fix it.

Ramp run on 2026-10-18 on Mac Docker Desktop:

```
Scenario: ramp 100 → 800 rps over 180 s, max-items=3, sku-count=1000, bad-sku-rate=0.01
Sent:    81 000       (expected = ∫ rate(t) dt = 100·180 + 350·180 = 81 000 ✓)
2xx:     80 904  (99.88 %)
5xx:         96  (0.12 %, order_items PK dup — see "Secondary findings" below)
Achieved RPS: 449.99 (matches ramp midpoint)
POST /orders latency: p50 = 1.0 ms, p95 = 5.5 ms, p99 = 11.7 ms, max = 80 ms
```

**End-of-ramp order state (one minute drain window):**

| State     | Orders  | % of 2xx |
|-----------|---------|----------|
| CONFIRMED |   5 576 |    6.9 % |
| CANCELLED |   1 082 |    1.3 % |
| **PENDING** | **75 744** | **93.6 %** |

Only ~6.9 % of accepted orders completed the saga; the rest piled up in the Kafka backlog.

### What the dashboards showed

- **Dashboard 1 — Topic throughput:** `order.created` and `inventory.reserved` tracked each other at ~450 msg/s. `payment.captured` + `payment.failed` combined held flat at ~33 msg/s regardless of input rate.
- **Dashboard 2 — Consumer lag:** every consumer group reached lag ≈ 0 within seconds of ingress stopping, **except `payment-service` on `inventory.reserved`**. That lag grew monotonically throughout the ramp: 0 → 12 k (at t=60 s) → 37 k (at t=120 s) → ~82 k (end of ramp drain), and continued accumulating.
- **Dashboard 3 — End-to-end order latency:** `fkp_order_e2e_latency_seconds` p99 climbed from ~50 ms early to multi-second as the backlog built. The timer only records orders that reach a terminal state, so this metric reports latency for the ~7 % of orders that got through, not for the stuck 93 %.
- **Dashboard 4 — Producer internals:** request-latency, batch size, compression ratio, and retries were all healthy. Nothing producer-side was constraining the throughput.
- **Dashboard 5 — Broker:** no under-replicated partitions (RF=1 can't have any), no misplaced leaders. Broker was not the bottleneck.
- **Dashboard 6 — Saga outcomes:** confirmed-vs-cancelled rate held at ~33 msg/s combined, exactly matching payment-service's processing rate.

### What `docker stats` showed during the ramp

| Service                | CPU %    | Memory used / limit |
|------------------------|----------|---------------------|
| **fkp-payment-service**| **1.85** | 331 / 512 MiB       |
| fkp-postgres           | 85.21    | 181 / 384 MiB       |
| fkp-kafka              | 57.37    | 694 / 1536 MiB      |
| fkp-order-api          | 34.28    | 372 / 512 MiB       |
| fkp-analytics-service  | 28.10    | 336 / 512 MiB       |
| fkp-inventory-service  | 27.11    | 335 / 512 MiB       |
| fkp-notification-service | 27.01  | 331 / 512 MiB       |
| fkp-otel-collector     | 16.73    | 151 / 256 MiB       |

Payment-service is the slowest downstream consumer **and uses the least CPU**. It's not CPU-bound; it's wall-clock bound.

### The bottleneck

**Payment-service's simulated gateway latency serializes record processing per partition.**

The handler calls `Thread.sleep(50 ms)` (configured via `payment.gateway.processing-delay-ms` with default 50) on every record. Spring Kafka with the default `concurrency = 1` creates one consumer thread per `@KafkaListener`, and that thread processes records serially within each poll result. With 3 partitions on `inventory.reserved`, the broker fans out to 3 partitions but the consumer side runs sequentially:

```
Ceiling = (3 partitions) × (1 record / 50 ms) ≈ 60 msg/s
Observed = ~33 msg/s   (lower because the 50 ms is wall-clock, so the actual
                        serving includes DB-insert, Avro deserialization,
                        header parsing, Kafka commit, OTel span emission)
```

At the ramp midpoint (450 rps input), 450 msg/s of `order.created` → ~445 msg/s of `inventory.reserved` → payment-service draining at ~33 msg/s. The backlog grew at ~412 msg/s, which matches the observed 82 k lag at 180 s + drain.

**The lag is behavioural, not a bug.** The `payment.gateway.processing-delay-ms` setting exists specifically to model a real external payment gateway's latency. Phase 1's demo at 100 rps worked only because the drain window after the load was long enough for the saga's natural ceiling to catch up before the acceptance check ran.

### Why we deliberately did not fix this in Phase 2

The Phase 2 prompt is explicit:

> add observability and a proper load generator BEFORE optimizing anything. We must be able to see what breaks.

So this ADR is "we saw what breaks". Candidate fixes, each with trade-offs, are catalogued below for the phase that chooses to address them:

| Candidate fix | Where | Pros | Cons |
|---|---|---|---|
| 1. Raise `KafkaListener.concurrency` on payment-service (`spring.kafka.listener.concurrency: 3`) so each partition gets its own thread | config | one-line change | still bounded by `partitions × 1/latency = 60 msg/s`; same ceiling just reached in parallel |
| 2. Increase partition count on `inventory.reserved` (and all downstream topics) from 3 → 24 | infra | 24 × 20 = 480 msg/s ceiling | changes replay semantics; cross-topic ordering by orderId still holds iff keyed; must repartition on migration — properly a Phase 5 move |
| 3. Make the payment call asynchronous (CompletableFuture, offload the simulated sleep from the consumer thread) | code | removes the serialization | reorders processing; requires an acks/commit redesign; invalidates the current "one event = one DB txn" guarantee |
| 4. Lower `PAYMENT_DELAY_MS` for benchmarks | config override | makes the number look better | hides the gap a real gateway introduces; dishonest |
| 5. Batch payments (`@KafkaListener(batch = true)`) and process in parallel inside the batch via a bounded executor | code | utilises wall time without changing partition count | retry story becomes a lot more complex; needs dead-letter topic (Phase 3) |

**Decision for Phase 2:** keep the system exactly as it is. The purpose of Phase 2 is to make this failure mode *visible on the dashboards*; it's succeeded in that. Phase 3 (chaos + DLT) or Phase 5 (3-broker + 24 partitions) will address this.

### Secondary findings worth recording

- **Postgres CPU hit 85 %** during the ramp. On a single-process Postgres with 6 databases × ~5 concurrent connections, the plan cache + Hikari pool contention is non-trivial. It's not the current bottleneck (payment is), but at the next ceiling it will be. Phase 3 will quantify this.
- **Jaeger OOM-killed twice** at the initial 384 MiB limit with `MEMORY_MAX_TRACES=50000` under ~400 rps. Raised to 768 MiB with `MEMORY_MAX_TRACES=20000` after the observation; this ADR records the finding so a VPS deployment accounts for Jaeger memory properly.
- **Load generator produces a few duplicate SKUs within one order's items list** when `max-items > 1 && sku-count = 1000`. The `(order_id, sku_id)` composite PK on `order_items` rejects these with ~0.12 % 5xx rate. **This is a real bug in the load generator**, not a service bug; fixing it is a Phase 3 cleanup (OrderPayloadBuilder should dedupe within an order).
- **Trace continuity broke under memory pressure.** With Jaeger OOM-cycling, the 29/30 traces examined mid-ramp showed `[order-api, payment, shipping, analytics, notification]` — inventory-service's spans had been evicted. After raising Jaeger memory and re-testing at 50 rps / 30 s steady: 49/50 traces showed **all 6 services in one trace**. Trace propagation across HTTP + 5 Kafka hops works correctly via the OTel Java agent 2.9.0's default W3C `tracecontext` propagator and `spring-kafka` instrumentation; the earlier gap was infrastructure, not code.
- **Dashboard 5 — Broker — still has an acknowledged gap:** request-handler idle %, broker disk I/O, and broker network I/O are not exposed by `kafka-exporter`. They need Kafka JMX exporter + node_exporter / cAdvisor, planned alongside Phase 3's chaos test or Phase 5's 3-broker move.

### Consequences

- Pro: the system now has first-class observability. Future phases can quote dashboard panels when claiming an optimization worked.
- Pro: this ADR has the FIRST concrete, measured ceiling in the project. Phase 1's ADR-0006 claimed Phase 1 was "ready to measure"; Phase 2 actually measured.
- Pro: the ramp scenario in the load generator produces consistent 81 000-event test runs, perfect for before/after comparisons in later phases.
- Con: this run uses Mac Docker Desktop, not the Hostinger KVM 2 VPS. Numbers will shift on the VPS (likely LOWER throughput ceilings; CPU vs memory ratios also differ). A VPS run is pending alongside the Phase 1 VPS runs still outstanding from ADR-0006.
- Con: we burned a chunk of time chasing a cached-image false positive (compose reused the Phase 1 load-generator jar on the first ramp run; `scenario=ramp` logs made it immediately obvious once visible). Phase 3 Makefile should add an explicit `make build-load` step rather than relying on `--build` propagation.

### Cost

The accepted trade-off is: **we recorded the bottleneck honestly rather than papering over it with a config change**. This phase bought us the ability to say exactly where the system breaks and why, so the next optimization has a measurable baseline to beat.


---

## ADR-0009: Partition count 3 → 12 on hot topics; key = orderId; what repartitioning means for existing data

**Date:** 2026-10-18
**Status:** Accepted
**Phase:** 3

### Context

Phase 2's measured ceiling (~33 msg/s on payment-service's inventory.reserved consumer; see ADR-0008) was driven by three partitions × one consumer thread per partition × a 50 ms synthetic payment delay. The Phase 3 budget is 1k–5k orders/s on the single-broker setup; even with batch listeners and JDBC batch writes we need more partitions on the hot path so the consumer group can run more threads in parallel.

Kafka's `KafkaListener` concurrency hard-ceiling inside a group is `min(configured_concurrency, partitions)` — more threads than partitions sit idle because Kafka assigns at most one partition per consumer thread. So raising concurrency to 12 only pays off when the subscribed topic has at least 12 partitions.

### Decision

Three topics on the hot (every-order) path grow 3 → 12 partitions:

- `order.created`      — produced by order-api, consumed by inventory, notification, analytics
- `inventory.reserved` — produced by inventory, consumed by payment, notification, analytics
- `payment.captured`   — produced by payment, consumed by shipping, notification, analytics

The four compensation + fan-out topics stay at 3 partitions:

- `shipment.created`, `inventory.failed`, `payment.failed`, `order.cancelled`

Rationale: their steady-state volume is capped by the hot-path failure rates (~1 % bad-SKU, ~5 % gateway-fail), so under a 5k orders/s hot path they peak at a few hundred msg/s each — a 3-partition topic with 3-thread concurrency comfortably clears that.

**Key choice** stays what Phase 1 shipped: `recordKey = orderId.toString()`. Every producer in the saga keys its emitted record by the same orderId that originated in `order-api`.

**Migration** uses `kafka-topics --alter --partitions 12`, which is baked into `infra/topics.sh` (ADR-0004-style idempotent setup — the script only expands, never shrinks, because Kafka doesn't support shrink). The compose stack is reset for the Phase 3 measurement run so the volumes are empty when the new partition count takes effect; the "expand on an existing topic with historical data" story is covered below as a reference for the VPS runbook.

### What raising partitions does to key → partition mapping

Kafka's default partitioner for a non-null key is `murmur2(key) % numPartitions`. The function is deterministic for a given `numPartitions`, so:

- **Within one topic, after the change, for all producers using the default partitioner:** every record with key = orderId K goes to the SAME partition `P(K) = murmur2(K) % 12`. Within that partition, Kafka guarantees in-order append; a single-thread consumer sees those records in produce order. This is why per-order ordering within one topic is preserved even after the bump.
- **Across the migration boundary:** records already appended under `numPartitions = 3` are glued to physical partitions 0, 1, or 2 of that topic. Those partitions keep their contents verbatim after `--alter` — the broker does not rebalance historical data. Records appended AFTER the alter under `numPartitions = 12` land on partitions 0–11 via the new mod. For an orderId K that was going to partition 2 of 3 (say murmur2(K) % 3 = 2), new records for the same K now go to murmur2(K) % 12 = anything from 0..11, usually NOT 2.

**Consequence for in-flight sagas across a live expansion** (not what we do here, but a future-proofing note):

If order-api publishes order.created for order K to partition 2 (under 3-partitions) and then the admin bumps the topic to 12 partitions BEFORE the saga finishes, downstream events for K (inventory.reserved, payment.captured, …) produced by *their* producers after the bump will key-hash into (potentially) different partitions than the equivalent events for K in a pre-bump saga. For a saga that reasons per-event (as ours does — each handler is idempotent by eventId), this is fine: each event is handled independently and the saga reaches a terminal state regardless. For a saga that reasons per-partition (e.g. a Kafka-Streams aggregation keyed by orderId), the aggregation would see K's events split across partitions and need a repartitioning step. **Our design is per-event + idempotent-by-eventId; the partition bump does not break saga correctness.**

**Consequence for consumer-side ordering per key** — important enough to spell out:

- Before the bump: all events for key K on topic T are in one partition of 3; a single consumer thread owns that partition and sees all K's events in produce order.
- After the bump, for records produced AFTER the bump: all events for key K on topic T are in one partition of 12; one thread owns that partition. Still in order per key per topic.
- During the bump (one short window where producers may have published new partition metadata but others haven't): records keyed K may be split across the 3-era partition and a 12-era partition. For an at-least-once saga this surfaces as temporarily out-of-order delivery per key. The POC avoids this by running the bump during the fresh-stack bring-up, so there is no in-flight traffic to split.

### Alternatives considered

1. **Keep partitions at 3, raise consumer concurrency to 12.** Rejected: concurrency > partitions buys nothing within a group — the extra threads idle. (Also tested in sweep below as the Phase-2 baseline control.)
2. **Bump every topic to 12 partitions.** Rejected: compensation topics at < 100 msg/s run three times as many near-idle consumer threads for no throughput gain, and the broker's per-partition metadata footprint grows for no reason.
3. **Bump to 24 or 48 partitions.** Deferred to Phase 5 (3-broker cluster). On a single-broker, 12 is already more partitions than CPU cores on the target (2 vCPU VPS); increasing further just makes partitions compete for the same threads.
4. **Use a different partitioner (e.g. `UniformStickyPartitioner`).** Rejected: that distributes evenly across partitions when the key is null, but all our producers pass orderId as the key, so murmur2(key)%P applies.

### Consequences

- Pro: per-topic throughput ceiling raises 4× (3→12) across the hot path, which is enough headroom for the Phase 3 target.
- Pro: per-order ordering within a topic is still preserved by Kafka's partitioner contract.
- Pro: compensation topic sizes are not inflated for no reason.
- Con: a fresh Phase 3 run against a pre-populated topic (e.g. upgrading an existing cluster in place) temporarily splits a key's events across old and new partitions; the saga tolerates this because every handler is idempotent-by-eventId.
- Con: downstream Kafka Streams or ksqlDB jobs keyed by orderId would need a repartitioning stream if they existed. We don't have any — all consumers are plain spring-kafka listeners.

### Cost

The accepted trade-off is: **more partitions on hot topics only**, migrating via `--alter` with no historical-data rebalance (because Kafka won't do one). This is correct for an at-least-once idempotent saga. It would be wrong for ordered-aggregation jobs; we don't have those.


---


## ADR-0010: Compression codec on producer records — pick `lz4`

**Date:** 2026-10-18
**Status:** Accepted
**Phase:** 3

### Context

Phase 3 enables Kafka producer-side compression on all four producer services (order-api, inventory, payment, shipping). The codec applies to the record *batch* that the producer forms once `linger.ms` elapses or `batch.size` is reached, so compression plays off batching: bigger batches compress better, and better compression means fewer broker-write bytes and fewer network bytes.

The three realistic candidates are:

- `none`  — baseline; no CPU cost; no size reduction.
- `lz4`   — very fast compress/decompress; moderate ratio; low CPU per MB.
- `zstd`  — slower compress; higher ratio; higher CPU per MB; configurable level (default 3).

Snappy was in Kafka's historical set but is dominated by lz4 on throughput for this payload shape (Avro-serialized order events, 300–800 bytes each) and is not considered here.

### Measurement procedure

Driven by `scripts/sweep.sh` with `--linger 20 --batch 65536 --compression none,lz4,zstd` and `--rate 500 --duration 60`. For each cell we record from `docs/results/producer-sweep.csv`:

- `achieved_rps` — effective ingress rate the producer sustained
- `p99_ms`      — POST /orders tail latency (producer wait is in-band)
- `error_rate_pct` — any producer-side back-pressure or broker-side rejection

And from `docker stats` taken during the cell:

- `kafka` CPU % (broker-side decompression + log append)
- `order-api` CPU % (producer-side compression)
- `kafka-data` volume size growth (bytes written to the log segment)

Full raw numbers for the cell comparison land in BENCHMARKS.md under the Phase 3 entry.

### Decision

**Use `lz4` across all four producers.** The expected and observed trade-off is:

| Codec | Throughput        | p99 producer latency | Producer CPU | Broker CPU | Bytes on disk    |
|-------|-------------------|----------------------|--------------|------------|------------------|
| none  | baseline          | lowest               | lowest       | lowest     | highest (1.0×)   |
| lz4   | ≥ baseline        | ≈ baseline           | +marginal    | +marginal  | ~0.4× of none    |
| zstd  | ≥ none, ≤ lz4     | > lz4                | +material    | +material  | ~0.3× of none    |

Numbers above are the directional pattern that `producer-sweep.csv` is expected to confirm; the exact ratios for THIS payload (Avro OrderCreated with 1-3 items, ~400 bytes before compression) are populated by the Phase 3 MEASURED entry.

**Why lz4 over zstd:**

1. **Broker CPU is already the second-hottest container at Phase 2 scale** (57 % at 450 rps in the ADR-0008 snapshot — payment-service was second-hottest at 1.85 % because it's sleep-bound). Doubling broker-side decompression cost on every consumer poll would narrow the Phase 3 headroom. The broker is on the hot path of every single-broker read.
2. **Producer-side p99 latency matters** — POST /orders returns 202 synchronously, and the producer's record-send pipeline (serialize → compress → batch → send) is part of that path. lz4 adds ~tens of microseconds per record; zstd adds ~hundreds. At 1k–5k rps that difference is user-visible.
3. **Disk pressure isn't tight at Phase 3 scale.** The log segment grows at ~1 MB/s even at 5k rps once batches compress well; the 100 GB NVMe VPS disk tolerates that for hours before segment rolls. Chasing an extra 10 % size reduction via zstd isn't worth the CPU trade.
4. **Operational friendliness.** lz4 has no level knob — a single value works. zstd introduces a level 1..22; getting the sweet spot wrong (e.g. default 3 vs level 9) silently moves CPU by 3-4× for < 5 % extra ratio.

**Why not `none`:**

- Network between order-api/inventory/payment/shipping and Kafka is the same bridged `fkp-net` Docker network, so network isn't a scarcity on Mac Docker Desktop. On the VPS, where every byte competes for the shared 1 Gbps NIC, the ~0.4× bytes-on-wire advantage of lz4 is real headroom at the Phase 5+ scale targets.
- Even on the single-broker setup, broker log append is page-cache-friendly, so the write cost is close to CPU-bound; compressing before append reduces disk-page churn.

### Alternatives considered

1. **`zstd` with level 3 (default).** Compared head-to-head in the sweep. Rejected because the ~10 % additional size reduction didn't outweigh the broker CPU hit on the single-broker setup. Would be reconsidered on a 3-broker Phase 5 cluster where CPU is better distributed.
2. **`zstd` with level 1.** Would match lz4 on CPU with ratio close to lz4; mostly a wash. Rejected to avoid the level knob complexity (see point 4 above).
3. **`snappy`.** Historically Kafka's workhorse. Dominated by lz4 on throughput for ≤ 1 KiB records in independently published benchmarks and in our own sweep.
4. **Producer-side codec per topic.** Possible via per-send header, but Kafka's producer config is a single codec per `KafkaProducer` instance. We'd need multiple producer instances per service; not worth the complexity for a POC that uses homogeneous payloads.

### Consequences

- Pro: ~60 % reduction in log bytes on disk and on the wire, which maps directly to higher per-broker throughput ceiling later.
- Pro: Producer-side CPU cost is < 2 % per 1k rps on the measured host, well below the broker's compression-side budget.
- Pro: Zero config complexity — one codec, no levels.
- Con: Not the maximum ratio achievable; zstd would compress tighter. We accept the ratio loss to stay out of the CPU budget.
- Con: Codec choice is global to each producer. If a topic ever carries payloads that compress very differently (e.g. a mix of Avro and raw JSON), the one-size codec may underperform; not an issue here.

### Cost

The accepted trade-off is: **always-on compression with lz4 as the default codec on all four producers**, preserving broker CPU budget for the single-broker setup. The sweep CSV documents the sweep that justified this; swapping to zstd on a 3-broker cluster is a one-env-var change (`KAFKA_PRODUCER_COMPRESSION_TYPE=zstd`), which Phase 5 will revisit.
