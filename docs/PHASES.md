# PHASES

Each phase ends with:

1. `docker compose up` green.
2. A `make demo-phaseN` target that runs the demo end-to-end.
3. One MEASURED entry in [`BENCHMARKS.md`](BENCHMARKS.md) from the VPS.
4. One ADR entry in [`DECISIONS.md`](DECISIONS.md) describing what we introduced, what broke, how we detected it, what we changed, what it cost.

The "target" column below is a **goal to measure against**, not a promise. The real number lands in `BENCHMARKS.md`.

---

## Phase 1 — Walking skeleton

**Target:** ~100–2k orders/s on the VPS.

**What's in:**

- All 6 services wired end-to-end: `order-api → inventory → payment → shipping`, plus `notification` and `analytics` as fan-out consumers.
- Avro schemas + Confluent Schema Registry.
- One Postgres per writer service.
- Idempotent consumers: each service has a `processed_events (event_id PK)` dedupe table.
- Partition count = 3 on every lifecycle topic. Replication factor = 1 (single broker).
- Producers use defaults (`acks=1`, non-idempotent). **The dual-write gap exists on purpose** — Phase 4 fixes it.

**What's deliberately out:**

- No DLT, no retry topics, no outbox, no observability stack, no autoscaling.
- No Redis. Inventory reservation goes straight to Postgres.
- No OTel tracing (traceId field exists in envelope but will be null).

**Demo (`make demo-phase1`):**

1. `docker compose up -d`
2. Wait for Schema Registry + all services ready.
3. Load-generator posts N orders at a configurable RPS for a fixed duration.
4. Print saga completion rate: `% of orderIds that reached shipment.created`.
5. Print per-service consumer lag and processing counts.

**Exit criterion:** saga completion rate ≥ 99% at the chosen RPS, recorded in `BENCHMARKS.md`.

---

## Phase 2 — Reliability knobs

**Target:** Match or exceed Phase 1 throughput while eliminating the "lost messages under broker restart" failure mode.

**What's in:**

- Producers: `acks=all`, `enable.idempotence=true`, `max.in.flight.requests.per.connection=5`, `retries=Integer.MAX_VALUE`.
- Consumers: manual acks (`AckMode.MANUAL_IMMEDIATE`), process-then-ack, `isolation.level=read_committed`.
- Per-consumer dead-letter topic (`.dlt` suffix) via `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`.
- Compensating event handlers:
  - `payment.failed` → `inventory-service` releases the reservation → emits `order.cancelled`.
  - `inventory.failed` → `order-api` emits `order.cancelled` (terminal).
- Chaos test: kill the broker mid-run with `docker kill`, verify no event loss.

**Exit criterion:** zero lost events under broker-kill chaos test; saga completion rate still ≥ 99%.

---

## Phase 3 — Observability

**Target:** Make the system observable without changing its throughput materially (expect ≤ 20% hit from telemetry).

**What's in:**

- Spring Boot Actuator + Micrometer Prometheus registry on every service.
- `kafka-exporter` for broker and consumer-group metrics.
- Prometheus + Grafana containers with pre-provisioned dashboards (saga completion rate, consumer lag, p99 end-to-end latency, DLT message rate).
- OpenTelemetry Java agent attached to each service; traces exported via OTLP to Jaeger.
- `traceparent` propagated through Kafka message headers.
- `traceId` field in `EventEnvelope` populated from the current span.

**Exit criterion:** a single trace in Jaeger shows all four hops of the saga for one `orderId`. Grafana dashboard shows live consumer lag per topic.

---

## Phase 4 — Transactional outbox + relay

**Target:** ~5k orders/s. Zero events lost on service crash between DB commit and Kafka send.

**What's in:**

- Each writer service writes business state + outbox row in the same Postgres transaction.
- `outbox-relay` service polls each service's outbox table, publishes to Kafka, marks rows sent. One relay instance per outbox (sharing is optional).
- Producers in business services are **removed**; only the relay produces to Kafka.
- Chaos test: `docker kill` a service after DB commit but (statistically) before relay publish → recovery completes the publish on restart with no duplication beyond what idempotent consumers already dedupe.

**Exit criterion:** zero events lost under crash-after-commit chaos. Saga completion rate ≥ 99% at target RPS.

---

## Phase 5 — Horizontal scaling within one host

**Target:** ~20–50k **events/s** total across topics (orders/s is lower; "events" counts every lifecycle topic).

**What's in:**

- Partition count on hot topics raised to 24–48. (Must be a multiple of the max consumer-instance count we'll run in Phase 6.)
- 3-broker Kafka cluster (single VPS, three broker processes, replication-factor 3).
- Redis hot-SKU reservation counters in `inventory-service` (atomic `DECRBY` on `sku:{skuId}:available`; Postgres is written asynchronously).
- `notification-service` and `analytics-service` moved to their own topic subscriptions sized for their own throughput; they stop sharing fate with the saga's critical path.
- Load generator switched to `kafka-producer-perf-test` directly against `order.created` (plus the Java generator keeps running the REST path for a slice of traffic).

**Exit criterion:** sustained target throughput for 10 minutes with saga completion rate ≥ 99% and p99 end-to-end latency < 2 s.

---

## Phase 6 — kind + KEDA autoscaling

**Target:** demonstrate autoscaling mechanism. Peak throughput still constrained by the host.

**What's in:**

- `kind` cluster (1 control plane + 1 worker, resource-trimmed for the VPS).
- Services deployed as Kubernetes Deployments. One Postgres per service via StatefulSet.
- Kafka cluster via Strimzi or the raw Confluent images, 3 brokers.
- KEDA installed. `ScaledObject` on each consumer deployment with the Kafka lag trigger (threshold tuned per topic).
- HPA on `order-api` based on CPU/RPS.
- Load-generator ramps traffic in a stepped profile; dashboards show pods scaling 1 → N.

**Exit criterion:** visible scale-out and scale-in cycle in Grafana + `kubectl get pods -w` during the load ramp.

---

## Phase 7 — Projected 1M events/s

**Target:** honest capacity math from Phase 6's measured anchors.

**What's in:**

- Capacity planning doc with:
  - Per-partition throughput ceiling measured in Phase 6.
  - Partition count needed to hit 1M events/s given that ceiling.
  - Broker count needed to host that partition count at RF=3 with headroom.
  - Consumer instance count for each service to keep lag flat.
  - Cost estimate on reference cloud SKUs (e.g. AWS `m7g.2xlarge` × N).
- Discussion of what fails first beyond this scale: hot partitions, Schema Registry throughput, Postgres I/O, operational cost.
- Table of **PROJECTED** numbers alongside the **MEASURED** anchors they extrapolate from. Every projected number carries its formula.

**Exit criterion:** the doc exists, every projection shows its formula, and nothing in it is presented as measured.
