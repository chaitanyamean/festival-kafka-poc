# festival-kafka-poc

Event-driven order platform that simulates **Amazon Great Indian Festival 2026** traffic. A walkthrough showing how the system scales in stages from **100 orders/s to a projected 1M events/s**, and which tool or pattern is introduced at each stage and why.

## Stack

- **Java 21**, Spring Boot 3.3+, Spring Kafka, Maven multi-module
- **Kafka** in KRaft mode (no ZooKeeper), **Confluent Schema Registry**, **Avro** schemas
- **PostgreSQL** (one database per service), **Redis** (hot-SKU stock counters; Phase 5+)
- **Docker Compose** for local/VPS; **kind + KEDA** for the autoscaling phase (Phase 6)
- Observability (Phase 3+): Micrometer, Prometheus, Grafana, kafka-exporter, OpenTelemetry → Jaeger
- Load generation: `kafka-producer-perf-test` and a custom Java generator

## Services

| Service | Role |
|---|---|
| `order-api` | REST ingress; emits `order.created` |
| `inventory-service` | Reserves stock; emits `inventory.reserved` or `inventory.failed` |
| `payment-service` | Captures payment; emits `payment.captured` or `payment.failed` |
| `shipping-service` | Creates shipment; emits `shipment.created` |
| `notification-service` | Independent consumer; sends/logs notifications |
| `analytics-service` | Independent consumer; aggregates lifecycle metrics |
| `outbox-relay` | Phase 4+. Reads transactional outbox tables, publishes to Kafka |
| `load-generator` | Posts orders to `order-api` at a configurable RPS |

## Saga flow (choreographed)

```
order-api ──order.created──▶ inventory-service ──inventory.reserved──▶ payment-service ──payment.captured──▶ shipping-service ──shipment.created─▶ (end)
                                     │                                          │
                              inventory.failed                            payment.failed
                                     │                                          │
                                     ▼                                          ▼
                             order.cancelled                       (release reservation → order.cancelled)
```

`notification-service` and `analytics-service` consume **every** lifecycle topic in their own consumer groups — they are the only truly independent consumers.

## Phases

See [`docs/PHASES.md`](docs/PHASES.md) for the full plan. Short version:

| Phase | Target | Key change |
|---|---|---|
| 1 | ~100–2k orders/s | Walking skeleton; naive dual-write; idempotent consumers |
| 2 | ~1k orders/s | Reliability knobs (`acks=all`, idempotent producer, DLT, compensations) |
| 3 | Visible | Observability stack (Prometheus/Grafana/OTel/kafka-exporter) |
| 4 | ~5k orders/s | Transactional outbox + outbox-relay (fixes dual-write) |
| 5 | ~20–50k events/s | Partition fan-out, Redis hot-SKU counters, consumer group split |
| 6 | kind + KEDA | Lag-based autoscaling of consumer deployments |
| 7 | **PROJECTED** 1M events/s | Capacity math; measured anchors from Phases 1–6 |

## Quickstart

```bash
# One-time
make bootstrap          # verifies prerequisites, builds parent, generates Avro
# Phase 1 demo
make demo-phase1        # brings infra + services up, runs load, prints saga stats
make down               # stops everything
```

> Prerequisites: Docker 24+, Docker Compose v2, GNU Make. Java/Maven are **not** required on the host — services build inside multi-stage Dockerfiles.

### Running integration tests locally on macOS

The Phase 1 Testcontainers tests use Docker to spin up Kafka, Schema Registry, and Postgres. On **Docker Desktop 4.35+ for Mac**, enable **Settings → Advanced → "Allow the default Docker socket to be used"** before running `mvn test`. Without it, Docker Desktop's socket mediation returns stub responses that older docker-java clients can't handle. On Linux (including the measurement VPS) this is a non-issue — Docker Engine is accessed directly. See [ADR-0005](docs/DECISIONS.md).

## Rules this repo lives by

- Every event carries `eventId` (UUID), `orderId`, `eventType`, `occurredAt`, `schemaVersion`, `traceId`. Partition key = `orderId` for lifecycle topics.
- All consumers are idempotent (dedupe on `eventId`). Delivery is **at-least-once**. We **do not** claim exactly-once.
- No hard-coded hostnames or ports. All config via env vars with `docker-compose` defaults.
- Every phase leaves `docker compose up` working and provides `make demo-phaseN`.
- Numbers in [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) are tagged **MEASURED** (with hardware, config, date, exact command) or **PROJECTED** (with the formula). Never one presented as the other.
- Phase decisions, failures, and trade-offs tracked ADR-style in [`docs/DECISIONS.md`](docs/DECISIONS.md).

## Measurement environment

Benchmarks are run on a Hostinger KVM 2 VPS (AMD EPYC 9354P, 2 vCPU pinned, 7.8 GiB RAM, no swap, 100 GB NVMe, Ubuntu 24.04 LTS). Full hardware record is kept at the top of [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md). Development is local; benchmark runs happen on the VPS.

## Repo layout

```
.
├── pom.xml                       # Parent POM
├── common-events/                # Avro schemas + shared Kafka constants
├── services/                     # One Maven module per service
│   ├── order-api/
│   ├── inventory-service/
│   ├── payment-service/
│   ├── shipping-service/
│   ├── notification-service/
│   ├── analytics-service/
│   └── load-generator/
├── infra/
│   ├── docker-compose.yml        # Kafka KRaft + Schema Registry + Kafka UI + 1 Postgres (per-service DBs) + 6 services
│   ├── topics.yaml               # Topic layout (name, partitions, RF, configs)
│   ├── topics.sh                 # Bootstrap script that applies topics.yaml via kafka-topics
│   └── postgres/
│       └── init.sql              # Creates one database per service on first Postgres start
├── docs/
│   ├── ARCHITECTURE.md
│   ├── PHASES.md
│   ├── BENCHMARKS.md
│   ├── DECISIONS.md
│   └── VPS_RUNBOOK.md
├── Makefile                      # bootstrap, up, down, build, topics, clean, demo-phase1, …
└── README.md
```

`outbox-relay` appears in Phase 4. `k8s/` manifests appear in Phase 6.
