# ARCHITECTURE

## Scope

Walkthrough of the Phase 1 architecture (Phases 2–7 build on this base). This document reflects the current repo; evolution is tracked in [`DECISIONS.md`](DECISIONS.md).

## Topology

```
                                    ┌────────────────────┐
                                    │   Schema Registry  │
                                    │  (BACKWARD compat) │
                                    └────────┬───────────┘
                                             │ Avro
                                             │ schemas
                   ┌─────────────────────────┼─────────────────────────┐
                   │                         │                         │
         POST /orders                   ┌────▼────┐              ┌─────▼────────────┐
            │                           │  Kafka  │              │                  │
            ▼                           │ (KRaft) │              │  All consumers   │
      ┌───────────┐  order.created      │  7 topics│              │  dedupe on       │
      │ order-api │ ─────────────────▶  │ 3 parts │              │  eventId         │
      └───────────┘                     │ RF=1    │              │                  │
            │ writes                    └────┬────┘              └──────────────────┘
            ▼                                │
       ┌────────┐                            │
       │Postgres│                            │
       │orderdb │                            │
       └────────┘                            │
                                             │
         order.created                       │
             │                               │
             ▼                               ▼
      ┌───────────────┐     ┌────────────────────┐
      │  inventory-   │     │  notification-     │
      │   service     │     │     service        │  (consumes ALL 7 topics)
      └───────┬───────┘     └────────────────────┘
              │             ┌────────────────────┐
   inventory.reserved       │   analytics-       │  (consumes ALL 7 topics)
              │             │     service        │
              ▼             └────────────────────┘
      ┌───────────────┐
      │   payment-    │
      │   service     │
      └───────┬───────┘
              │
     payment.captured            payment.failed
              │                        │
              ▼                        ▼  compensation
      ┌───────────────┐     ┌───────────────┐
      │  shipping-    │     │  inventory-   │  (releases stock)
      │   service     │     │  service      │
      └───────┬───────┘     └───────┬───────┘
              │                     │
   shipment.created           order.cancelled
              │                     │
              ▼                     ▼
         (saga done)            (saga done)
```

## Services

| Service | Role | Reads | Writes |
|---|---|---|---|
| `order-api` | REST ingress | `shipment.created` (→ status=COMPLETED), `order.cancelled` (→ status=CANCELLED) | `order.created` |
| `inventory-service` | Stock reservation + compensation | `order.created`, `payment.failed` | `inventory.reserved`, `inventory.failed`, `order.cancelled` |
| `payment-service` | Simulated payment capture | `inventory.reserved` | `payment.captured`, `payment.failed` |
| `shipping-service` | Shipment creation | `payment.captured` | `shipment.created` |
| `notification-service` | Fan-out logging | all 7 lifecycle topics | — (writes `notifications_log` table only) |
| `analytics-service` | Fan-out metrics | all 7 lifecycle topics | — (writes `lifecycle_events` + `saga_counts` tables only) |

The six services share one Postgres instance; each has its own database (ADR-0007).

## Event envelope (ADR-0001)

Every lifecycle event embeds a shared `EventEnvelope` Avro record:

| Field | Type | Purpose |
|---|---|---|
| `eventId` | string (UUID) | Dedupe key in consumers. Mirrored into Kafka header `event-id`. |
| `orderId` | string (UUID) | Partition key for every lifecycle topic. |
| `eventType` | string | FQ event name (e.g. `order.created`). Mirrored into header `event-type`. |
| `occurredAt` | timestamp-millis | Producer-side event time. |
| `schemaVersion` | int | Monotonic per event type. Independent from Schema Registry version. |
| `traceId` | nullable string | OTel traceparent trace-id. Populated starting Phase 3. |

## Topics (ADR-0003)

Seven lifecycle topics, all with partition key `orderId`:

```
order.created        inventory.reserved      inventory.failed
payment.captured     payment.failed          shipment.created
order.cancelled
```

Phase 1 settings: `partitions=3`, `replicationFactor=1`. Phase 5 bumps partitions to 24–48 and moves to RF=3 across 3 brokers.

Topic layout lives in `infra/topics.yaml`; bootstrap script `infra/topics.sh` applies it via `kafka-topics --create --if-not-exists`.

## Delivery semantics

**At-least-once**, end-to-end. Specifically:

- Producers: Phase 1 uses `acks=1` (Phase 2 upgrades to `acks=all` + `enable.idempotence=true`).
- Consumers: manual `ack-mode=record`; offsets commit after the listener method returns successfully.
- Idempotency: every writer service has a `processed_events(event_id PK)` table. The dedupe insert + the business update run in the same `@Transactional` method so a crash before method return rolls both back and a retry re-processes the event. `analytics-service` collapses this into `lifecycle_events.event_id` as its PK.

**Delivery is at-least-once. We do not claim exactly-once.** Exactly-once would require Kafka transactions tied to the service DB — out of scope for the walkthrough; Phase 4's outbox pattern is the pragmatic alternative.

## Dual-write (ADR-0004)

Writer services commit to Postgres and then publish to Kafka in separate operations. A crash between the DB commit and the Kafka send loses the event. Phase 4's transactional outbox + `outbox-relay` closes the gap. Chaos tests in Phase 2 quantify the window.

## Data model summary

| Service | Postgres DB | Tables |
|---|---|---|
| order-api | `orderdb` | `orders`, `order_items`, `processed_events` |
| inventory-service | `inventorydb` | `skus`, `reservations`, `reservation_items`, `processed_events` (seed: 5 SKUs × 100k stock) |
| payment-service | `paymentdb` | `payments`, `processed_events` |
| shipping-service | `shippingdb` | `shipments`, `processed_events` |
| notification-service | `notificationdb` | `notifications_log`, `processed_events` |
| analytics-service | `analyticsdb` | `lifecycle_events`, `saga_counts` |

## Technology choices

| Choice | Why |
|---|---|
| Spring Boot 3.3 / Java 21 | Virtual threads for I/O-heavy load generator; records for domain models; mature Spring Kafka. |
| Kafka in KRaft mode | No ZooKeeper; one less thing to run. Confluent cp-kafka 7.6 is production-ready for KRaft. |
| Confluent Schema Registry | Enforces Avro contracts at produce + consume time. BACKWARD compat is the default. |
| Avro | Compact binary; schema-on-write; explicit evolution rules. |
| Postgres per service, logical DBs on one instance | Database-per-service isolation at the schema level. One Postgres container keeps RAM usage sane on the 7.8 GiB VPS (see ADR-0007). |
| Monetary values as `long` minor units | No `BigDecimal` ceremony in generated Avro Java code (ADR-0002). |
| Docker Compose for local + VPS | Phase 6 moves to kind + KEDA; Compose is the Phase 1-5 target. |
| JdbcTemplate (not JPA) | Predictable SQL, no entity-manager overhead, straightforward dedupe + conditional UPDATE patterns. |

## Phases

See [`PHASES.md`](PHASES.md) for the full roadmap. In short:

1. **Phase 1** — Walking skeleton (this doc).
2. **Phase 2** — Reliability knobs (`acks=all`, DLT, compensations, chaos test).
3. **Phase 3** — Observability (Prometheus, Grafana, OTel, kafka-exporter).
4. **Phase 4** — Transactional outbox + relay.
5. **Phase 5** — Horizontal scale (24+ partitions, 3 brokers, Redis hot counters).
6. **Phase 6** — kind + KEDA autoscaling.
7. **Phase 7** — PROJECTED 1M events/s with explicit formulas; measured anchors from Phase 6.
