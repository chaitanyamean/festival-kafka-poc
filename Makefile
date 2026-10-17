# festival-kafka-poc — Phase 1 Makefile
#
# All targets wrap `docker compose -f infra/docker-compose.yml`. Parameters are
# tunable via environment variables with sensible defaults, so the vanilla
# invocation (`make demo-phase1`) runs an end-to-end sanity demo.

SHELL := /bin/bash

COMPOSE_FILE := infra/docker-compose.yml
COMPOSE      := docker compose -f $(COMPOSE_FILE)

# -------- Tunable parameters --------
LOAD_RPS           ?= 100
LOAD_DURATION_SEC  ?= 60
LOAD_MAX_ITEMS     ?= 3
LOAD_TIMEOUT_MS    ?= 5000
LOAD_BAD_SKU_RATE  ?= 0.01
HEALTH_WAIT_SECS   ?= 240
LAG_DRAIN_SECS     ?= 90

# App services polled for health (compose container names are fkp-<service>)
APP_SERVICES := order-api inventory-service payment-service shipping-service notification-service analytics-service

.DEFAULT_GOAL := help

# -------- Housekeeping --------

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z0-9_.-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN { FS = ":.*?## " }; { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 }'

.PHONY: bootstrap
bootstrap: ## Check prerequisites (docker + compose on PATH)
	@echo "--- Docker ---"       && docker --version
	@echo "--- Compose ---"      && docker compose version
	@echo
	@echo "OK: prerequisites present."

.PHONY: clean
clean: ## Remove build artifacts (Maven targets) and prune compose state + dangling images
	@echo "Removing Maven target/ directories..."
	@rm -rf common-events/target services/*/target
	@echo "Stopping compose stack and removing volumes..."
	-@$(COMPOSE) down -v --remove-orphans
	@echo "Pruning dangling Docker images..."
	-@docker image prune -f
	@echo "Done."

# -------- Compose lifecycle --------

.PHONY: build
build: ## Build all service images (uses Docker buildkit cache)
	$(COMPOSE) build

.PHONY: up
up: ## Build (if needed) and start infra + services in the background
	$(COMPOSE) up -d --build

.PHONY: down
down: ## Stop everything. Removes volumes unless KEEP_VOLUMES=1.
	@if [ -n "$(KEEP_VOLUMES)" ]; then $(COMPOSE) down; else $(COMPOSE) down -v; fi

.PHONY: ps
ps: ## List compose services
	$(COMPOSE) ps

.PHONY: logs
logs: ## Tail logs for one service: make logs SERVICE=order-api
	@if [ -z "$(SERVICE)" ]; then echo "Usage: make logs SERVICE=<name>"; exit 2; fi
	$(COMPOSE) logs -f $(SERVICE)

# -------- Phase 1 demo pieces --------

.PHONY: wait-healthy
wait-healthy: ## Block until every app service reports healthy
	@echo "Waiting for app services to be healthy (timeout $(HEALTH_WAIT_SECS)s)..."
	@all_healthy=0; \
	for i in $$(seq 1 $(HEALTH_WAIT_SECS)); do \
	  ok=1; \
	  for svc in $(APP_SERVICES); do \
	    s=$$(docker inspect -f '{{.State.Health.Status}}' fkp-$$svc 2>/dev/null || echo missing); \
	    if [ "$$s" != "healthy" ]; then ok=0; break; fi; \
	  done; \
	  if [ $$ok -eq 1 ]; then all_healthy=1; echo "All $(words $(APP_SERVICES)) app services healthy at t=$${i}s."; break; fi; \
	  sleep 1; \
	done; \
	if [ $$all_healthy -ne 1 ]; then \
	  echo; echo "ERROR: services not healthy within $(HEALTH_WAIT_SECS)s. Current state:"; \
	  $(COMPOSE) ps; exit 1; \
	fi

.PHONY: run-load
run-load: ## Run load-generator against order-api
	@echo "Running load: $(LOAD_RPS) rps for $(LOAD_DURATION_SEC)s (max-items=$(LOAD_MAX_ITEMS), bad-sku-rate=$(LOAD_BAD_SKU_RATE))..."
	$(COMPOSE) --profile load run --rm load-generator \
	  --host http://order-api:8080 \
	  --rate $(LOAD_RPS) \
	  --duration $(LOAD_DURATION_SEC) \
	  --max-items $(LOAD_MAX_ITEMS) \
	  --timeout-ms $(LOAD_TIMEOUT_MS) \
	  --bad-sku-rate $(LOAD_BAD_SKU_RATE)

.PHONY: drain
drain: ## Wait a fixed interval for consumers to catch up
	@echo "Draining $(LAG_DRAIN_SECS)s so consumers can finish the backlog..."
	@sleep $(LAG_DRAIN_SECS)

.PHONY: saga-stats
saga-stats: ## Print saga counts, order status distribution, and consumer group lag
	@echo
	@echo "===== saga_counts (analytics-service) ====="
	@docker exec fkp-postgres psql -U fkp -d analyticsdb -c \
	  "SELECT lpad(event_count::text, 10) AS count, event_type FROM saga_counts ORDER BY event_type;"
	@echo
	@echo "===== GMV + failure rate (analytics-service) ====="
	@docker exec fkp-postgres psql -U fkp -d analyticsdb -c "\
	  WITH c AS (SELECT event_type, event_count FROM saga_counts) \
	  SELECT \
	    (SELECT value FROM metrics_counters WHERE name='gmv_cents_total') AS gmv_cents_total, \
	    (SELECT event_count FROM c WHERE event_type='order.created')      AS orders_created, \
	    (SELECT event_count FROM c WHERE event_type='order.cancelled')    AS orders_cancelled, \
	    ROUND(100.0 * (SELECT event_count FROM c WHERE event_type='order.cancelled') \
	      / NULLIF((SELECT event_count FROM c WHERE event_type='order.created'), 0), 2) AS failure_rate_pct;"
	@echo
	@echo "===== orders status distribution (order-api) ====="
	@docker exec fkp-postgres psql -U fkp -d orderdb -c \
	  "SELECT status, COUNT(*) AS orders FROM orders GROUP BY status ORDER BY status;"
	@echo
	@echo "===== payments status distribution (payment-service) ====="
	@docker exec fkp-postgres psql -U fkp -d paymentdb -c \
	  "SELECT status, COUNT(*) AS payments FROM payments GROUP BY status ORDER BY status;" || true
	@echo
	@echo "===== shipments total (shipping-service) ====="
	@docker exec fkp-postgres psql -U fkp -d shippingdb -c \
	  "SELECT COUNT(*) AS shipments FROM shipments;" || true
	@echo
	@echo "===== consumer group lag ====="
	@docker exec fkp-kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --all-groups 2>/dev/null \
	  | awk 'NR == 1 || /^[a-z]/' | head -40 || true

.PHONY: verify-zero-loss
verify-zero-loss: ## Phase 1 acceptance: assert every order terminated (CONFIRMED or CANCELLED)
	@bash infra/verify_zero_loss.sh

.PHONY: demo-phase1
demo-phase1: ## End-to-end Phase 1 demo. Stack is kept running; call `make down` when finished.
	$(MAKE) up
	$(MAKE) wait-healthy
	$(MAKE) run-load
	$(MAKE) drain
	$(MAKE) saga-stats
	@echo
	@echo "---- Acceptance check ----"
	@$(MAKE) verify-zero-loss
	@echo
	@echo "Phase 1 demo complete. Stack is still running."
	@echo "Capture the load-generator summary + saga stats above into docs/BENCHMARKS.md."
	@echo "Run 'make down' to stop everything (or 'make down KEEP_VOLUMES=1' to retain state)."

# -------- Convenience shortcuts --------

.PHONY: psql
psql: ## Open a psql shell: make psql DB=orderdb (default orderdb)
	@DB=$${DB:-orderdb}; \
	case $$DB in \
	  orderdb|inventorydb|paymentdb|shippingdb|notificationdb|analyticsdb) ;; \
	  *) echo "Unknown DB '$$DB'. Options: orderdb|inventorydb|paymentdb|shippingdb|notificationdb|analyticsdb"; exit 2 ;; \
	esac; \
	echo "psql into fkp-postgres / $$DB (type \\q to exit)"; \
	docker exec -it fkp-postgres psql -U fkp -d $$DB

.PHONY: topics
topics: ## Create or update Kafka topics from infra/topics.yaml (idempotent)
	$(COMPOSE) up --no-deps topic-init
	@echo
	@echo "--- Topics after bootstrap ---"
	@docker exec fkp-kafka kafka-topics --bootstrap-server kafka:9092 --list | sort

.PHONY: topics-list
topics-list: ## List current Kafka topics without creating any
	@docker exec fkp-kafka kafka-topics --bootstrap-server kafka:9092 --list | sort

# =============================================================================
# Phase 2 — observability stack, scenarios, raw-broker bench
# =============================================================================

# Compose invocation with the observability profile enabled. Use for any target
# that needs Prometheus / Grafana / Jaeger / OTel Collector / kafka-exporter to
# be up alongside the app services.
COMPOSE_OBS := $(COMPOSE) --profile observability

# -------- Phase 2 tunables --------
RAMP_FROM_RPS         ?= 100
RAMP_TO_RPS           ?= 1000
RAMP_DURATION_SEC     ?= 300
SPIKE_BASELINE_RPS    ?= 100
SPIKE_MULTIPLIER      ?= 10
SPIKE_BASELINE_SEC    ?= 60
SPIKE_SPIKE_SEC       ?= 60
SPIKE_COOLDOWN_SEC    ?= 30
LD_RPS                ?= 200
LD_DURATION_SEC       ?= 120
LD_HOT_SKUS           ?= SKU-00001,SKU-00002,SKU-00003
LD_HOT_SHARE          ?= 0.8
REPORT_DIR            ?= docs/benchmarks/raw/latest

# -------- Observability lifecycle --------

.PHONY: up-obs
up-obs: ## Build + start the full stack including the observability profile
	$(COMPOSE_OBS) up -d --build

.PHONY: down-obs
down-obs: ## Stop everything (observability included). Removes volumes unless KEEP_VOLUMES=1.
	@if [ -n "$(KEEP_VOLUMES)" ]; then $(COMPOSE_OBS) down; else $(COMPOSE_OBS) down -v; fi

.PHONY: wait-healthy-obs
wait-healthy-obs: ## Block until app services AND prometheus/grafana/jaeger/otel-collector report healthy
	@echo "Waiting for app + observability services to be healthy (timeout $(HEALTH_WAIT_SECS)s)..."
	@all_healthy=0; \
	TARGETS="$(APP_SERVICES) prometheus grafana jaeger otel-collector"; \
	for i in $$(seq 1 $(HEALTH_WAIT_SECS)); do \
	  ok=1; \
	  for svc in $$TARGETS; do \
	    s=$$(docker inspect -f '{{.State.Health.Status}}' fkp-$$svc 2>/dev/null || echo missing); \
	    if [ "$$s" != "healthy" ]; then ok=0; break; fi; \
	  done; \
	  if [ $$ok -eq 1 ]; then all_healthy=1; echo "All services healthy at t=$${i}s."; break; fi; \
	  sleep 1; \
	done; \
	if [ $$all_healthy -ne 1 ]; then \
	  echo; echo "ERROR: services not healthy within $(HEALTH_WAIT_SECS)s. Current state:"; \
	  $(COMPOSE_OBS) ps; exit 1; \
	fi

.PHONY: dashboards
dashboards: ## Print the URLs for Grafana / Prometheus / Jaeger / Kafka UI
	@echo
	@echo "---- Phase 2 observability endpoints (bound to 127.0.0.1 by default) ----"
	@printf "  Grafana      : http://localhost:3000  (anonymous Viewer; admin/admin for edits)\n"
	@printf "  Prometheus   : http://localhost:9090\n"
	@printf "  Jaeger UI    : http://localhost:16686\n"
	@printf "  Kafka UI     : http://localhost:8090\n"
	@printf "  Schema Reg   : http://localhost:8081\n"
	@printf "  Order API    : http://localhost:8080  (swagger: /swagger-ui.html)\n"
	@echo
	@echo "Dashboards (festival-kafka-poc folder in Grafana):"
	@echo "  1 — Topic throughput             http://localhost:3000/d/fkp-topic-throughput"
	@echo "  2 — Consumer lag                 http://localhost:3000/d/fkp-consumer-lag"
	@echo "  3 — End-to-end order latency     http://localhost:3000/d/fkp-order-e2e-latency"
	@echo "  4 — Kafka producer internals     http://localhost:3000/d/fkp-producer"
	@echo "  5 — Broker                       http://localhost:3000/d/fkp-broker"
	@echo "  6 — Saga outcomes                http://localhost:3000/d/fkp-saga-outcomes"

# -------- Scenario runners (Phase 2) --------
#
# Each target mounts $(REPORT_DIR) into the container so the load generator
# can drop a JSON report alongside the raw transcripts captured by the demo
# target.  Overriding REPORT_DIR routes reports to a different folder per run.

.PHONY: load-steady
load-steady: ## Run the steady-100-60s scenario profile (Phase 1 equivalent)
	@mkdir -p $(REPORT_DIR)
	$(COMPOSE_OBS) --profile load run --rm \
	  -v $(PWD)/$(REPORT_DIR):/report \
	  load-generator \
	  --host http://order-api:8080 \
	  --scenario-file classpath:scenarios.yaml --profile steady-100-60s \
	  --max-items $(LOAD_MAX_ITEMS) --sku-count 1000 \
	  --bad-sku-rate $(LOAD_BAD_SKU_RATE) --timeout-ms $(LOAD_TIMEOUT_MS) \
	  --report-json /report/load-steady.json

.PHONY: load-ramp
load-ramp: ## Run the ramp scenario (defaults: 100→1000 rps over 5 min)
	@mkdir -p $(REPORT_DIR)
	$(COMPOSE_OBS) --profile load run --rm \
	  -v $(PWD)/$(REPORT_DIR):/report \
	  load-generator \
	  --host http://order-api:8080 \
	  --scenario ramp \
	  --from-rps $(RAMP_FROM_RPS) --to-rps $(RAMP_TO_RPS) --duration $(RAMP_DURATION_SEC) \
	  --max-items $(LOAD_MAX_ITEMS) --sku-count 1000 \
	  --bad-sku-rate $(LOAD_BAD_SKU_RATE) --timeout-ms $(LOAD_TIMEOUT_MS) \
	  --report-json /report/load-ramp.json

.PHONY: load-festival-spike
load-festival-spike: ## Run the festival-spike scenario (baseline → 10x spike → cooldown)
	@mkdir -p $(REPORT_DIR)
	$(COMPOSE_OBS) --profile load run --rm \
	  -v $(PWD)/$(REPORT_DIR):/report \
	  load-generator \
	  --host http://order-api:8080 \
	  --scenario festival-spike \
	  --baseline-rps $(SPIKE_BASELINE_RPS) --spike-multiplier $(SPIKE_MULTIPLIER) \
	  --baseline-duration $(SPIKE_BASELINE_SEC) --spike-duration $(SPIKE_SPIKE_SEC) \
	  --cooldown-duration $(SPIKE_COOLDOWN_SEC) \
	  --max-items $(LOAD_MAX_ITEMS) --sku-count 1000 \
	  --bad-sku-rate $(LOAD_BAD_SKU_RATE) --timeout-ms $(LOAD_TIMEOUT_MS) \
	  --report-json /report/load-festival-spike.json

.PHONY: load-lightning-deal
load-lightning-deal: ## Run the lightning-deal scenario (80% of items on 3 hot SKUs)
	@mkdir -p $(REPORT_DIR)
	$(COMPOSE_OBS) --profile load run --rm \
	  -v $(PWD)/$(REPORT_DIR):/report \
	  load-generator \
	  --host http://order-api:8080 \
	  --scenario lightning-deal \
	  --rate $(LD_RPS) --duration $(LD_DURATION_SEC) \
	  --hot-skus "$(LD_HOT_SKUS)" --hot-sku-share $(LD_HOT_SHARE) \
	  --max-items $(LOAD_MAX_ITEMS) --sku-count 1000 \
	  --bad-sku-rate $(LOAD_BAD_SKU_RATE) --timeout-ms $(LOAD_TIMEOUT_MS) \
	  --report-json /report/load-lightning-deal.json

# -------- Raw broker benchmark (Phase 2) --------

.PHONY: raw-broker-bench
raw-broker-bench: ## Run kafka-producer-perf-test + kafka-consumer-perf-test for a broker-only ceiling
	bash scripts/raw-broker-bench.sh

# -------- Phase 2 end-to-end demo --------

.PHONY: demo-phase2
demo-phase2: ## End-to-end Phase 2 demo: bring up observability stack, run ramp, print URLs
	$(MAKE) up-obs
	$(MAKE) wait-healthy-obs
	@echo
	@echo "---- Ramp from $(RAMP_FROM_RPS) → $(RAMP_TO_RPS) rps over $(RAMP_DURATION_SEC)s ----"
	$(MAKE) load-ramp
	$(MAKE) drain
	$(MAKE) saga-stats
	@echo
	@echo "---- Acceptance check ----"
	@$(MAKE) verify-zero-loss || echo "Zero-loss check FAILED — expected if the ramp drove the stack past its ceiling. See dashboards."
	$(MAKE) dashboards
	@echo
	@echo "Phase 2 demo complete. Stack is still running."
	@echo " * Browse dashboards and Jaeger using the URLs above."
	@echo " * To stop: 'make down-obs' (or 'make down-obs KEEP_VOLUMES=1' to retain state)."
	@echo " * Report JSON: $(REPORT_DIR)/load-ramp.json"
