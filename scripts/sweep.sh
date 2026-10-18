#!/usr/bin/env bash
# festival-kafka-poc — Phase 3 producer-config sweep.
#
# Varies producer linger.ms + batch.size + compression.type across a grid, runs
# `make load-ramp` for each cell, and parses the JSON report into a CSV row.
# Each cell is a fresh ramp against the *already running* stack — the stack is
# NOT restarted between cells so JVMs stay warm (fair comparison).
#
# Prerequisites: `make up-obs` already ran. Services pick up the new env values
# on each cell via `docker compose up -d --no-build order-api inventory-service
# payment-service shipping-service` which does a rolling recreate.
#
# Usage:
#   scripts/sweep.sh [--out docs/results/producer-sweep.csv]
#                    [--rate 500]
#                    [--duration 60]
#                    [--linger "0,5,20"]
#                    [--batch  "16384,65536,131072"]
#                    [--compression "none,lz4,zstd"]
#                    [--acks all]
#                    [--idempotence true]
#                    [--drain 60]
#
# Defaults produce a 3 x 3 x 3 = 27-cell sweep. Each cell:
#   1. Export the KAFKA_PRODUCER_* env vars for this cell.
#   2. docker compose up -d order-api inventory-service payment-service
#      shipping-service   (picks up the new env; rolling restart)
#   3. Wait for services to be healthy.
#   4. make load-ramp with --rate/--duration overrides, write JSON report.
#   5. Sleep DRAIN seconds for the saga to settle.
#   6. Parse JSON into CSV row.
#
# Output CSV columns:
#   linger_ms, batch_size, compression, acks, idempotence,
#   sent, status_2xx, status_5xx, errors, achieved_rps,
#   error_rate_pct, p50_ms, p95_ms, p99_ms, max_ms,
#   elapsed_sec, timestamp

set -euo pipefail

# ------------- defaults -------------
OUT="docs/results/producer-sweep.csv"
RATE=500
DURATION=60
LINGER_VALUES="0,5,20"
BATCH_VALUES="16384,65536,131072"
COMPRESSION_VALUES="none,lz4,zstd"
ACKS="all"
IDEMPOTENCE="true"
DRAIN=60

# ------------- arg parsing -------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)         OUT="$2";                 shift 2 ;;
    --rate)        RATE="$2";                shift 2 ;;
    --duration)    DURATION="$2";            shift 2 ;;
    --linger)      LINGER_VALUES="$2";       shift 2 ;;
    --batch)       BATCH_VALUES="$2";        shift 2 ;;
    --compression) COMPRESSION_VALUES="$2";  shift 2 ;;
    --acks)        ACKS="$2";                shift 2 ;;
    --idempotence) IDEMPOTENCE="$2";         shift 2 ;;
    --drain)       DRAIN="$2";               shift 2 ;;
    -h|--help)     sed -n '2,30p' "$0"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

mkdir -p "$(dirname "$OUT")"
TMPDIR_SWEEP="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_SWEEP"' EXIT

# ------------- preflight -------------
if ! docker inspect fkp-order-api >/dev/null 2>&1; then
  echo "ERROR: fkp-order-api not running. Run \`make up-obs\` first." >&2
  exit 1
fi

APP_SERVICES="order-api inventory-service payment-service shipping-service notification-service analytics-service"

# ------------- CSV header -------------
if [[ ! -f "$OUT" ]]; then
    echo "linger_ms,batch_size,compression,acks,idempotence,sent,status_2xx,status_5xx,errors,achieved_rps,error_rate_pct,p50_ms,p95_ms,p99_ms,max_ms,elapsed_sec,timestamp" > "$OUT"
fi

# ------------- sweep -------------
IFS=',' read -ra LINGERS <<< "$LINGER_VALUES"
IFS=',' read -ra BATCHES <<< "$BATCH_VALUES"
IFS=',' read -ra COMPRESSIONS <<< "$COMPRESSION_VALUES"
TOTAL=$(( ${#LINGERS[@]} * ${#BATCHES[@]} * ${#COMPRESSIONS[@]} ))
i=0

for linger in "${LINGERS[@]}"; do
for batch in "${BATCHES[@]}"; do
for compression in "${COMPRESSIONS[@]}"; do
  i=$((i + 1))
  echo
  echo "=================================================================="
  echo " [$i/$TOTAL] linger=$linger batch=$batch compression=$compression acks=$ACKS idempotence=$IDEMPOTENCE"
  echo "=================================================================="

  export KAFKA_PRODUCER_ACKS="$ACKS"
  export KAFKA_PRODUCER_ENABLE_IDEMPOTENCE="$IDEMPOTENCE"
  export KAFKA_PRODUCER_LINGER_MS="$linger"
  export KAFKA_PRODUCER_BATCH_SIZE="$batch"
  export KAFKA_PRODUCER_COMPRESSION_TYPE="$compression"

  # Rolling-restart the 4 producer services so they pick up the new env.
  # --no-build keeps the restart quick; images already built in `make up-obs`.
  docker compose -f infra/docker-compose.yml --profile observability \
      up -d --no-build order-api inventory-service payment-service shipping-service

  # Wait for health (short loop since containers are already warm).
  for _ in $(seq 1 60); do
    all=1
    for svc in $APP_SERVICES; do
      s=$(docker inspect -f '{{.State.Health.Status}}' "fkp-$svc" 2>/dev/null || echo missing)
      if [[ "$s" != "healthy" ]]; then all=0; break; fi
    done
    if [[ $all -eq 1 ]]; then break; fi
    sleep 1
  done

  # Run one load cell. Use /tmp inside the container (mounted) for the report.
  cell_dir="$TMPDIR_SWEEP/cell-$i"
  mkdir -p "$cell_dir"
  REPORT="$cell_dir/report.json"

  docker compose -f infra/docker-compose.yml --profile observability --profile load \
      run --rm \
      -v "$cell_dir:/report" \
      -e KAFKA_PRODUCER_ACKS \
      -e KAFKA_PRODUCER_ENABLE_IDEMPOTENCE \
      -e KAFKA_PRODUCER_LINGER_MS \
      -e KAFKA_PRODUCER_BATCH_SIZE \
      -e KAFKA_PRODUCER_COMPRESSION_TYPE \
      load-generator \
      --host http://order-api:8080 \
      --scenario steady --rate "$RATE" --duration "$DURATION" \
      --max-items 2 --bad-sku-rate 0.0 --timeout-ms 5000 \
      --report-json /report/report.json

  if [[ ! -f "$REPORT" ]]; then
    echo "WARN: no report produced for cell $i; skipping CSV row." >&2
    continue
  fi

  # Parse JSON → CSV. Python is less fragile than jq for nested keys with "2xx".
  row=$(python3 - "$REPORT" "$linger" "$batch" "$compression" "$ACKS" "$IDEMPOTENCE" <<'PY'
import json, sys, datetime
path, linger, batch, compression, acks, idemp = sys.argv[1:]
with open(path) as f: d = json.load(f)
s = d["status"]; l = d["latencyMs"]
print(",".join(str(x) for x in [
    linger, batch, compression, acks, idemp,
    d["sent"], s["2xx"], s["5xx"], s["errors"],
    f'{d["achievedRps"]:.2f}', f'{d["errorRatePct"]:.4f}',
    f'{l["p50"]:.3f}', f'{l["p95"]:.3f}', f'{l["p99"]:.3f}', f'{l["max"]:.3f}',
    f'{d["elapsedSec"]:.2f}',
    datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
]))
PY
)
  echo "$row" >> "$OUT"
  echo "ROW: $row"

  echo "sleep $DRAIN  (drain)"
  sleep "$DRAIN"
done
done
done

echo
echo "=================================================================="
echo " Sweep complete. $i rows appended to $OUT"
echo "=================================================================="
echo
column -t -s, "$OUT" | head -40
