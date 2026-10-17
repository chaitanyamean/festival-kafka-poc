#!/usr/bin/env bash
# festival-kafka-poc — raw broker benchmark.
#
# Wraps the two perf tools that ship inside the Confluent cp-kafka image:
#   kafka-producer-perf-test  (producer-side throughput + latency)
#   kafka-consumer-perf-test  (consumer-side throughput)
#
# Why:
#   The application saga has many moving parts (Spring, Postgres, Avro
#   serialization, dedupe table, etc.). When app throughput tops out, you want
#   to know whether the broker could have gone faster. This script gives a
#   broker-only ceiling you can compare against.
#
# Usage:
#   scripts/raw-broker-bench.sh [--topic NAME]
#                               [--records N]
#                               [--record-size BYTES]
#                               [--throughput RPS|-1]
#                               [--acks 0|1|all]
#                               [--partitions N]
#                               [--replication-factor N]
#                               [--keep-topic]
#
# Defaults produce a ~1 million record run that completes in roughly 10–30 s
# on commodity hardware. --throughput=-1 means "flat out, no limit".
#
# Requirements:
#   - fkp-kafka container up (start with `make up`).
#   - The script execs inside fkp-kafka; nothing is installed on the host.
#
# Output:
#   Two blocks of perf output with headers.  A one-line summary line at the
#   very end for machine parsing:
#     RAW-BROKER-BENCH producer=<rec/s> p99=<ms> consumer=<MB/s>

set -euo pipefail

# ------------- defaults -------------
TOPIC="perf.bench"
RECORDS=1000000
RECORD_SIZE=512
THROUGHPUT=-1
ACKS=1
PARTITIONS=3
REPL=1
KEEP_TOPIC=0
CONTAINER="fkp-kafka"
BOOTSTRAP="kafka:9092"

# ------------- arg parsing -------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --topic)              TOPIC="$2";          shift 2 ;;
    --records)            RECORDS="$2";        shift 2 ;;
    --record-size)        RECORD_SIZE="$2";    shift 2 ;;
    --throughput)         THROUGHPUT="$2";     shift 2 ;;
    --acks)               ACKS="$2";           shift 2 ;;
    --partitions)         PARTITIONS="$2";     shift 2 ;;
    --replication-factor) REPL="$2";           shift 2 ;;
    --keep-topic)         KEEP_TOPIC=1;        shift   ;;
    --container)          CONTAINER="$2";      shift 2 ;;
    --bootstrap)          BOOTSTRAP="$2";      shift 2 ;;
    -h|--help)
      sed -n '2,30p' "$0"; exit 0 ;;
    *)
      echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

# ------------- preflight -------------
if ! docker inspect "$CONTAINER" >/dev/null 2>&1; then
  echo "ERROR: container '$CONTAINER' not running. Did you \`make up\`?" >&2
  exit 1
fi

exec_in_kafka() { docker exec "$CONTAINER" "$@"; }

echo "======================================================================"
echo " RAW BROKER BENCHMARK"
echo "   topic=$TOPIC records=$RECORDS record-size=${RECORD_SIZE}B"
echo "   throughput=$THROUGHPUT acks=$ACKS partitions=$PARTITIONS rf=$REPL"
echo "   container=$CONTAINER bootstrap=$BOOTSTRAP"
echo "======================================================================"

# ------------- topic bootstrap (idempotent) -------------
exec_in_kafka kafka-topics \
  --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPL" \
  >/dev/null

cleanup() {
  if [[ $KEEP_TOPIC -eq 0 ]]; then
    echo
    echo "---- cleanup: deleting topic $TOPIC ----"
    exec_in_kafka kafka-topics \
      --bootstrap-server "$BOOTSTRAP" \
      --delete --topic "$TOPIC" || true
  fi
}
trap cleanup EXIT

# ------------- producer bench -------------
echo
echo "---- 1) kafka-producer-perf-test ----"
PRODUCER_OUT=$(exec_in_kafka kafka-producer-perf-test \
  --topic "$TOPIC" \
  --num-records "$RECORDS" \
  --record-size "$RECORD_SIZE" \
  --throughput "$THROUGHPUT" \
  --producer-props \
      "bootstrap.servers=$BOOTSTRAP" \
      "acks=$ACKS" \
      "linger.ms=5" \
      "batch.size=16384" \
      "compression.type=none")
echo "$PRODUCER_OUT"

# ------------- consumer bench -------------
echo
echo "---- 2) kafka-consumer-perf-test ----"
CONSUMER_OUT=$(exec_in_kafka kafka-consumer-perf-test \
  --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" \
  --messages "$RECORDS" \
  --group "perf-bench-$(date +%s)" \
  --show-detailed-stats \
  --print-metrics 2>&1 | tail -30)
echo "$CONSUMER_OUT"

# ------------- one-line summary (regex-friendly) -------------
# Producer output example:
#   1000000 records sent, 48234.1 records/sec (23.55 MB/sec), 211.3 ms avg latency, ... 99th. ... max latency.
PROD_RATE=$(echo "$PRODUCER_OUT" | grep -oE '[0-9.]+ records/sec' | head -1 | awk '{print $1}')
PROD_P99=$( echo "$PRODUCER_OUT" | grep -oE '[0-9.]+ ms 99th' | head -1 | awk '{print $1}')
# Consumer output's last line contains:
#   start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec
# but with --show-detailed-stats the format differs. Use the comma-separated
# summary line printed before metrics (comes from the default perf test).
CONS_SUMMARY=$(echo "$CONSUMER_OUT" | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}' | tail -1 || true)
CONS_MB_SEC=$(echo "$CONS_SUMMARY" | awk -F, '{print $4}' | tr -d ' ')

echo
echo "======================================================================"
echo " RAW-BROKER-BENCH producer=${PROD_RATE:-?} rec/s  p99=${PROD_P99:-?} ms  consumer=${CONS_MB_SEC:-?} MB/s"
echo "======================================================================"
