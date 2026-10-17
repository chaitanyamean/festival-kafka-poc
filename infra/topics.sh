#!/usr/bin/env bash
# Topic bootstrap: reads infra/topics.yaml and runs `kafka-topics --create`
# (idempotently) for each entry. Designed to run inside the cp-kafka image, where
# kafka-topics is on PATH. yq is downloaded on demand if missing.
#
# Env:
#   KAFKA_BOOTSTRAP   broker list (default: kafka:9092)
#   YAML_FILE         topics config file  (default: /infra/topics.yaml)

set -euo pipefail

BS="${KAFKA_BOOTSTRAP:-kafka:9092}"
YAML_FILE="${YAML_FILE:-/infra/topics.yaml}"

if [ ! -f "$YAML_FILE" ]; then
    echo "ERROR: config file not found: $YAML_FILE" >&2
    exit 1
fi

# cp-kafka doesn't ship yq. Install v4 binary once per container lifetime.
if ! command -v yq >/dev/null 2>&1; then
    echo "yq not found; downloading mikefarah/yq v4..."
    arch=$(uname -m)
    case "$arch" in
        x86_64)         yqarch=amd64 ;;
        aarch64|arm64)  yqarch=arm64 ;;
        *)              echo "unsupported arch: $arch"; exit 1 ;;
    esac
    YQ_BIN="${YQ_BIN:-/tmp/yq}"
    curl -fsSL "https://github.com/mikefarah/yq/releases/latest/download/yq_linux_${yqarch}" -o "$YQ_BIN"
    chmod +x "$YQ_BIN"
    export PATH="$(dirname "$YQ_BIN"):$PATH"
fi

count=$(yq '.topics | length' "$YAML_FILE")
echo "Creating $count topics on $BS from $YAML_FILE..."

for i in $(seq 0 $((count - 1))); do
    name=$(yq ".topics[$i].name"               "$YAML_FILE")
    partitions=$(yq ".topics[$i].partitions"   "$YAML_FILE")
    rf=$(yq ".topics[$i].replication_factor"   "$YAML_FILE")

    # Per-topic configs (optional). If present, flatten to --config k=v args.
    cfg_args=()
    cfg_count=$(yq -r ".topics[$i].configs | length // 0" "$YAML_FILE")
    if [ "$cfg_count" -gt 0 ] 2>/dev/null; then
        while IFS= read -r kv; do
            [ -n "$kv" ] && cfg_args+=(--config "$kv")
        done < <(yq -r ".topics[$i].configs | to_entries | .[] | \"\(.key)=\(.value)\"" "$YAML_FILE")
    fi

    kafka-topics --bootstrap-server "$BS" \
        --create --if-not-exists \
        --topic "$name" \
        --partitions "$partitions" \
        --replication-factor "$rf" \
        "${cfg_args[@]}"
done

echo "---- topics after bootstrap ----"
kafka-topics --bootstrap-server "$BS" --list | sort
