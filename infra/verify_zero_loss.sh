#!/usr/bin/env bash
# Phase 1 acceptance: every order.created event must have a matching terminal
# outcome (shipment.created OR order.cancelled). Pulls counts from the
# analytics-service saga_counts aggregation and asserts the invariant.
#
# Exits 0 on pass, non-zero on fail. Prints a short diagnostic in either case.

set -euo pipefail

CONTAINER="${CONTAINER:-fkp-postgres}"
DB="${DB:-analyticsdb}"
USER="${PG_USER:-fkp}"

q() {
    docker exec "$CONTAINER" psql -tAU "$USER" -d "$DB" -c "$1" | tr -d '[:space:]'
}

count() { q "SELECT event_count FROM saga_counts WHERE event_type = '$1';"; }

created=$(count "order.created")
shipped=$(count "shipment.created")
cancelled=$(count "order.cancelled")
inv_failed=$(count "inventory.failed")
pay_failed=$(count "payment.failed")

terminal=$((shipped + cancelled))
missing=$((created - terminal))

printf '  order.created      : %s\n'   "$created"
printf '  shipment.created   : %s\n'   "$shipped"
printf '  order.cancelled    : %s\n'   "$cancelled"
printf '  inventory.failed   : %s (compensation path 1)\n'  "$inv_failed"
printf '  payment.failed     : %s (compensation path 2)\n'  "$pay_failed"
printf '  terminal (ship+can): %s\n'   "$terminal"
printf '  in-flight          : %s\n'   "$missing"
echo

if [ "$created" -eq 0 ]; then
    echo "FAIL: no orders observed. Did the load generator run?"
    exit 1
fi

if [ "$missing" -eq 0 ]; then
    echo "PASS: zero lost orders. All $created orders ended CONFIRMED or CANCELLED."
    exit 0
fi

# Allow a small tolerance for sagas still in flight when verification runs.
# Default tolerance: 0 (strict). Set TOLERANCE to a positive int to loosen.
TOLERANCE="${TOLERANCE:-0}"
if [ "$missing" -le "$TOLERANCE" ]; then
    echo "PASS (within tolerance=$TOLERANCE): $missing order(s) still in flight."
    exit 0
fi

echo "FAIL: $missing order(s) did not reach a terminal state."
echo "  Common causes:"
echo "   1. Consumer group lag not drained yet. Try: make saga-stats; sleep 20; make verify-zero-loss"
echo "   2. A service crashed. Check: make ps; make logs SERVICE=<name>"
echo "   3. Dual-write loss (ADR-0004). Expected rate ~0 in a non-chaos run."
exit 1
