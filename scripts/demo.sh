#!/usr/bin/env bash
# Phase 4 + 5 demonstration with two INDEPENDENT relays using the MOCK source.
#
#   1. client registers with relay A and relay B (separate subscriptions)
#   2. foreground: a live change arrives over SSE
#   3. client goes offline; changes are missed on both relays, with
#      overlapping but different coverage, plus a duplicate
#   4. client reconnects and catches up from A (first available)
#   5. client syncs every relay: B's extra change arrives, overlaps dedup
#   6. relay A restarts: journal, subscriptions and cursors survive
#   7. relay A goes down: the client fails over to B
#
# Mock events are synthetic ("kind":"mock_change", "source":"mock").
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=${WORK:-$ROOT/.demo/mock}
SPACE=${SPACE:-uhC0kDemoSpaceDemoSpaceDemoSpaceDemoSpaceDemoSpaceDem}
KEY=${KEY:-demo-registration-key-please-change}
A_PORT=${A_PORT:-8787}; A_ADMIN=${A_ADMIN:-9787}
B_PORT=${B_PORT:-8788}; B_ADMIN=${B_ADMIN:-9788}
RELAY="$ROOT/target/debug/hc-push-relay"
CLIENT="$ROOT/target/debug/hc-push-client"

say() { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }
fail() { printf '\n\033[1;31mFAIL: %s\033[0m\n' "$*"; exit 1; }

say "Building (cargo build -p hc_push_relay -p hc_push_client)"
(cd "$ROOT" && cargo build -q -p hc_push_relay -p hc_push_client)
rm -rf "$WORK"; mkdir -p "$WORK"

declare -A PID
start_relay() { # name port admin
  HC_PUSH_SOURCE=mock HC_PUSH_SPACES="$SPACE" \
  HC_PUSH_REGISTRATION=key HC_PUSH_REGISTRATION_KEYS="$KEY" \
  HC_PUSH_BIND="127.0.0.1:$2" HC_PUSH_ADMIN_BIND="127.0.0.1:$3" \
  HC_PUSH_DATA_DIR="$WORK/relay-$1" HC_PUSH_LOG_FORMAT=json \
  "$RELAY" >>"$WORK/relay-$1.log" 2>&1 &
  PID[$1]=$!
  for _ in $(seq 1 50); do curl -sf "http://127.0.0.1:$2/v1/health" >/dev/null && return; sleep 0.1; done
  fail "relay $1 did not start"
}
stop_relay() { kill -TERM "${PID[$1]}" 2>/dev/null || true; wait "${PID[$1]}" 2>/dev/null || true; }
cleanup() { for n in "${!PID[@]}"; do stop_relay "$n"; done; }
trap cleanup EXIT

inject() { # admin-port id...
  local port=$1; shift
  local ids; ids=$(printf '"%s",' "$@"); ids="[${ids%,}]"
  curl -sf -X POST "http://127.0.0.1:$port/admin/v1/mock/changes" \
    -H 'content-type: application/json' -d "{\"space\":\"$SPACE\",\"ids\":$ids}"
  echo
}
C() {
  "$CLIENT" --state "$WORK/client.json" --space "$SPACE" --registration-key "$KEY" \
    --relay "a=http://127.0.0.1:$A_PORT" --relay "b=http://127.0.0.1:$B_PORT" --label demo-phone "$@"
}
ids_of() { grep -o '"type":"mock","id":"[^"]*"' "$1" | sed 's/.*"id":"//; s/"$//' | tr '\n' ' '; }

say "Starting two independent relays (separate journals, no coordinator)"
start_relay a "$A_PORT" "$A_ADMIN"
start_relay b "$B_PORT" "$B_ADMIN"
echo "relay A: $(curl -sf "http://127.0.0.1:$A_PORT/v1/capabilities" | grep -o '"journal_id":"[^"]*"')"
echo "relay B: $(curl -sf "http://127.0.0.1:$B_PORT/v1/capabilities" | grep -o '"journal_id":"[^"]*"')"

say "1. Client registers with both relays"
C register
C status

say "2. Foreground: client holds an SSE stream to A; a change arrives live"
C listen --relay a --idle-secs 2 --max-secs 10 >"$WORK/live.jsonl" &
LISTEN=$!
sleep 0.7
inject "$A_ADMIN" live-1 >/dev/null
wait $LISTEN
echo "received live: $(ids_of "$WORK/live.jsonl")"
grep -q live-1 "$WORK/live.jsonl" || fail "live notification not received"

say "3. Client is offline. Changes happen; relays see overlapping but different sets"
inject "$A_ADMIN" op-1 op-2 op-3 op-4 op-5 live-1   # live-1 again: journal dedups
inject "$B_ADMIN" op-3 op-4 b-only-1               # B misses op-1/2/5 but saw b-only-1

say "4. Client reconnects: sync (first available relay)"
C sync --policy first >"$WORK/sync1.jsonl"
echo "delivered: $(ids_of "$WORK/sync1.jsonl")"
[ "$(grep -c . "$WORK/sync1.jsonl")" = 5 ] || fail "expected 5 missed events from A"

say "5. Client syncs every relay for coverage: B adds b-only-1, op-3/op-4 are duplicates"
C sync --policy all >"$WORK/sync2.jsonl"
echo "delivered: $(ids_of "$WORK/sync2.jsonl")"
[ "$(ids_of "$WORK/sync2.jsonl")" = "b-only-1 " ] || fail "expected only b-only-1"

say "6. Relay A restarts (SIGTERM, graceful); journal and subscriptions persist"
stop_relay a
start_relay a "$A_PORT" "$A_ADMIN"
inject "$A_ADMIN" after-restart >/dev/null
C sync --policy first >"$WORK/sync3.jsonl"
echo "delivered: $(ids_of "$WORK/sync3.jsonl")"
grep -q after-restart "$WORK/sync3.jsonl" || fail "persistence across restart"

say "7. Relay A goes down; a change reaches B only; client fails over"
stop_relay a
inject "$B_ADMIN" while-a-down >/dev/null
C sync --policy first >"$WORK/sync4.jsonl"
echo "delivered: $(ids_of "$WORK/sync4.jsonl")"
grep -q while-a-down "$WORK/sync4.jsonl" || fail "failover to B"

say "Final client state (cursors are per relay and never shared)"
C status

printf '\n\033[1;32mDEMO PASSED\033[0m  logs and outputs in %s\n' "$WORK"
