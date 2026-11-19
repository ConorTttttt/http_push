#!/usr/bin/env bash
# Two-conductor variant of holochain-e2e.sh.
#
# Conductor A authors a note. The relay observes ONLY conductor B. B can
# only learn about A's ops through Kitsune2 (publish / gossip) over a local
# kitsune2-bootstrap-srv (bootstrap + embedded iroh relay). Once B has
# validated and integrated them, the relay sees them through B's
# DumpOpTimings and the client receives the hints.
#
# This shows that ops arriving from *another peer* reach a lightweight
# client via the relay, with validation done by B's conductor. It still uses
# the conductor's app API, not a Kitsune2 hook (see docs).
#
# Extra requirement: kitsune2-bootstrap-srv 0.5.x (K2_BOOTSTRAP env var), e.g.
#   cargo install kitsune2_bootstrap_srv --version =0.5.3
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
HOLOCHAIN=${HOLOCHAIN:-holochain}
HC=${HC:-hc}
K2_BOOTSTRAP=${K2_BOOTSTRAP:-kitsune2-bootstrap-srv}
WORK=${WORK:-$ROOT/.demo/holochain-gossip-e2e}
BOOT_PORT=${BOOT_PORT:-44400}
A_PORT=${A_PORT:-44441}
B_PORT=${B_PORT:-44442}
RELAY_PORT=${RELAY_PORT:-8791}
HAPP=${HAPP:-$ROOT/fixtures/happ/workdir/notes.happ}
APP_ID=notes
SEED=hc-push-gossip-test
PASS=e2e-test-passphrase
RELAY="$ROOT/target/debug/hc-push-relay"
CLIENT="$ROOT/target/debug/hc-push-client"

log() { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }
fail() { printf '\n\033[1;31mFAIL: %s\033[0m\n' "$*"; exit 1; }

[ -f "$HAPP" ] || fail "test hApp not built: run fixtures/happ/build.sh"
(cd "$ROOT" && cargo build -q -p hc_push_relay -p hc_push_client)
rm -rf "$WORK"; mkdir -p "$WORK"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT

PRELOAD=""
if [ ! -e /proc/net/if_inet6 ]; then
  cc -shared -fPIC -o "$WORK/no-ipv6-shim.so" "$ROOT/scripts/support/no-ipv6-shim.c" -ldl
  PRELOAD="$WORK/no-ipv6-shim.so"
fi

log "Local Kitsune2 bootstrap server + iroh relay on 127.0.0.1:$BOOT_PORT"
LD_PRELOAD="$PRELOAD" "$K2_BOOTSTRAP" --listen "127.0.0.1:$BOOT_PORT" >"$WORK/bootstrap.log" 2>&1 &
PIDS+=($!)
sleep 1

start_conductor() { # name port
  mkdir -p "$WORK/$1"
  cat > "$WORK/$1.yaml" <<EOF
data_root_path: $WORK/$1
keystore:
  type: lair_server_in_proc
  lair_root: $WORK/$1/ks
admin_interfaces:
- driver:
    type: websocket
    port: $2
    danger_bind_addr: 127.0.0.1
    allowed_origins: '*'
network:
  bootstrap_url: http://127.0.0.1:$BOOT_PORT
  relay_url: http://127.0.0.1:$BOOT_PORT
  target_arc_factor: 1
  advanced:
    irohTransport:
      relayAllowPlainText: true
EOF
  echo "$PASS" | LD_PRELOAD="$PRELOAD" "$HOLOCHAIN" --piped -c "$WORK/$1.yaml" >"$WORK/$1.log" 2>&1 &
  PIDS+=($!)
  for _ in $(seq 1 60); do "$HC" client call -p "$2" list-apps >/dev/null 2>&1 && return; sleep 0.5; done
  fail "conductor $1 did not start (see $WORK/$1.log)"
}

log "Starting conductors A (author) and B (observed by the relay)"
start_conductor a "$A_PORT"
start_conductor b "$B_PORT"
for p in "$A_PORT" "$B_PORT"; do
  "$HC" client call -p "$p" install-app --app-id "$APP_ID" "$HAPP" "$SEED" >/dev/null
  "$HC" client call -p "$p" enable-app "$APP_ID" >/dev/null
done
DNA=$("$HC" client call -p "$B_PORT" list-dnas | grep -o 'uhC0k[A-Za-z0-9_-]*' | head -1)
echo "DNA: $DNA"

log "Relay attached to conductor B only"
HC_PUSH_SOURCE=holochain HC_PUSH_HC_ADMIN_ADDR=127.0.0.1:$B_PORT HC_PUSH_HC_APP_ID=$APP_ID \
HC_PUSH_HC_POLL_INTERVAL_MS=300 HC_PUSH_HC_SIGNALS=false HC_PUSH_REGISTRATION=open \
HC_PUSH_BIND=127.0.0.1:$RELAY_PORT HC_PUSH_DATA_DIR="$WORK/relay" HC_PUSH_LOG_FORMAT=pretty \
"$RELAY" >"$WORK/relay.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 60); do curl -sf "http://127.0.0.1:$RELAY_PORT/v1/health" >/dev/null && break; sleep 0.5; done

C() { "$CLIENT" --state "$WORK/client.json" --relay "b=http://127.0.0.1:$RELAY_PORT" --space "$DNA" "$@"; }
C register
sleep 3
C sync >/dev/null

log "Conductor A authors a note"
echo "$PASS" | "$HC" client zome-call-auth --piped -p "$A_PORT" "$APP_ID" >"$WORK/auth.log" 2>&1
echo "$PASS" | "$HC" client zome-call --piped -p "$A_PORT" "$APP_ID" "$DNA" notes create_note '"authored on A"' >"$WORK/zome-call.out" 2>>"$WORK/auth.log" \
  || fail "zome call on A failed"
"$HC" client call -p "$A_PORT" dump-op-timings "$DNA" --limit 1000 >"$WORK/a-ops.json"

log "Waiting for B to receive (publish/gossip), validate and integrate A's ops"
python3 -I - "$WORK/a-ops.json" >"$WORK/a-ops.txt" <<'PY'
import base64, json, sys
for t in json.load(open(sys.argv[1]))["timings"]:
    print("u" + base64.urlsafe_b64encode(bytes(t["op_hash"])).decode().rstrip("="))
PY
FOUND=0
: >"$WORK/events.jsonl"
for _ in $(seq 1 90); do
  C sync >>"$WORK/events.jsonl" 2>/dev/null || true
  # Any op authored on A (not genesis of B) that reached B via the network?
  if grep -o '"type":"dht_op","id":"[^"]*"' "$WORK/events.jsonl" | sed 's/.*"id":"//; s/"$//' \
     | grep -qxFf "$WORK/a-ops.txt"; then FOUND=1; break; fi
  sleep 2
done
grep -c . "$WORK/events.jsonl" | xargs echo "events received from relay on B:"
[ "$FOUND" = 1 ] || fail "no op authored on A was observed on B within the timeout (see $WORK/*.log)"
grep -o '"type":"dht_op","id":"[^"]*"' "$WORK/events.jsonl" | sed 's/.*"id":"//; s/"$//' \
  | grep -xFf "$WORK/a-ops.txt" | sed 's/^/  op from A, integrated on B: /'

printf '\n\033[1;32mPASS: op authored on conductor A -> Kitsune2 -> validated+integrated on B -> relay -> HTTP client\033[0m\n'
