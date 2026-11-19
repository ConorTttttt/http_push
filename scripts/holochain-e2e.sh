#!/usr/bin/env bash
# Phase 3 end-to-end test against a REAL Holochain conductor.
#
# Proves: a real zome call (create_note) on Holochain 0.7.x produces DHT ops
# that go through Holochain's own validation and integration; the relay's
# Holochain source observes them through the supported `DumpOpTimings` app
# API (and the zome's post_commit signal through `on_signal`), journals them,
# and a lightweight client receives the hints over HTTP.
#
# It does NOT exercise Kitsune2 gossip between conductors: one conductor,
# no peers. See docs/holochain-integration.md.
#
# Requirements: `holochain` and `hc` 0.7.x on PATH (or HOLOCHAIN / HC env
# vars), the test hApp built (fixtures/happ/build.sh), cargo.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
HOLOCHAIN=${HOLOCHAIN:-holochain}
HC=${HC:-hc}
WORK=${WORK:-$ROOT/.demo/holochain-e2e}
ADMIN_PORT=${ADMIN_PORT:-44440}
RELAY_PORT=${RELAY_PORT:-8790}
HAPP=${HAPP:-$ROOT/fixtures/happ/workdir/notes.happ}
APP_ID=notes
PASS=e2e-test-passphrase
RELAY="$ROOT/target/debug/hc-push-relay"
CLIENT="$ROOT/target/debug/hc-push-client"

log() { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }
fail() { printf '\n\033[1;31mFAIL: %s\033[0m\n' "$*"; exit 1; }

[ -f "$HAPP" ] || fail "test hApp not built: run fixtures/happ/build.sh"
"$HOLOCHAIN" --version >/dev/null || fail "holochain binary not found"

log "Building relay and client"
(cd "$ROOT" && cargo build -q -p hc_push_relay -p hc_push_client)

rm -rf "$WORK"
mkdir -p "$WORK/conductor"
PIDS=()
cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

log "Starting Holochain conductor ($("$HOLOCHAIN" --version)), admin on 127.0.0.1:$ADMIN_PORT"
cat > "$WORK/conductor-config.yaml" <<EOF
data_root_path: $WORK/conductor
keystore:
  type: lair_server_in_proc
  lair_root: $WORK/conductor/ks
admin_interfaces:
- driver:
    type: websocket
    port: $ADMIN_PORT
    # Explicit loopback: never expose the admin websocket.
    danger_bind_addr: 127.0.0.1
    allowed_origins: '*'
network:
  # Single node, no peers: unroutable bootstrap/relay keep it offline.
  bootstrap_url: https://127.0.0.1:9/
  relay_url: https://127.0.0.1:9/
  target_arc_factor: 1
EOF
# Hosts without IPv6 (no /proc/net/if_inet6): Holochain 0.7.1 aborts while
# dual-binding its websocket. Preload a tiny errno-mapping shim so its own
# IPv4 fallback runs. Test-environment workaround; see the shim's header.
PRELOAD=""
if [ ! -e /proc/net/if_inet6 ]; then
  echo "host has no IPv6: using scripts/support/no-ipv6-shim.c for the conductor"
  cc -shared -fPIC -o "$WORK/no-ipv6-shim.so" "$ROOT/scripts/support/no-ipv6-shim.c" -ldl
  PRELOAD="$WORK/no-ipv6-shim.so"
fi
echo "$PASS" | LD_PRELOAD="$PRELOAD" "$HOLOCHAIN" --piped -c "$WORK/conductor-config.yaml" >"$WORK/holochain.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 60); do
  "$HC" client call -p "$ADMIN_PORT" list-apps >/dev/null 2>&1 && break
  sleep 0.5
done
"$HC" client call -p "$ADMIN_PORT" list-apps >/dev/null || fail "conductor did not start (see $WORK/holochain.log)"

log "Installing test hApp"
"$HC" client call -p "$ADMIN_PORT" install-app --app-id "$APP_ID" "$HAPP" >"$WORK/install.log"
"$HC" client call -p "$ADMIN_PORT" enable-app "$APP_ID" >/dev/null
DNA=$("$HC" client call -p "$ADMIN_PORT" list-dnas | grep -o 'uhC0k[A-Za-z0-9_-]*' | head -1)
[ -n "$DNA" ] || fail "could not determine DNA hash"
echo "DNA hash: $DNA"

log "Starting relay with the Holochain source on 127.0.0.1:$RELAY_PORT"
HC_PUSH_SOURCE=holochain \
HC_PUSH_HC_ADMIN_ADDR=127.0.0.1:$ADMIN_PORT \
HC_PUSH_HC_APP_ID=$APP_ID \
HC_PUSH_HC_POLL_INTERVAL_MS=300 \
HC_PUSH_REGISTRATION=open \
HC_PUSH_BIND=127.0.0.1:$RELAY_PORT \
HC_PUSH_DATA_DIR="$WORK/relay" \
HC_PUSH_LOG_FORMAT=pretty \
RUST_LOG=info,hc_push_holochain=debug \
"$RELAY" >"$WORK/relay.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 60); do
  curl -sf "http://127.0.0.1:$RELAY_PORT/v1/health" >/dev/null && break
  sleep 0.5
done
curl -sf "http://127.0.0.1:$RELAY_PORT/v1/capabilities" >"$WORK/capabilities.json" || fail "relay did not start (see $WORK/relay.log)"
grep -q "$DNA" "$WORK/capabilities.json" || fail "relay does not serve $DNA"
grep -q conductor_op_level "$WORK/capabilities.json" || fail "op-level source missing"
echo "capabilities: $(cat "$WORK/capabilities.json")"

C() { "$CLIENT" --state "$WORK/client.json" --relay "hp=http://127.0.0.1:$RELAY_PORT" --space "$DNA" "$@"; }

log "Client registers, then goes offline"
# Let the relay journal the genesis ops first so we only see the new ones.
sleep 2
C register
C sync >/dev/null

log "Real zome call on the conductor while the client is offline"
echo "$PASS" | "$HC" client zome-call-auth --piped -p "$ADMIN_PORT" "$APP_ID" >"$WORK/auth.log" 2>&1
echo "$PASS" | "$HC" client zome-call --piped -p "$ADMIN_PORT" "$APP_ID" "$DNA" notes create_note '"hello from a real conductor"' >"$WORK/zome-call.out" 2>>"$WORK/auth.log" \
  || fail "zome call failed (see $WORK/auth.log)"
echo "create_note returned: $(head -c 300 "$WORK/zome-call.out")"

log "Waiting for Holochain to validate and integrate the new ops"
for _ in $(seq 1 40); do
  n=$(curl -sf "http://127.0.0.1:$RELAY_PORT/v1/health" | grep -o '"head_cursor":"[^"]*"' | grep -o '[0-9]*"$' | tr -d '"')
  [ "${n:-0}" -gt 0 ] && sleep 1 && break
  sleep 0.5
done

log "Client comes back and catches up over HTTP"
C sync >"$WORK/events.jsonl"
cat "$WORK/events.jsonl"
OPS=$(grep -c '"kind":"op_integrated"' "$WORK/events.jsonl" || true)
SIGS=$(grep -c '"kind":"app_signal"' "$WORK/events.jsonl" || true)
echo "op_integrated events: $OPS, app_signal events: $SIGS"
[ "$OPS" -ge 1 ] || fail "no op_integrated events received"
[ "$SIGS" -ge 1 ] || fail "no app_signal event received"

log "Cross-checking op hashes against the conductor's own DumpOpTimings (admin API)"
"$HC" client call -p "$ADMIN_PORT" dump-op-timings "$DNA" --limit 1000 >"$WORK/op-timings.json"
# `hc` prints hashes as raw bytes; HoloHash base64 is "u" + base64url(39 bytes).
python3 -I - "$WORK/op-timings.json" "$WORK/events.jsonl" <<'PY' || fail "relay reported op hashes the conductor does not hold as integrated+accepted"
import base64, json, sys
dump = json.load(open(sys.argv[1]))
held = {}
for t in dump["timings"]:
    h = "u" + base64.urlsafe_b64encode(bytes(t["op_hash"])).decode().rstrip("=")
    held[h] = t
bad = 0
for line in open(sys.argv[2]):
    e = json.loads(line)
    for r in e["refs"]:
        if r["type"] != "dht_op":
            continue
        t = held.get(r["id"])
        ok = t is not None and t["when_integrated"] is not None and t["validation_status"] == "Accepted"
        print(("ok   " if ok else "BAD  ") + r["id"], "" if t is None else t["validation_status"])
        bad += not ok
sys.exit(1 if bad else 0)
PY
echo "every reported op is held, integrated and Accepted by the conductor's own validation"

log "Second sync delivers nothing new (cursor persisted, dedup)"
C sync >"$WORK/events2.jsonl"
[ ! -s "$WORK/events2.jsonl" ] || fail "events were delivered twice"

printf '\n\033[1;32mPASS: real Holochain zome call -> integrated DHT ops -> relay journal -> HTTP client\033[0m\n'
