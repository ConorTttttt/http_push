#!/usr/bin/env bash
# Three-conductor synchronization test.
#
#   A  authors notes.
#   B  always-on "HoloPort": receives A's ops through Kitsune2, validates and
#      integrates them, and hosts the HTTP relay.
#   C  lightweight node: its conductor is OFFLINE while A authors.
#
# Flow:
#   1. A, B, C join; C's device client subscribes to B's relay, then C goes offline.
#   2. A (and B) author notes; B integrates them; the relay journals op hints
#      (MODE=event: from the OpsIntegrated signal; MODE=polling: DumpOpTimings).
#      A second, online client ("observer") listens over SSE to measure
#      notification latency.
#   3. A goes offline, so only B can serve the data.
#   4. C's device client syncs over HTTP (conductor still stopped): gets the
#      missing op hashes (the completion criterion) and B's signed peer record.
#   5. C's conductor starts, is given B's peer record (AddAgentInfo), and
#      catches up by native Kitsune2 gossip with B.
#   6. Verify, with C's own DumpOpTimings, that every hinted op is held,
#      integrated, Accepted and *locally validated* by C.
#
# The relay never carries op data and never takes part in gossip; the local
# kitsune2-bootstrap-srv is the same transport infrastructure (bootstrap +
# iroh relay) every Holochain network uses.
#
# MODE=polling (default): stock holochain 0.7.1 everywhere.
# MODE=event: B runs the patched conductor (HOLOCHAIN_B) and the relay is the
#   ops-integrated-signal build (RELAY); see scripts/build-patched-holochain.sh.
#   A second, stock relay is attached to B to show unmodified clients are not
#   affected by the new signal.
# GOSSIP=fast (default) shortens Kitsune2 gossip intervals; GOSSIP=default
#   keeps upstream defaults.
# C_BOOTSTRAP=on (default) restarts C with the bootstrap service; =off
#   restarts C with an unreachable bootstrap URL, so the only fresh peer record
#   it gets is the one handed over by the relay (decentralized discovery).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
MODE=${MODE:-polling}
GOSSIP=${GOSSIP:-fast}
C_BOOTSTRAP=${C_BOOTSTRAP:-on}
PEER_HINT=${PEER_HINT:-on}               # off: control run, B's record is NOT handed to C
# network.request_timeout_s for C only (default 60). Holochain sets the iroh
# connect timeout to floor(3/8 of it): 60 -> 22 s.
C_REQUEST_TIMEOUT_S=${C_REQUEST_TIMEOUT_S:-}
SYNC_TIMEOUT=${SYNC_TIMEOUT:-300}   # seconds to wait for C to catch up
EXPECT_SYNC=${EXPECT_SYNC:-1}       # 0: report a timeout instead of failing (control runs)
NOTES=${NOTES:-5}
HOLOCHAIN=${HOLOCHAIN:-holochain}
HOLOCHAIN_B=${HOLOCHAIN_B:-$HOLOCHAIN}
HC=${HC:-hc}
K2_BOOTSTRAP=${K2_BOOTSTRAP:-kitsune2-bootstrap-srv}
RELAY=${RELAY:-$ROOT/target/debug/hc-push-relay}
LEGACY_RELAY=${LEGACY_RELAY:-$ROOT/target/debug/hc-push-relay}
CLIENT=${CLIENT:-$ROOT/target/debug/hc-push-client}
WORK=${WORK:-$ROOT/.demo/three-node-$MODE-$GOSSIP-boot$C_BOOTSTRAP-hint$PEER_HINT${C_REQUEST_TIMEOUT_S:+-rto$C_REQUEST_TIMEOUT_S}}
BOOT_PORT=${BOOT_PORT:-44500}
A_PORT=44551; B_PORT=44552; C_PORT=44553
RELAY_PORT=${RELAY_PORT:-8792}; RELAY_ADMIN=9792; LEGACY_PORT=8793
HAPP=${HAPP:-$ROOT/fixtures/happ/workdir/notes.happ}
APP_ID=notes
SEED=hc-push-three-node-$MODE
PASS=e2e-test-passphrase
KEY=three-node-registration-key-0001
OPT=(python3 -I "$ROOT/scripts/support/op_timings.py")

case "$MODE" in
  polling) POLL_MS=1000 ;;
  event)   POLL_MS=30000 ;;   # signals carry latency; polling only reconciles
  *) echo "MODE must be polling or event" >&2; exit 2 ;;
esac

log() { printf '\n\033[1;34m== [%s] %s\033[0m\n' "$(date +%T)" "$*"; }
fail() { printf '\n\033[1;31mFAIL: %s\033[0m\n' "$*"; exit 1; }
now_ms() { date +%s%3N; }

[ -f "$HAPP" ] || fail "test hApp not built: run fixtures/happ/build.sh"
if [ "$RELAY" = "$ROOT/target/debug/hc-push-relay" ]; then
  (cd "$ROOT" && cargo build -q -p hc_push_relay -p hc_push_client)
fi
rm -rf "$WORK"; mkdir -p "$WORK"
declare -A PID
cleanup() { for p in "${PID[@]:-}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT

PRELOAD=""
if [ ! -e /proc/net/if_inet6 ]; then
  cc -shared -fPIC -o "$WORK/no-ipv6-shim.so" "$ROOT/scripts/support/no-ipv6-shim.c" -ldl
  PRELOAD="$WORK/no-ipv6-shim.so"
fi

GOSSIP_YAML=""
if [ "$GOSSIP" = fast ]; then
  GOSSIP_YAML="    k2Gossip:
      initiateIntervalMs: 2000
      initiateJitterMs: 500
      minInitiateIntervalMs: 1000"
fi

LD_PRELOAD="$PRELOAD" "$K2_BOOTSTRAP" --listen "127.0.0.1:$BOOT_PORT" >"$WORK/bootstrap.log" 2>&1 &
PID[boot]=$!
sleep 1

write_config() { # name port [bootstrap-url]
  mkdir -p "$WORK/$1"
  local boot=${3:-http://127.0.0.1:$BOOT_PORT}
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
  bootstrap_url: $boot
  relay_url: http://127.0.0.1:$BOOT_PORT
  target_arc_factor: 1
$( [ "$1" = c ] && [ -n "$C_REQUEST_TIMEOUT_S" ] && echo "  request_timeout_s: $C_REQUEST_TIMEOUT_S" )
  advanced:
    irohTransport:
      relayAllowPlainText: true
$GOSSIP_YAML
EOF
}
start_conductor() { # name port binary
  echo "$PASS" | RUST_LOG="${CONDUCTOR_LOG:-warn,kitsune2_gossip=info}" LD_PRELOAD="$PRELOAD" \
    "$3" --piped -c "$WORK/$1.yaml" >>"$WORK/$1.log" 2>&1 &
  PID[$1]=$!
  for _ in $(seq 1 120); do "$HC" client call -p "$2" list-apps >/dev/null 2>&1 && return; sleep 0.5; done
  fail "conductor $1 did not start (see $WORK/$1.log)"
}
stop_conductor() { kill -TERM "${PID[$1]}" 2>/dev/null || true; wait "${PID[$1]}" 2>/dev/null || true; unset "PID[$1]"; }
dump() { "$HC" client call -p "$2" dump-op-timings "$DNA" --limit 1000 >"$WORK/$1"; }
note() { # port text
  echo "$PASS" | "$HC" client zome-call --piped -p "$1" "$APP_ID" "$DNA" notes create_note "\"$2\"" >>"$WORK/zome-calls.log" 2>&1
}
C_CLIENT() { "$CLIENT" --state "$WORK/c-device.json" --relay "b=http://127.0.0.1:$RELAY_PORT" --space "$DNA" --registration-key "$KEY" --label phone-C "$@"; }
OBSERVER() { "$CLIENT" --state "$WORK/observer.json" --relay "b=http://127.0.0.1:$RELAY_PORT" --space "$DNA" --registration-key "$KEY" --label observer "$@"; }

log "Starting conductors A, B ($MODE), C; gossip tuning: $GOSSIP"
"$HOLOCHAIN" --version | sed 's/^/A,C: /'; "$HOLOCHAIN_B" --version | sed 's/^/B:   /'
for n in a b c; do write_config $n $(eval echo \$${n^^}_PORT); done
start_conductor a "$A_PORT" "$HOLOCHAIN"
start_conductor b "$B_PORT" "$HOLOCHAIN_B"
start_conductor c "$C_PORT" "$HOLOCHAIN"
for p in "$A_PORT" "$B_PORT" "$C_PORT"; do
  "$HC" client call -p "$p" install-app --app-id "$APP_ID" "$HAPP" "$SEED" >/dev/null
  "$HC" client call -p "$p" enable-app "$APP_ID" >/dev/null
done
DNA=$("$HC" client call -p "$B_PORT" list-dnas | grep -o 'uhC0k[A-Za-z0-9_-]*' | head -1)
echo "DNA: $DNA"
for n in a b c; do
  echo "$PASS" | "$HC" client zome-call-auth --piped -p $(eval echo \$${n^^}_PORT) "$APP_ID" >>"$WORK/auth.log" 2>&1
done

log "Relay on B (poll interval ${POLL_MS} ms)"
HC_PUSH_SOURCE=holochain HC_PUSH_HC_ADMIN_ADDR=127.0.0.1:$B_PORT HC_PUSH_HC_APP_ID=$APP_ID \
HC_PUSH_HC_POLL_INTERVAL_MS=$POLL_MS HC_PUSH_REGISTRATION=key HC_PUSH_REGISTRATION_KEYS=$KEY \
HC_PUSH_BIND=127.0.0.1:$RELAY_PORT HC_PUSH_ADMIN_BIND=127.0.0.1:$RELAY_ADMIN \
HC_PUSH_DATA_DIR="$WORK/relay" HC_PUSH_LOG_FORMAT=pretty RUST_LOG=info \
"$RELAY" >"$WORK/relay.log" 2>&1 &
PID[relay]=$!
for _ in $(seq 1 120); do curl -sf "http://127.0.0.1:$RELAY_PORT/v1/health" >/dev/null && break; sleep 0.5; done
curl -sf "http://127.0.0.1:$RELAY_PORT/v1/capabilities" >"$WORK/capabilities.json" || fail "relay did not start"
EVENT_DRIVEN=false
grep -q holochain-ops-integrated "$WORK/capabilities.json" && EVENT_DRIVEN=true
echo "event-driven OpsIntegrated source active: $EVENT_DRIVEN"
[ "$MODE" = event ] && [ "$EVENT_DRIVEN" != true ] && fail "MODE=event but the relay did not enable the OpsIntegrated source"

if [ "$MODE" = event ]; then
  # Unmodified client on the same patched conductor: must never receive the
  # new signal (holochain_client 0.9.1 panics on unknown signal variants).
  HC_PUSH_SOURCE=holochain HC_PUSH_HC_ADMIN_ADDR=127.0.0.1:$B_PORT HC_PUSH_HC_APP_ID=$APP_ID \
  HC_PUSH_REGISTRATION=open HC_PUSH_BIND=127.0.0.1:$LEGACY_PORT HC_PUSH_DATA_DIR="$WORK/legacy-relay" \
  HC_PUSH_LOG_FORMAT=pretty RUST_BACKTRACE=1 \
  "$LEGACY_RELAY" >"$WORK/legacy-relay.log" 2>&1 &
  PID[legacy]=$!
fi

log "Waiting until every conductor holds all genesis ops (3 agents x 7 ops)"
for _ in $(seq 1 240); do
  ok=1
  for n in a b c; do
    dump "$n-genesis.json" $(eval echo \$${n^^}_PORT)
    [ "$("${OPT[@]}" hashes "$WORK/$n-genesis.json" | wc -l)" -ge 21 ] || ok=0
  done
  [ $ok = 1 ] && break
  sleep 1
done
[ $ok = 1 ] || fail "network did not converge on genesis ops"

log "Both device clients subscribe and drain the backlog"
C_CLIENT register; C_CLIENT sync >/dev/null
OBSERVER register; OBSERVER sync >/dev/null
OBSERVER --metrics-out "$WORK/observer-metrics.jsonl" listen --relay b --idle-secs 25 --max-secs 300 >"$WORK/observer-events.jsonl" 2>"$WORK/observer.log" &
PID[observer]=$!

log "Conductor C goes offline"
stop_conductor c
C_OFFLINE_AT=$(now_ms)

log "A authors $NOTES notes, B authors 1 (C is offline)"
AUTHOR_START=$(now_ms)
for i in $(seq 1 "$NOTES"); do note "$A_PORT" "note $i from A"; done
note "$B_PORT" "note from B"
AUTHOR_END=$(now_ms)
dump a-after.json "$A_PORT"
"${OPT[@]}" diff "$WORK/a-after.json" "$WORK/a-genesis.json" >"$WORK/new-on-a.txt"
echo "new ops integrated on A: $(wc -l <"$WORK/new-on-a.txt")"

log "Waiting for B to integrate them and the relay to journal hints"
B_SEEN_AT=""
for _ in $(seq 1 240); do
  dump b-after.json "$B_PORT"
  "${OPT[@]}" diff "$WORK/b-after.json" "$WORK/b-genesis.json" >"$WORK/new-on-b.txt"
  if "${OPT[@]}" check "$WORK/b-after.json" "$WORK/new-on-a.txt" >/dev/null 2>&1; then B_SEEN_AT=$(now_ms); break; fi
  sleep 0.5
done
[ -n "$B_SEEN_AT" ] || fail "B did not integrate A's ops"
echo "new ops integrated on B: $(wc -l <"$WORK/new-on-b.txt")"
B_TIMES=$("${OPT[@]}" times "$WORK/b-after.json" "$WORK/new-on-b.txt")

log "A goes offline: only B can serve the missing ops now"
stop_conductor a
wait "${PID[observer]}" 2>/dev/null || true; unset "PID[observer]"
echo "observer (online, SSE): $(grep -c . "$WORK/observer-events.jsonl" || true) events; $(tail -1 "$WORK/observer.log")"

log "C wakes up: HTTP sync for hints while its conductor is still stopped"
C_WAKE_AT=$(now_ms)
C_CLIENT --metrics-out "$WORK/c-metrics.jsonl" sync >"$WORK/c-hints.jsonl"
"${OPT[@]}" hints "$WORK/c-hints.jsonl" >"$WORK/hinted-ops.txt"
HINTED=$(wc -l <"$WORK/hinted-ops.txt")
echo "hinted ops: $HINTED"
[ "$HINTED" -ge "$(wc -l <"$WORK/new-on-b.txt")" ] || fail "fewer hints than ops B integrated"
C_CLIENT --metrics-out "$WORK/c-metrics.jsonl" peers --relay b >"$WORK/b-peers.json"
echo "B peer records from relay: $(python3 -I -c 'import json,sys;print(len(json.load(open(sys.argv[1]))))' "$WORK/b-peers.json")"

log "C's conductor starts and syncs from B by Kitsune2 gossip"
C_LOG_LINES=$(wc -l <"$WORK/c.log")
if [ "$C_BOOTSTRAP" = off ]; then
  write_config c "$C_PORT" "https://127.0.0.1:9/"
  echo "C restarts WITHOUT bootstrap: peers come from its own store and the relay hint only"
fi
C_START_AT=$(now_ms)
start_conductor c "$C_PORT" "$HOLOCHAIN"
C_READY_AT=$(now_ms)
B_URL=$(python3 -I -c 'import json,sys;print(json.loads(json.load(open(sys.argv[1]))[0]["agentInfo"])["url"])' "$WORK/b-peers.json")
# Hand B's peer record to C once C's own agent has rejoined the space
# (before that the record has no space to go into).
C_JOINED_AT=""; C_KNOWS_B_AT=""
for _ in $(seq 1 240); do
  agents=$("$HC" client call -p "$C_PORT" list-agents --dna "$DNA" 2>/dev/null || true)
  if [ -z "$C_JOINED_AT" ] && echo "$agents" | grep -q '"agent_pub_key":"uhCAk'; then
    C_JOINED_AT=$(now_ms)
    if [ "$PEER_HINT" = on ]; then
      "$HC" client call -p "$C_PORT" add-agents "$(cat "$WORK/b-peers.json")" >/dev/null
    else
      C_KNOWS_B_AT=0; break
    fi
  fi
  if [ -n "$C_JOINED_AT" ] && echo "$agents" | grep -qF "$B_URL"; then C_KNOWS_B_AT=$(now_ms); break; fi
  sleep 0.25
done
[ -n "$C_KNOWS_B_AT" ] || fail "C never learned B's peer record"
C_SYNCED_AT=""
for _ in $(seq 1 $((SYNC_TIMEOUT * 2))); do
  dump c-after.json "$C_PORT"
  if "${OPT[@]}" check "$WORK/c-after.json" "$WORK/hinted-ops.txt" >"$WORK/c-check.json" 2>/dev/null; then
    C_SYNCED_AT=$(now_ms); break
  fi
  sleep 0.5
done
cat "$WORK/c-check.json"; echo
if [ -z "$C_SYNCED_AT" ]; then
  if [ "$EXPECT_SYNC" = 1 ]; then fail "C did not validate and integrate all hinted ops within $SYNC_TIMEOUT s"; fi
  printf '\n\033[1;33mCONTROL: C did not catch up within %s s (peer hint %s, bootstrap on restart %s)\033[0m\n' "$SYNC_TIMEOUT" "$PEER_HINT" "$C_BOOTSTRAP"
  echo "{\"mode\":\"$MODE\",\"peer_hint\":\"$PEER_HINT\",\"c_bootstrap_on_restart\":\"$C_BOOTSTRAP\",\"synced\":false,\"timeout_s\":$SYNC_TIMEOUT,\"c_check\":$(cat "$WORK/c-check.json")}" >"$WORK/results.json"
  exit 0
fi
echo "C validated and integrated all $HINTED hinted ops itself (locally_validated=true, Accepted)"
stop_conductor c
tail -n +"$((C_LOG_LINES + 1))" "$WORK/c.log" | sed 's/\x1b\[[0-9;]*m//g' >"$WORK/c-after-restart.log"

if [ "$MODE" = event ]; then
  log "Unmodified client on the patched conductor"
  if grep -qiE "panicked|Failed to deserialize signal" "$WORK/legacy-relay.log"; then
    fail "legacy relay received a signal it cannot decode"
  fi
  echo "stock holochain_client 0.9.1 connection on patched B: no undecodable signals"
fi

curl -sf "http://127.0.0.1:$RELAY_ADMIN/admin/v1/stats" >"$WORK/relay-stats.json"
curl -sf "http://127.0.0.1:$RELAY_ADMIN/admin/v1/metrics" >"$WORK/relay-metrics.prom"

python3 -I - "$WORK" <<PY
import json, sys
w = sys.argv[1]
def jl(p):
    try:
        return [json.loads(l) for l in open(p) if l.strip()]
    except FileNotFoundError:
        return []
stats = json.load(open(f"{w}/relay-stats.json"))["metrics"]
obs = (jl(f"{w}/observer-metrics.jsonl") or [{}])[-1]
c = jl(f"{w}/c-metrics.jsonl")
c_sync = next(m for m in c if m["cmd"] == "sync")
c_peers = next(m for m in c if m["cmd"] == "peers")
b_times = json.loads('''$B_TIMES''')
events = jl(f"{w}/c-hints.jsonl")
import base64, re
def b64(raw):
    return "u" + base64.urlsafe_b64encode(bytes(raw)).decode().rstrip("=")
dump = {b64(t["op_hash"]): t for t in json.load(open(f"{w}/c-after.json"))["timings"]}
hinted = [l.strip() for l in open(f"{w}/hinted-ops.txt") if l.strip()]
start = $C_START_AT
fetched = [dump[h] for h in hinted if dump[h]["when_received"] // 1000 >= start]
rec = sorted(t["when_received"] // 1000 - start for t in fetched)
integ = sorted(t["when_integrated"] // 1000 - start for t in fetched)
phases = {
    "ops_already_held": len(hinted) - len(fetched),
    "ops_fetched_after_restart": len(fetched),
    "first_received_ms": rec[0] if rec else None,
    "last_received_ms": rec[-1] if rec else None,
    "last_integrated_ms": integ[-1] if integ else None,
}
b_url = json.loads(json.load(open(f"{w}/b-peers.json"))[0]["agentInfo"])["url"]
attempts = []
for l in open(f"{w}/c-after-restart.log"):
    m = re.search(r"^(\S+).*(Initiated gossip with|Failed to initiate gossip)\s*:?\s*(\S*)", l)
    if m:
        target = "B" if b_url and b_url in l else "other peer (A, offline)"
        attempts.append({"at": m.group(1), "event": m.group(2), "target": target})
dets = sorted(e["observed_at_ms"] - e["occurred_at_ms"] for e in events
              if e.get("occurred_at_ms") and e["refs"][0]["type"] == "dht_op")
det = {"count": len(dets), "min_ms": dets[0], "p50_ms": dets[len(dets)//2], "max_ms": dets[-1]} if dets else {}
by_source = {}
for e in events:
    by_source[e["source"]] = by_source.get(e["source"], 0) + 1
res = {
    "mode": "$MODE", "gossip": "$GOSSIP", "c_request_timeout_s": "${C_REQUEST_TIMEOUT_S:-60 (default)}", "c_bootstrap_on_restart": "$C_BOOTSTRAP", "peer_hint": "$PEER_HINT", "synced": True, "event_driven_source": $( [ "$EVENT_DRIVEN" = true ] && echo True || echo False ),
    "notes_authored": $NOTES + 1,
    "ops_new_on_a": $(wc -l <"$WORK/new-on-a.txt"),
    "ops_new_on_b": $(wc -l <"$WORK/new-on-b.txt"),
    "ops_hinted_to_c": $HINTED,
    "ops_validated_by_c": json.load(open(f"{w}/c-check.json"))["expected"],
    "hint_sources": by_source,
    "authoring_ms": $AUTHOR_END - $AUTHOR_START,
    "author_to_b_integrated_ms": (b_times["max_ms"] or 0) - $AUTHOR_START,
    "relay_detection_latency": stats["detection_latency"],
    "observer_sse": {"delivered": obs.get("delivered"), "latency": obs.get("latency"), "transfer": obs.get("transfer")},
    "c_hint_sync": {"duration_ms": c_sync["duration_ms"], "delivered": c_sync["delivered"], "transfer": c_sync["transfer"]},
    "c_peer_hint_request": c_peers["transfer"],
    "c_gossip_sync_ms": $C_SYNCED_AT - $C_START_AT,
    "c_conductor_ready_ms": $C_READY_AT - $C_START_AT,
    "c_agent_rejoined_ms": $C_JOINED_AT - $C_START_AT,
    "c_knows_b_ms": ($C_KNOWS_B_AT - $C_START_AT) if $C_KNOWS_B_AT else None,
    "c_phases": phases,
    "c_gossip_attempts": attempts,
    "hinted_ops_detection_ms": det,
    "c_offline_ms": $C_START_AT - $C_OFFLINE_AT,
    "relay_http": {"response_body_bytes": stats["http_response_body_bytes"], "requests": stats["http_requests"]},
}
json.dump(res, open(f"{w}/results.json", "w"), indent=2)
print(json.dumps(res, indent=2))
PY

printf '\n\033[1;32mPASS (%s): ops authored on A -> integrated on B -> relay hints -> C (offline at the time) synced from B by Kitsune2 gossip and validated them itself\033[0m\n' "$MODE"
echo "results: $WORK/results.json"
