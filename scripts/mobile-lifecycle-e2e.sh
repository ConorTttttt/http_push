#!/usr/bin/env bash
# Mobile lifecycle simulation: a lightweight node C that is offline most of
# the time catches up from always-on peers after waking up.
#
#   A  ordinary peer (stock holochain 0.7.1). Authors data, then goes
#      offline: its peer record stays in the bootstrap service, so C sees a
#      STALE peer.
#   B, D  two independent always-on "HoloPorts": each runs its own
#      conductor (patched: OpsIntegrated) and its OWN relay with its own
#      journal. Neither relay knows about the other.
#   C  the "phone": a conductor that is stopped while it is asleep.
#
#   1. C starts and syncs (genesis), its device client subscribes to both relays.
#   2. C shuts down.
#   3. A authors NOTES notes (>= 100 DHT ops by default).
#   4. B and D receive them by gossip, validate and integrate them; each
#      relay journals op hints (with DHT locations). A goes offline.
#   5. C's device client, conductor still stopped, syncs hints from both
#      relays over HTTP (deduplicated) and fetches their signed peer records.
#   6. C's conductor starts (bootstrap enabled: it also learns stale A).
#   7. hc-push-device hands B's and D's records to C (AddAgentInfo; the
#      conductor verifies the signatures), optionally asks C to gossip with
#      them next (REQUEST_ROUND=on, patched C), and C syncs its arc by
#      Kitsune2 gossip.
#   8. Done when every hinted op in C's arc is held, integrated, Accepted
#      and locally validated by C (C's own DumpOpTimings). With C_ARC=zero, C
#      must hold none of them; instead it reads A's notes with zome `get`,
#      which the network serves from B/D (A is offline).
#   9. C shuts down. results.json has timings, CPU time and byte counts.
#
# Variants (env):
#   C_HOLOCHAIN       conductor binary for C (stock or patched)
#   REQUEST_ROUND     on|off  RequestGossipRound (needs patched C and DEVICE)
#   C_ARC             full|zero (target_arc_factor 1 / 0)
#   C_REQUEST_TIMEOUT_S  network.request_timeout_s for C (iroh connect
#                        timeout = 3/8 of it; default 60 -> 22 s)
#   C_BOOTSTRAP       on|off  bootstrap service for C after waking
#   GOSSIP            fast|default
#   RELAY_RESTART     on: relay B is stopped while A authors the middle third
#                     of the notes and restarted before the last third. Its
#                     OpsIntegrated notices for the middle third are lost; the
#                     check is that B's journal still ends up with every op
#                     (reconciled by the DumpOpTimings poller from its
#                     checkpoint) and that events flow again after the
#                     restart (re-subscription).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
PATCHED=${PATCHED:-$ROOT/.demo/patched}
HOLOCHAIN=${HOLOCHAIN:-holochain}
HOLOCHAIN_PATCHED=${HOLOCHAIN_PATCHED:-$PATCHED/holochain-0.7.1/target/debug/holochain}
C_HOLOCHAIN=${C_HOLOCHAIN:-$HOLOCHAIN}
HC=${HC:-hc}
K2_BOOTSTRAP=${K2_BOOTSTRAP:-kitsune2-bootstrap-srv}
RELAY=${RELAY:-$PATCHED/relay-src/target/debug/hc-push-relay}
CLIENT=${CLIENT:-$PATCHED/relay-src/target/debug/hc-push-client}
DEVICE=${DEVICE:-$PATCHED/relay-src/target/debug/hc-push-device}
REQUEST_ROUND=${REQUEST_ROUND:-off}
C_ARC=${C_ARC:-full}
C_REQUEST_TIMEOUT_S=${C_REQUEST_TIMEOUT_S:-}
C_BOOTSTRAP=${C_BOOTSTRAP:-on}
GOSSIP=${GOSSIP:-fast}
RELAY_RESTART=${RELAY_RESTART:-off}
NOTES=${NOTES:-34}
SYNC_TIMEOUT=${SYNC_TIMEOUT:-300}
LABEL=${LABEL:-c-$( [ "$C_HOLOCHAIN" = "$HOLOCHAIN" ] && echo stock || echo patched )-round$REQUEST_ROUND-arc$C_ARC-boot$C_BOOTSTRAP${C_REQUEST_TIMEOUT_S:+-rto$C_REQUEST_TIMEOUT_S}$( [ "$RELAY_RESTART" = on ] && echo -relayrestart )}
WORK=${WORK:-$ROOT/.demo/lifecycle-$LABEL}
BOOT_PORT=${BOOT_PORT:-44600}
A_PORT=44651; B_PORT=44652; C_PORT=44653; D_PORT=44654
RB_PORT=8892; RB_ADMIN=9892; RD_PORT=8893; RD_ADMIN=9893
HAPP=${HAPP:-$ROOT/fixtures/happ/workdir/notes.happ}
APP_ID=notes
SEED=hc-push-lifecycle-$$
PASS=e2e-test-passphrase
KEY_B=lifecycle-relay-b-key-0001
KEY_D=lifecycle-relay-d-key-0002
OPT=(python3 -I "$ROOT/scripts/support/op_timings.py")

log() { printf '\n\033[1;34m== [%s] %s\033[0m\n' "$(date +%T)" "$*"; }
fail() { printf '\n\033[1;31mFAIL: %s\033[0m\n' "$*"; exit 1; }
now_ms() { date +%s%3N; }
cpu_ms() { # pid -> utime+stime in ms
  python3 -I -c 'import os,sys;f=open(f"/proc/{sys.argv[1]}/stat").read().rsplit(")",1)[1].split();print((int(f[11])+int(f[12]))*1000//os.sysconf("SC_CLK_TCK"))' "$1"
}

[ -f "$HAPP" ] || fail "test hApp not built: run fixtures/happ/build.sh"
for b in "$HOLOCHAIN_PATCHED" "$RELAY" "$CLIENT" "$DEVICE"; do
  [ -x "$b" ] || fail "$b missing: run scripts/build-patched-holochain.sh"
done
case "$C_ARC" in full) C_ARC_FACTOR=1; ARC=full ;; zero) C_ARC_FACTOR=0; ARC=empty ;; *) fail "C_ARC must be full or zero" ;; esac
ROUND_FLAG=()
[ "$REQUEST_ROUND" = on ] && ROUND_FLAG=(--request-round)

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

write_config() { # name port arc-factor [bootstrap-url]
  mkdir -p "$WORK/$1"
  local boot=${4:-http://127.0.0.1:$BOOT_PORT}
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
  target_arc_factor: $3
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
  for _ in $(seq 1 240); do "$HC" client call -p "$2" list-apps >/dev/null 2>&1 && return; sleep 0.1; done
  fail "conductor $1 did not start (see $WORK/$1.log)"
}
stop_conductor() { kill -TERM "${PID[$1]}" 2>/dev/null || true; wait "${PID[$1]}" 2>/dev/null || true; unset "PID[$1]"; }
dump() { "$HC" client call -p "$2" dump-op-timings "$DNA" --limit 1000 >"$WORK/$1"; }
zome() { # port fn payload
  echo "$PASS" | "$HC" client zome-call --piped -p "$1" "$APP_ID" "$DNA" notes "$2" "$3"
}
start_relay() { # name conductor-port public admin key
  HC_PUSH_SOURCE=holochain HC_PUSH_HC_ADMIN_ADDR=127.0.0.1:$2 HC_PUSH_HC_APP_ID=$APP_ID \
  HC_PUSH_HC_POLL_INTERVAL_MS=30000 HC_PUSH_REGISTRATION=key HC_PUSH_REGISTRATION_KEYS=$5 \
  HC_PUSH_BIND=127.0.0.1:$3 HC_PUSH_ADMIN_BIND=127.0.0.1:$4 \
  HC_PUSH_DATA_DIR="$WORK/relay-$1" HC_PUSH_LOG_FORMAT=pretty RUST_LOG=info \
  "$RELAY" >"$WORK/relay-$1.log" 2>&1 &
  PID[relay-$1]=$!
  for _ in $(seq 1 120); do curl -sf "http://127.0.0.1:$3/v1/health" >/dev/null && break; sleep 0.25; done
  curl -sf "http://127.0.0.1:$3/v1/capabilities" >"$WORK/capabilities-$1.json" || fail "relay $1 did not start"
  grep -q holochain-ops-integrated "$WORK/capabilities-$1.json" || fail "relay $1: OpsIntegrated source not active"
}
C_CLIENT() {
  "$CLIENT" --state "$WORK/c-device.json" \
    --relay "b=http://127.0.0.1:$RB_PORT" --relay "d=http://127.0.0.1:$RD_PORT" \
    --relay-key "b=$KEY_B" --relay-key "d=$KEY_D" \
    --space "$DNA" --label phone-C --arc "$ARC" "$@"
}

log "Starting A (stock), B and D (patched, each with its own relay), C ($LABEL)"
"$HOLOCHAIN" --version | sed 's/^/A:    /'
"$HOLOCHAIN_PATCHED" --version | sed 's/^/B, D: /'
"$C_HOLOCHAIN" --version | sed 's/^/C:    /'
write_config a "$A_PORT" 1; write_config b "$B_PORT" 1; write_config d "$D_PORT" 1
write_config c "$C_PORT" "$C_ARC_FACTOR"
start_conductor a "$A_PORT" "$HOLOCHAIN"
start_conductor b "$B_PORT" "$HOLOCHAIN_PATCHED"
start_conductor d "$D_PORT" "$HOLOCHAIN_PATCHED"
start_conductor c "$C_PORT" "$C_HOLOCHAIN"
for p in "$A_PORT" "$B_PORT" "$D_PORT" "$C_PORT"; do
  "$HC" client call -p "$p" install-app --app-id "$APP_ID" "$HAPP" "$SEED" >/dev/null
  "$HC" client call -p "$p" enable-app "$APP_ID" >/dev/null
done
DNA=$("$HC" client call -p "$B_PORT" list-dnas | grep -o 'uhC0k[A-Za-z0-9_-]*' | head -1)
echo "DNA: $DNA"
for p in "$A_PORT" "$C_PORT"; do
  echo "$PASS" | "$HC" client zome-call-auth --piped -p "$p" "$APP_ID" >>"$WORK/auth.log" 2>&1
done
start_relay b "$B_PORT" "$RB_PORT" "$RB_ADMIN" "$KEY_B"
start_relay d "$D_PORT" "$RD_PORT" "$RD_ADMIN" "$KEY_D"

log "1. C starts and syncs: network converges on genesis ops; C's client subscribes to both relays"
for _ in $(seq 1 240); do
  ok=1
  for n in a b d; do
    dump "$n-genesis.json" $(eval echo \$${n^^}_PORT)
    [ "$("${OPT[@]}" hashes "$WORK/$n-genesis.json" | wc -l)" -ge 28 ] || ok=0
  done
  dump c-genesis.json "$C_PORT"
  # A zero-arc C holds only its own 7 genesis ops.
  [ "$("${OPT[@]}" hashes "$WORK/c-genesis.json" | wc -l)" -ge $([ "$C_ARC" = full ] && echo 28 || echo 7) ] || ok=0
  [ $ok = 1 ] && break
  sleep 1
done
[ $ok = 1 ] || fail "network did not converge on genesis ops"
C_CLIENT register >/dev/null 2>&1
C_CLIENT sync --policy all >/dev/null 2>&1
echo "C holds $("${OPT[@]}" hashes "$WORK/c-genesis.json" | wc -l) ops; subscribed to relays b and d"

log "2. C shuts down"
stop_conductor c
C_OFFLINE_AT=$(now_ms)

log "3. A authors $NOTES notes (relay B restart during authoring: $RELAY_RESTART)"
AUTHOR_START=$(now_ms)
: >"$WORK/note-hashes.jsonl"
THIRD=$((NOTES / 3))
for i in $(seq 1 "$NOTES"); do
  if [ "$RELAY_RESTART" = on ] && [ "$i" = $((THIRD + 1)) ]; then
    kill -TERM "${PID[relay-b]}"; wait "${PID[relay-b]}" 2>/dev/null || true
    echo "relay B stopped before note $i"
  fi
  if [ "$RELAY_RESTART" = on ] && [ "$i" = $((2 * THIRD + 1)) ]; then
    # Let B integrate the middle third while its relay is down.
    sleep 3
    start_relay b "$B_PORT" "$RB_PORT" "$RB_ADMIN" "$KEY_B"
    # Wait until the restarted relay's OpsIntegrated source is connected and
    # subscribed again (it reports connected in /v1/health).
    for _ in $(seq 1 80); do
      curl -sf "http://127.0.0.1:$RB_PORT/v1/health" | python3 -I -c 'import json,sys;h=json.load(sys.stdin);sys.exit(0 if any(x["id"]=="holochain-ops-integrated" and x["state"]=="connected" for x in h["sources"]) else 1)' && break
      sleep 0.25
    done
    echo "relay B restarted before note $i"
  fi
  zome "$A_PORT" create_note "\"note $i from A\"" >>"$WORK/note-hashes.jsonl"
done
AUTHOR_END=$(now_ms)
dump a-after.json "$A_PORT"
"${OPT[@]}" diff "$WORK/a-after.json" "$WORK/a-genesis.json" >"$WORK/new-on-a.txt"
NEW_A=$(wc -l <"$WORK/new-on-a.txt")
echo "new DHT ops authored by A: $NEW_A"

log "4. B and D receive, validate and integrate them; A goes offline"
for n in b d; do
  seen=""
  for _ in $(seq 1 480); do
    dump "$n-after.json" $(eval echo \$${n^^}_PORT)
    if "${OPT[@]}" check "$WORK/$n-after.json" "$WORK/new-on-a.txt" >/dev/null 2>&1; then seen=$(now_ms); break; fi
    sleep 0.25
  done
  [ -n "$seen" ] || fail "$n did not integrate A's ops"
  eval "${n^^}_SEEN_AT=$seen"
  echo "$n integrated all $NEW_A ops $((seen - AUTHOR_END)) ms after authoring ended"
done
stop_conductor a
A_OFFLINE_AT=$(now_ms)

log "5. C wakes up: hints + signed peer records over HTTP from both relays (conductor still stopped)"
sleep 2 # let both relays' event paths settle
C_WAKE_AT=$(now_ms)
C_CLIENT --metrics-out "$WORK/c-metrics.jsonl" sync --policy all >"$WORK/c-hints.jsonl"
C_HINTS_AT=$(now_ms)
C_CLIENT --metrics-out "$WORK/c-metrics.jsonl" peers --relay b >"$WORK/b-peers.json" 2>/dev/null
C_CLIENT --metrics-out "$WORK/c-metrics.jsonl" peers --relay d >"$WORK/d-peers.json" 2>/dev/null
C_PEERS_AT=$(now_ms)
echo "hints received: $(wc -l <"$WORK/c-hints.jsonl") (arc $ARC)"

RELAY_B_CHECK="null"
if [ "$RELAY_RESTART" = on ]; then
  log "5b. Relay B's journal after its restart: every op A authored, from either path"
  RELAY_B_CHECK=$(python3 -I - "$WORK" <<'PY'
import json, sys
w = sys.argv[1]
ops = [l.strip() for l in open(f"{w}/new-on-a.txt") if l.strip()]
by_op = {}
for l in open(f"{w}/c-hints.jsonl"):
    e = json.loads(l)
    if e["event_id"].startswith(json.load(open(f"{w}/capabilities-b.json"))["journal_id"] + "-"):
        by_op[e["refs"][0]["id"]] = e["source"]
missing = [o for o in ops if o not in by_op]
sources = {}
for o in ops:
    if o in by_op:
        sources[by_op[o]] = sources.get(by_op[o], 0) + 1
print(json.dumps({"ops": len(ops), "journaled_by_b": len(ops) - len(missing), "missing": len(missing), "by_source": sources}))
PY
)
  echo "$RELAY_B_CHECK"
  python3 -I -c 'import json,sys;d=json.loads(sys.argv[1]);s=d["by_source"];sys.exit(0 if d["missing"]==0 and s.get("holochain-op-timings",0)>0 and s.get("holochain-ops-integrated",0)>0 else 1)' "$RELAY_B_CHECK" \
    || fail "relay B did not reconcile the ops it missed while stopped, or events did not resume"
fi

log "6. C's conductor starts (bootstrap $C_BOOTSTRAP)"
C_LOG_LINES=$(wc -l <"$WORK/c.log")
[ "$C_BOOTSTRAP" = off ] && write_config c "$C_PORT" "$C_ARC_FACTOR" "https://127.0.0.1:9/"
C_START_AT=$(now_ms)
start_conductor c "$C_PORT" "$C_HOLOCHAIN"
C_READY_AT=$(now_ms)
C_CPU_READY=$(cpu_ms "${PID[c]}")

log "7-8. C syncs its arc from B/D by Kitsune2 gossip, validates and integrates (request round: $REQUEST_ROUND)"
set +e
"$DEVICE" catch-up --admin "127.0.0.1:$C_PORT" --app-id "$APP_ID" --dna "$DNA" \
  --hints "$WORK/c-hints.jsonl" --peers "$WORK/b-peers.json" --peers "$WORK/d-peers.json" \
  --arc "$ARC" "${ROUND_FLAG[@]}" --t0-ms "$C_START_AT" --timeout-s "$SYNC_TIMEOUT" \
  --out "$WORK/device.json" >/dev/null 2>"$WORK/device.log"
DEVICE_EXIT=$?
set -e
C_SYNCED_AT=$(now_ms)
C_CPU_SYNCED=$(cpu_ms "${PID[c]}")
python3 -I -c 'import json,sys;d=json.load(open(sys.argv[1]));print(json.dumps({k:d[k] for k in ("synced","hints","progress","t_ms","request_round")},indent=1))' "$WORK/device.json" || cat "$WORK/device.log"
[ "$DEVICE_EXIT" = 0 ] || fail "C did not validate and integrate its required ops within $SYNC_TIMEOUT s"

GETS="[]"
if [ "$C_ARC" = zero ]; then
  log "8b. zero-arc C reads A's notes with zome get (served by B/D; A is offline)"
  GETS=$(python3 -I - "$WORK" "$C_PORT" "$APP_ID" "$DNA" "$PASS" "$HC" <<'PY'
import json, subprocess, sys, time
w, port, app, dna, pw, hc = sys.argv[1:]
hashes = [l.strip() for l in open(f"{w}/note-hashes.jsonl") if l.strip()]
out = []
# Like an app would, retry a failed get (up to 3 attempts) and record each.
for h in (hashes[0], hashes[len(hashes) // 2], hashes[-1]):
    attempts = []
    for _ in range(3):
        t = time.time()
        r = subprocess.run([hc, "client", "zome-call", "--piped", "-p", port, app, dna, "notes", "get_note", h],
                           input=pw + "\n", capture_output=True, text=True)
        ms = int((time.time() - t) * 1000)
        found = r.returncode == 0 and r.stdout.strip() not in ("", "null")
        err = None
        if r.returncode:
            m = [l for l in r.stderr.splitlines() if "timed out" in l or "Error" in l]
            err = (m[0] if m else r.stderr.strip().splitlines()[0])[:160]
        attempts.append({"ms": ms, "found": found, "error": err})
        if found:
            break
    out.append({"found": attempts[-1]["found"], "total_ms": sum(a["ms"] for a in attempts), "attempts": attempts})
print(json.dumps(out))
PY
)
  echo "$GETS"
  python3 -I -c 'import json,sys;g=json.loads(sys.argv[1]);sys.exit(0 if g and all(x["found"] for x in g) else 1)' "$GETS" \
    || fail "zero-arc C could not get A's notes from the network"
  dump c-after.json "$C_PORT"
  HELD=$( ("${OPT[@]}" check "$WORK/c-after.json" "$WORK/new-on-a.txt" 2>/dev/null || true) \
    | python3 -I -c 'import json,sys;d=json.load(sys.stdin);print(d["expected"]-d["missing"])')
  echo "A's ops held by zero-arc C after the gets: $HELD of $NEW_A (gets cache records; nothing is required to be held)"
fi
C_CPU_END=$(cpu_ms "${PID[c]}")

log "9. C shuts down"
stop_conductor c
tail -n +"$((C_LOG_LINES + 1))" "$WORK/c.log" | sed 's/\x1b\[[0-9;]*m//g' >"$WORK/c-after-restart.log"
for n in b d; do
  curl -sf "http://127.0.0.1:$(eval echo \$R${n^^}_ADMIN)/admin/v1/stats" >"$WORK/relay-$n-stats.json"
done

python3 -I - "$WORK" <<PY
import json, re, sys
w = sys.argv[1]
jl = lambda p: [json.loads(l) for l in open(p) if l.strip()]
dev = json.load(open(f"{w}/device.json"))
metrics = jl(f"{w}/c-metrics.jsonl")
sync = next(m for m in metrics if m["cmd"] == "sync")
peers = [m for m in metrics if m["cmd"] == "peers"]
hints = jl(f"{w}/c-hints.jsonl")
peer_url = {}
for n in ("b", "d"):
    for r in json.load(open(f"{w}/{n}-peers.json")):
        peer_url[json.loads(r["agentInfo"])["url"]] = n.upper()
attempts = []
for l in open(f"{w}/c-after-restart.log"):
    m = re.search(r"^(\S+)\s.*?(Gossip round requested|Initiated requested gossip with|Failed to initiate requested gossip with|Initiated gossip with|Failed to initiate gossip)", l)
    if m:
        # Stock Kitsune2 does not log the peer of a failed initiation.
        target = next((v for u, v in peer_url.items() if u in l), "not logged" if "Failed to initiate gossip" in l else "other")
        attempts.append({"at": m.group(1), "event": m.group(2), "target": target})
dets = sorted(e["observed_at_ms"] - e["occurred_at_ms"] for e in hints if e.get("occurred_at_ms") and e["refs"][0]["type"] == "dht_op")
stat = lambda v: {"count": len(v), "p50_ms": v[len(v)//2], "max_ms": v[-1]} if v else {}
by_relay = {}
for e in hints:
    by_relay[e["event_id"].split("-")[0]] = by_relay.get(e["event_id"].split("-")[0], 0) + 1
located = sum(1 for e in hints if e["refs"][0].get("loc") is not None)
t = dev["t_ms"]
res = {
    "label": "$LABEL",
    "c_conductor": "$( [ "$C_HOLOCHAIN" = "$HOLOCHAIN" ] && echo "stock 0.7.1" || echo "patched 0.7.1 (+K2 request_round)")",
    "request_round": "$REQUEST_ROUND", "c_arc": "$C_ARC", "c_bootstrap": "$C_BOOTSTRAP",
    "c_request_timeout_s": "${C_REQUEST_TIMEOUT_S:-60 (default)}", "gossip": "$GOSSIP",
    "stale_peer": "A offline since step 4; its record stays in bootstrap",
    "relay_b_restart": json.loads('''$RELAY_B_CHECK'''),
    "notes_authored_by_a": $NOTES,
    "ops_authored_by_a": $NEW_A,
    "b_integrated_after_authoring_ms": $B_SEEN_AT - $AUTHOR_END,
    "d_integrated_after_authoring_ms": $D_SEEN_AT - $AUTHOR_END,
    "notification": {
        "detection_b_or_d_integrated_to_relay_journal": stat(dets),
        "hints_by_relay_journal": by_relay,
        "hints_with_location": located,
        "c_http_sync_ms": $C_HINTS_AT - $C_WAKE_AT,
        "c_http_sync": {"delivered": sync["delivered"], "duplicates": sync["duplicates"], "transfer": sync["transfer"], "relays": sync.get("relays")},
        "c_peer_records_ms": $C_PEERS_AT - $C_HINTS_AT,
        "c_peer_records_transfer": [p["transfer"] for p in peers],
    },
    "data_sync": {
        "conductor_ready_ms": $C_READY_AT - $C_START_AT,
        "own_agent_joined_ms": t["own_agent_joined"],
        "peer_discovery_ms": t["peers_known"],
        "round_requested_ms": t["round_requested"],
        "first_gossip_initiated_ms": None,
        "first_required_op_fetched_ms": t["first_required_op_fetched"],
        "required_already_held_at_start": dev["required_already_held_at_start"],
        "all_required_ops_validated_ms": t["all_required_ops_validated"],
        "required_ops": dev["hints"]["required"],
        "progress": dev["progress"],
        "gossip_attempts": attempts,
        "zero_arc_gets": json.loads('''$GETS'''),
    },
    "cpu_ms": {"c_startup": $C_CPU_READY, "c_until_synced": $C_CPU_SYNCED, "c_total": $C_CPU_END},
    "bytes": {
        "http_in": sync["transfer"]["bytes_received"] + sum(p["transfer"]["bytes_received"] for p in peers),
        "http_out": sync["transfer"]["bytes_sent"] + sum(p["transfer"]["bytes_sent"] for p in peers),
        "kitsune2_open_connections": dev["k2_bytes_open_connections"],
    },
    "device_report": dev,
}
from datetime import datetime
start = $C_START_AT
for a in attempts:
    if a["event"].startswith("Initiated"):
        ts = datetime.fromisoformat(a["at"].replace("Z", "+00:00")).timestamp() * 1000
        res["data_sync"]["first_gossip_initiated_ms"] = int(ts - start)
        break
json.dump(res, open(f"{w}/results.json", "w"), indent=2)
print(json.dumps({k: res[k] for k in ("label", "ops_authored_by_a", "notification", "data_sync", "cpu_ms", "bytes")}, indent=1))
PY
printf '\n\033[1;32mPASS (%s)\033[0m\n' "$LABEL"
echo "results: $WORK/results.json"
