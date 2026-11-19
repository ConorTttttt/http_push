#!/usr/bin/env bash
# Run the mobile lifecycle simulation (scripts/mobile-lifecycle-e2e.sh) for
# each variant REPS times and collect every results.json into
# docs/results-data/lifecycle/, then print a summary table.
#
#   baseline       stock C, ordinary Kitsune2 target selection
#   rto8           stock C, network.request_timeout_s 8 (connect timeout 3 s)
#   patched-noreq  patched C, no request (patch alone must not change behaviour)
#   round          patched C + RequestGossipRound with the relays' peers
#   zero           patched C, zero storage arc, + RequestGossipRound
#   zero-rto8      as zero, with request_timeout_s 8
#
# Env: VARIANTS (space separated), REPS, plus everything the e2e script takes.
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUT=${OUT:-$ROOT/docs/results-data/lifecycle}
REPS=${REPS:-3}
VARIANTS=${VARIANTS:-baseline rto8 patched-noreq round zero zero-rto8}
HOLOCHAIN=${HOLOCHAIN:-holochain}
HOLOCHAIN_PATCHED=${HOLOCHAIN_PATCHED:-$ROOT/.demo/patched/holochain-0.7.1/target/debug/holochain}
mkdir -p "$OUT" "$ROOT/.demo/bench"
for v in $VARIANTS; do
  case "$v" in
    baseline)      env=(C_HOLOCHAIN="$HOLOCHAIN" REQUEST_ROUND=off) ;;
    rto8)          env=(C_HOLOCHAIN="$HOLOCHAIN" REQUEST_ROUND=off C_REQUEST_TIMEOUT_S=8) ;;
    patched-noreq) env=(C_HOLOCHAIN="$HOLOCHAIN_PATCHED" REQUEST_ROUND=off) ;;
    round)         env=(C_HOLOCHAIN="$HOLOCHAIN_PATCHED" REQUEST_ROUND=on) ;;
    zero)          env=(C_HOLOCHAIN="$HOLOCHAIN_PATCHED" REQUEST_ROUND=on C_ARC=zero) ;;
    zero-rto8)     env=(C_HOLOCHAIN="$HOLOCHAIN_PATCHED" REQUEST_ROUND=on C_ARC=zero C_REQUEST_TIMEOUT_S=8) ;;
    *) echo "unknown variant $v" >&2; exit 2 ;;
  esac
  for i in $(seq 1 "$REPS"); do
    work="$ROOT/.demo/bench/$v-$i"
    echo "== $v run $i"
    if env "${env[@]}" WORK="$work" LABEL="$v" bash "$ROOT/scripts/mobile-lifecycle-e2e.sh" >"$work.log" 2>&1; then
      cp "$work/results.json" "$OUT/$v-$i.json"
      python3 -I -c 'import json,sys;r=json.load(open(sys.argv[1]));d=r["data_sync"];print("   validated all", d["required_ops"], "required ops at", d["all_required_ops_validated_ms"], "ms after C start")' "$OUT/$v-$i.json"
    else
      python3 -I -c 'import json,sys;json.dump({"label":sys.argv[1],"failed":True,"log_tail":open(sys.argv[2]).read().splitlines()[-15:]},open(sys.argv[3],"w"),indent=1)' "$v" "$work.log" "$OUT/$v-$i.json"
      echo "   FAILED (see $work.log)"
    fi
  done
done
python3 -I "$ROOT/scripts/support/lifecycle_table.py" "$OUT"
