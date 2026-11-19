#!/usr/bin/env bash
# Build the PROPOSED upstream changes (not part of any Holochain or Kitsune2
# release):
#
#   kitsune2 v0.5.1   + upstream/kitsune2-0.5.1/*.patch
#       Gossip::request_round (start the next round with a verified peer)
#   holochain 0.7.1   + upstream/holochain-0.7.1/*.patch
#       0001 SystemSignal::OpsIntegrated (+ AppRequest::SubscribeOpsIntegrated)
#       0002 AppRequest::RequestGossipRound (needs the Kitsune2 patch)
#
# and, against the patched Holochain crates (via [patch.crates-io], in a copy
# of the workspace so the repository's Cargo.lock keeps pointing at
# crates.io):
#
#   hc-push-relay   --features ops-integrated-signal
#   hc-push-device  --features request-gossip-round
#   hc-push-client
#
# Outputs (printed at the end):
#   $HC_SRC/target/debug/holochain
#   $WORK/relay-src/target/debug/{hc-push-relay,hc-push-device,hc-push-client}
#
# Env: HC_SRC / K2_SRC (existing checkouts to reuse), WORK.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=${WORK:-$ROOT/.demo/patched}
HC_SRC=${HC_SRC:-$WORK/holochain-0.7.1}
K2_SRC=${K2_SRC:-$WORK/kitsune2-0.5.1}
mkdir -p "$WORK"

apply_series() { # checkout dir, patch dir
  local p subject missing=0 sum stamp
  sum=$(cat "$2"/*.patch | sha256sum | cut -d' ' -f1)
  stamp="$1/.hc-push-series-$(basename "$2")"
  if [ "$(cat "$stamp" 2>/dev/null)" = "$sum" ]; then
    echo "series $(basename "$2") already applied in $1"; return
  fi
  # A checkout that already has the series as commits (git am).
  for p in "$2"/*.patch; do
    subject=$(git mailinfo /dev/null /dev/null <"$p" | sed -n 's/^Subject: //p')
    git -C "$1" log -n 100 --format=%s | grep -Fxq "$subject" || missing=1
  done
  if [ "$missing" = 0 ]; then
    echo "series $(basename "$2") already committed in $1"; return
  fi
  for p in "$2"/*.patch; do
    git -C "$1" apply "$p"
    echo "applied: $(basename "$p")"
  done
  echo "$sum" >"$stamp"
}

[ -d "$K2_SRC/.git" ] || git clone --depth 1 --branch v0.5.1 https://github.com/holochain/kitsune2 "$K2_SRC"
[ -d "$HC_SRC/.git" ] || git clone --depth 1 --branch holochain-0.7.1 https://github.com/holochain/holochain "$HC_SRC"
apply_series "$K2_SRC" "$ROOT/upstream/kitsune2-0.5.1"
apply_series "$HC_SRC" "$ROOT/upstream/holochain-0.7.1"

# Holochain against the patched Kitsune2.
{
  echo "[patch.crates-io]"
  for pair in kitsune2_api:api kitsune2_bootstrap_client:bootstrap_client \
    kitsune2_bootstrap_srv:bootstrap_srv kitsune2_core:core kitsune2_dht:dht \
    kitsune2_gossip:gossip kitsune2:kitsune2 kitsune2_test_utils:test_utils \
    kitsune2_transport_iroh:transport_iroh; do
    echo "${pair%%:*} = { path = \"$K2_SRC/crates/${pair##*:}\" }"
  done
} > "$WORK/k2-patch.toml"
# Holochain's dev profile already builds dependencies at opt-level 2; its
# release profile (opt-level=z + fat LTO) is far slower to build. Not
# --locked: the path overrides rewrite Kitsune2's lock entries.
(cd "$HC_SRC" && cargo build --config "$WORK/k2-patch.toml" -p holochain --bin holochain)

rm -rf "$WORK/relay-src/crates"
mkdir -p "$WORK/relay-src/.cargo"
cp "$ROOT/Cargo.toml" "$ROOT/Cargo.lock" "$WORK/relay-src/"
cp -r "$ROOT/crates" "$WORK/relay-src/"
{
  echo "[patch.crates-io]"
  for pair in \
    holochain_client:client fixt:fixt holo_hash:holo_hash \
    holochain_conductor_api:holochain_conductor_api \
    holochain_integrity_types:holochain_integrity_types \
    holochain_keystore:holochain_keystore holochain_nonce:holochain_nonce \
    holochain_secure_primitive:holochain_secure_primitive \
    holochain_state_types:holochain_state_types holochain_trace:holochain_trace \
    holochain_types:holochain_types holochain_util:holochain_util \
    holochain_websocket:holochain_websocket holochain_zome_types:holochain_zome_types \
    mr_bundle:mr_bundle holochain_timestamp:timestamp; do
    echo "${pair%%:*} = { path = \"$HC_SRC/crates/${pair##*:}\" }"
  done
} > "$WORK/relay-src/.cargo/config.toml"
(cd "$WORK/relay-src" && cargo build -p hc_push_relay -p hc_push_client -p hc_push_device \
  --features hc_push_relay/ops-integrated-signal,hc_push_device/request-gossip-round)

echo
echo "patched holochain: $HC_SRC/target/debug/holochain"
echo "event-driven relay: $WORK/relay-src/target/debug/hc-push-relay"
echo "device helper:      $WORK/relay-src/target/debug/hc-push-device"
