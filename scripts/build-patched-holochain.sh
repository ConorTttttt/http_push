#!/usr/bin/env bash
# Build the PROPOSED upstream change (not part of any Holochain release):
#
#   1. holochain 0.7.1 + upstream/holochain-0.7.1-ops-integrated.patch
#      (SystemSignal::OpsIntegrated, AppRequest::SubscribeOpsIntegrated)
#   2. hc-push-relay / hc-push-client with `--features ops-integrated-signal`,
#      compiled against the patched Holochain crates via [patch.crates-io].
#
# The relay build happens in a copy of the workspace so the repository's
# Cargo.lock keeps pointing at crates.io.
#
# Outputs (printed at the end):
#   $HC_SRC/target/debug/holochain
#   $WORK/relay-src/target/debug/hc-push-relay
#
# Env: HC_SRC (existing holochain-0.7.1 checkout to reuse), WORK.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=${WORK:-$ROOT/.demo/patched}
HC_SRC=${HC_SRC:-$WORK/holochain-0.7.1}
PATCH="$ROOT/upstream/holochain-0.7.1-ops-integrated.patch"
mkdir -p "$WORK"

if [ ! -d "$HC_SRC/.git" ]; then
  git clone --depth 1 --branch holochain-0.7.1 https://github.com/holochain/holochain "$HC_SRC"
fi
if git -C "$HC_SRC" apply --reverse --check "$PATCH" 2>/dev/null; then
  echo "patch already applied in $HC_SRC"
else
  git -C "$HC_SRC" apply "$PATCH"
  echo "applied $(basename "$PATCH")"
fi

# Holochain's dev profile already builds dependencies at opt-level 2; its
# release profile (opt-level=z + fat LTO) is far slower to build.
(cd "$HC_SRC" && cargo build --locked -p holochain --bin holochain)

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
(cd "$WORK/relay-src" && cargo build -p hc_push_relay -p hc_push_client --features hc_push_relay/ops-integrated-signal)

echo
echo "patched holochain: $HC_SRC/target/debug/holochain"
echo "event-driven relay: $WORK/relay-src/target/debug/hc-push-relay"
