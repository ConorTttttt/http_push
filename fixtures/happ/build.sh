#!/usr/bin/env bash
# Build the test hApp. Requires: rustup target wasm32-unknown-unknown, `hc`.
set -euo pipefail
cd "$(dirname "$0")"
HC=${HC:-hc}
RUSTFLAGS='-C link-arg=--import-undefined --cfg getrandom_backend="custom"' \
  cargo build --release --target wasm32-unknown-unknown
"$HC" dna pack workdir -o workdir/notes.dna
"$HC" app pack workdir -o workdir/notes.happ
ls -l workdir/notes.happ
