# Holochain HTTP Push & Catch-up Relay (prototype)

Lets lightweight (mobile) Holochain clients find out that relevant DHT
changes exist, and catch up after being offline, over short HTTP exchanges
instead of continuous peer-to-peer connections. Any HoloPort, EdgeNode or
Linux host can run a relay next to its conductor. Clients use several
independent relays at once; there is no central server.

> **Status: working prototype.** Relays deliver *hints* (op hashes, signal
> digests), never DHT data. Clients validate by reading through Holochain.
> Direct Kitsune2 gossip integration is **not** implemented; see
> [docs/holochain-integration.md](docs/holochain-integration.md).

| Piece | State |
|---|---|
| HTTP relay: subscriptions, SSE, cursor sync, SQLite journal, retention, auth, rate limits | Implemented, tested |
| Mobile-portable Rust client + CLI: multi-relay, cursors, dedup, failover, gaps | Implemented, tested |
| Real conductor source: `DumpOpTimings` op-level poller + app-signal digests (Holochain 0.7.1) | Implemented. Verified against a real conductor, including ops arriving from a second conductor over Kitsune2 |
| `mock` source | **Mock**: synthetic `mock_change` events, labelled `fidelity: mock` |
| Web Push / APNs / FCM wake-ups, discovery, op-data sync to clients, Kitsune2 module | Designed only ([docs/architecture.md](docs/architecture.md)) |

## Documentation

* [docs/architecture.md](docs/architecture.md): components, trust and security
  model, decentralization, mobile constraints, Web Push, migration path.
* [docs/protocol.md](docs/protocol.md): HTTP API v1, envelopes, cursors, errors,
  client algorithm.
* [docs/holochain-integration.md](docs/holochain-integration.md): checked
  versions, verified APIs, gaps that need Holochain changes.

## Repository layout

```
crates/protocol           wire types (no runtime; mobile-portable)
crates/source-api         ChangeSource trait: the gossip-integration boundary
crates/holochain-adapter  real conductor sources (holochain_client 0.9.1)
crates/relay              relay library + `hc-push-relay` binary + mock source
crates/client             client library + `hc-push-client` CLI
fixtures/happ             minimal test hApp (hdk 0.7.1) for the real-conductor tests
scripts/                  demo.sh, holochain-e2e.sh, holochain-gossip-e2e.sh
deploy/                   compose.yaml (mock pair / EdgeNode sidecar), Caddyfile, .env.example
Dockerfile                distroless OCI image (relay + client CLI)
```

## Build and test

Requires Rust ≥ 1.85 (tested with 1.97) and a C compiler. The Holochain
crates build bundled SQLCipher and vendored OpenSSL, which also needs `perl`.

```sh
cargo build --workspace
cargo test --workspace          # 53 tests: unit + HTTP API + client flows
cargo build -p hc_push_relay --no-default-features   # relay without Holochain deps
```

## Run it

### 1. Mock relay on an ordinary Linux machine

```sh
HC_PUSH_SOURCE=mock \
HC_PUSH_SPACES=uhC0kDemoSpaceDemoSpaceDemoSpaceDemoSpaceDemoSpaceDem \
HC_PUSH_REGISTRATION_KEYS=demo-registration-key-please-change \
HC_PUSH_ADMIN_BIND=127.0.0.1:9787 HC_PUSH_LOG_FORMAT=pretty \
cargo run -p hc_push_relay

curl -s localhost:8787/v1/capabilities
curl -s -XPOST localhost:9787/admin/v1/mock/changes \
  -H 'content-type: application/json' \
  -d '{"space":"uhC0kDemoSpaceDemoSpaceDemoSpaceDemoSpaceDemoSpaceDem","ids":["a","b"]}'
```

### 2. End-to-end demo: two independent relays, one client (Phases 4 + 5)

```sh
scripts/demo.sh
```

The client:

1. registers with relays A and B;
2. receives a live SSE notification;
3. goes offline while changes land on both relays, with overlapping and
   different coverage plus a duplicate;
4. reconnects and recovers the missed events;
5. syncs every relay and deduplicates across them;
6. survives a graceful restart of relay A (persisted journal, tokens and
   cursors);
7. fails over to relay B when A is down.

Output and logs are in `.demo/mock/`.

### 3. Real Holochain conductor (Phase 3)

Needs `holochain` and `hc` 0.7.x (release binaries from
`github.com/holochain/holochain/releases/tag/holochain-0.7.1`) and the
`wasm32-unknown-unknown` target.

```sh
rustup target add wasm32-unknown-unknown
fixtures/happ/build.sh                       # builds notes.happ (needs `hc`)
scripts/holochain-e2e.sh                     # one conductor
cargo install kitsune2_bootstrap_srv --version =0.5.3
scripts/holochain-gossip-e2e.sh              # two conductors over Kitsune2
```

`holochain-e2e.sh` makes a real `create_note` zome call while the client is
offline. The client then catches up over HTTP: 8 `op_integrated` events and
1 `app_signal` digest. Every reported op hash is cross-checked against the
conductor's own `DumpOpTimings` as integrated and `Accepted`.
`holochain-gossip-e2e.sh` authors on conductor A and observes, through a
relay attached only to conductor B, the ops B received over Kitsune2 and
validated.

Use `HOLOCHAIN=/path/holochain HC=/path/hc K2_BOOTSTRAP=/path/kitsune2-bootstrap-srv`
when the binaries are not on `PATH`.

### 4. Client CLI

```sh
hc-push-client --state phone.json --space <DNA> --registration-key <KEY> \
  --relay a=https://relay-a.example --relay b=https://relay-b.example register
hc-push-client ... sync --policy first      # or --policy all
hc-push-client ... listen --relay a --idle-secs 30
hc-push-client ... status
```

Delivered events go to stdout as JSON lines.

### 5. Container / EdgeNode

```sh
docker build -t hc-push-relay:local .
cp deploy/.env.example deploy/.env           # set keys, app id, domain
docker compose -f deploy/compose.yaml --profile mock up --build      # two mock relays
docker compose -f deploy/compose.yaml --profile edgenode up --build  # EdgeNode + relay + TLS
```

In the `edgenode` profile the relay shares the EdgeNode container's network
namespace and reaches the conductor admin port on `127.0.0.1:4444`; nothing
conductor-related is published. Caddy terminates TLS and forwards only
`/v1/*`. EdgeNode itself is unmodified. Install your hApp with EdgeNode's
`happ_tool` and set `HC_PUSH_HC_APP_ID`. The relay waits (with backoff)
until the conductor and app are available.

The image is distroless and non-root (uid 65532), with health check
`hc-push-relay healthcheck`. Mount `/data` for the journal.

## Configuration (environment)

| Variable | Default | Meaning |
|---|---|---|
| `HC_PUSH_BIND` | `127.0.0.1:8787` (image: `0.0.0.0:8787`) | Public HTTP listener (put TLS in front) |
| `HC_PUSH_DATA_DIR` | `./data` (image: `/data`) | Journal location (`journal.sqlite3`) |
| `HC_PUSH_SOURCE` | `mock` | `mock` or `holochain` |
| `HC_PUSH_SPACES` | – | Mock: required. Holochain: optional filter |
| `HC_PUSH_REGISTRATION` | `key` | `key` or `open` |
| `HC_PUSH_REGISTRATION_KEYS` / `_FILE` | – | Registration keys (≥ 16 chars) |
| `HC_PUSH_RELAY_ID` | random, persisted | Public relay id |
| `HC_PUSH_RETENTION_MAX_EVENTS` / `_MAX_AGE_SECS` | 100000 / 604800 | Journal retention |
| `HC_PUSH_MAX_SUBSCRIPTIONS` | 10000 | Bound on subscriptions |
| `HC_PUSH_SUBSCRIPTION_IDLE_TTL_SECS` | 2592000 | Idle subscriptions expire |
| `HC_PUSH_RATE_LIMIT_PER_MINUTE` | 120 | Per-subscription rate |
| `HC_PUSH_REGISTRATION_RATE_PER_MINUTE` | 10 | Per-address registration rate |
| `HC_PUSH_MAX_SSE_CONNECTIONS` | 1024 | Global SSE cap |
| `HC_PUSH_TRUST_FORWARDED_FOR` | false | Use `X-Forwarded-For` (behind Caddy only) |
| `HC_PUSH_ADVERTISE_SPACES` | true | List spaces in `/v1/capabilities` |
| `HC_PUSH_ADMIN_BIND` | off | Loopback operator listener (mock injection, stats) |
| `HC_PUSH_MOCK_INTERVAL_MS` | 0 | Mock: periodic synthetic changes |
| `HC_PUSH_HC_ADMIN_ADDR` | `127.0.0.1:4444` | Conductor admin ws (**never public**) |
| `HC_PUSH_HC_APP_ID` | – | App to observe |
| `HC_PUSH_HC_APP_PORT` | auto | Existing app interface; else one is attached on loopback |
| `HC_PUSH_HC_POLL_INTERVAL_MS` | 2000 | `DumpOpTimings` poll interval |
| `HC_PUSH_HC_OP_TIMINGS` / `HC_PUSH_HC_SIGNALS` | true / true | Enable each Holochain source |
| `HC_PUSH_LOG_FORMAT` | `json` | `json` or `pretty`; filter with `RUST_LOG` |

## What is mocked or simulated

* `HC_PUSH_SOURCE=mock` (`crates/relay/src/mock.rs`) emits synthetic
  `mock_change` events with `mock` refs. It is advertised with
  `fidelity: "mock"` and `source: "mock"` in every envelope. `scripts/demo.sh`,
  the compose `mock` profile and the Rust integration tests use it.
* The mock admin injection endpoint exists only on the loopback operator
  listener.
* The real-conductor tests run without Internet peers: a single conductor,
  or two conductors on a local bootstrap/relay server.
* `scripts/support/no-ipv6-shim.c` is a test-environment workaround for
  Holochain on kernels without IPv6 (gap G6). It is not part of the relay.

## Known limitations

* Hints only. No DHT op data reaches clients. Op hashes cannot be fetched
  directly with zome `get`, so apps re-run their queries (gap G2).
* The Holochain source polls (default 2 s). It is not push-driven (gap G1).
* Coverage per relay is the relay conductor's storage arc for the
  configured app. Relays with different arcs report different subsets.
* App-signal events are local to the relay's conductor and are deduplicated
  by payload digest, so identical payloads within the retention window
  collapse into one event.
* No Web Push / APNs / FCM yet, so a suspended app is not woken. It catches
  up on its next foreground or OS-scheduled background run.
* Static relay configuration; no discovery. Registration keys are shared
  secrets. There is no membrane- or agent-based authorization yet.
* Rate-limiter and SSE state is per process (fine for one relay per host).
* SQLite journal: single writer per relay instance; no replication.

## Future work

* Push-style integrated-op signal and richer `OpTimingDump` in Holochain (G1,
  G2); then storage-arc-aware coverage in `/v1/capabilities` from
  `DumpNetworkMetrics`.
* Kitsune2 HTTP transport / relay-as-K2-peer for real op sync into an
  on-device conductor; Merkle/region reconciliation using K2 slice hashes.
* Optional RFC 8030 Web Push adapter (RFC 8291/8292), APNs/FCM wake-up
  adapters. These are always content-free "sync now" nudges.
* Native Android/iOS bindings (e.g. UniFFI over `hc_push_client`).
* Decentralized relay discovery: signed capability entries in a DNA,
  agent-info URLs, `.well-known`.
* Agent-signed or membrane-aware registration (proving DNA membership
  without giving the relay agent keys).
* Bandwidth and battery benchmarks against continuous Kitsune2
  participation.
* Relay incentives and operator economics (e.g. HoloFuel / Unyt-style
  metering of sync requests).

## License

Apache-2.0
