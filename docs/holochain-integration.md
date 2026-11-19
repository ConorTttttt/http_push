# Holochain / Kitsune2 integration findings

This records what was verified in source code and by running a real conductor.
It covers what the relay uses, what it deliberately does not use, and what
would need changes in Holochain core. Every API named here was checked
against the source at the revisions below; nothing is inferred from memory.

## Checked versions (2026-11-19)

| Project | Revision | Notes |
|---|---|---|
| holochain/holochain `main` | `90198dd` (2026-11-18) | 0.8.0-dev.15 / conductor API 0.8.0-dev.13 / kitsune2 0.6.0-dev.3 |
| holochain/holochain `main-0.7` | `c5f5028` (2026-10-14) | **Stable line used**: holochain 0.7.1, `holochain_client` 0.9.1, `holochain_conductor_api` 0.7.1, kitsune2 0.5.x |
| holochain/kitsune2 `main` | `1d26e00` (2026-11-09) | 0.6.0-dev.3; stable 0.5.3 on crates.io |
| holochain/hc-http-gw `main` | `8556a11` (2026-11-10) | 0.4.0, targets Holochain 0.7 (`holochain_client` 0.9) |
| Holo-Host/edgenode `main` | `ed76b36` (2026-10-06) | Holochain 0.7.0 under s6-overlay |
| Holo-Host/h2hc-linker `main` | `cab242d` (2026-08-28) | alpha gateway for zero-arc browser clients |

Binaries used for the live tests: `holochain 0.7.1` and `hc 0.7.1` from the
GitHub release `holochain-0.7.1`, plus `kitsune2-bootstrap-srv 0.5.3` built
from crates.io. The test hApp uses `hdk 0.7.1` and `hdi 0.8.1`.

Not consulted directly: developer.holochain.org and the RFC 8030 text were
not reachable through this environment's network proxy. The conductor
behaviour below comes from source code and live runs. The Web Push notes in
`architecture.md` are from the RFCs as published (RFC 8030/8291/8292) and
should be re-checked against the RFC text before implementing that adapter.

## 1. How Kitsune2 discovers and exchanges op data

Source: `kitsune2/crates/api/src/{op_store,gossip,space,fetch,publish,report}.rs`,
`holochain/crates/holochain_p2p/src/op_store.rs`.

* The **host** (Holochain) implements `OpStore`. Kitsune2 holds only op ids
  and asks the host for data:
  * `process_incoming_ops(Vec<IncomingOp>)`: ops received from peers, by
    publish *or* gossip (K2 does not distinguish them).
  * `retrieve_op_hashes_in_time_slice(arc, start, end)` and
    `retrieve_op_ids_bounded(arc, start, limit_bytes)`: the latter pages by
    *stored-at* time and returns the next start timestamp. That is a natural
    cursor primitive, and the closest K2 analogue of our journal cursor.
  * `store_slice_hash` / `retrieve_slice_hashes`: the time-sliced hash
    summaries gossip uses to find differing regions.
* `Space::inform_ops_stored(Vec<StoredOp>)` tells gossip that the host has
  persisted ops. Until then they are not in the gossip DHT model.
* `Gossip::get_state_summary` exposes `DhtSegmentState { disc_top_hash,
  ring_top_hashes }` and per-peer `PeerMeta { storage_arc, new_ops_bookmark,
  … }`.
* `Fetch::request_ops(op_ids, source)` pulls op data from a peer.
* `Report::fetched_op(space, source, op_id, size_bytes)` is a pluggable hook
  called whenever op data is fetched from a peer.

In Holochain 0.7 the flow for an op arriving from a peer is:

```
K2 fetch → HolochainOpStore::process_incoming_ops → HcP2pHandler::handle_publish
  → incoming DHT ops workflow → sys + app validation
  → integrate_dht_ops_workflow → network.new_integrated_data(stored_ops)
  → kitsune2 Space::inform_ops_stored
```

(`holochain/src/core/workflow/integrate_dht_ops_workflow.rs`, `new_integrated_data`
at line ~101 on `main`.)

**None of this is exposed outside the conductor process.** Kitsune2 module
factories (`ReportFactory`, `GossipFactory`, `OpStoreFactory`, …) are chosen
when Holochain builds its Kitsune2 instance. A third-party process cannot
attach to them.

## 2. Public APIs that expose op-level change information

Source: `holochain_conductor_api/src/app_interface.rs`, `state_dump.rs`.

| API | Interface | Usable by the relay? |
|---|---|---|
| `AppRequest::DumpOpTimings { dna_hash, cursor, limit }` (added 0.7.0-rc.5) | **app** (authenticated, scoped to the app's DNAs) and admin | **Yes, used.** Returns `OpTimingDump { op_hash, when_received, when_integrated, abandoned_at, validation_status, locally_validated }`, ordered by `(when_received, op_hash)` with an exclusive `OpTimingsCursor`. Covers every op this conductor holds for the DNA (its whole arc), including ops authored by other agents that arrived by publish or gossip. |
| `AppRequest::DumpNetworkMetrics { dna_hash, include_dht_summary }` | app | Not yet. Gives the gossip state summary (segment/ring top hashes, peer storage arcs). Useful for advertising coverage and arc in `/v1/capabilities` later. |
| `AppRequest::AgentInfo` / `PeerMetaInfo` | app | Not yet. Peer list and arcs. |
| `AdminRequest::DumpFullState` | admin only | No. Returns full ops; needs admin access; heavy. |
| `AppWebsocket::on_signal` → `Signal::App`, `Signal::AppDirect`, `Signal::System` | app | **Yes, used for app signals.** |

### Signals do not reveal gossip

`Signal::App` is produced only by `emit_signal` inside **coordinator** zome
execution: zome calls, `post_commit` (local commits only), and
`recv_remote_signal`. Validation callbacks run in integrity zomes (HDI),
which have no `emit_signal`. `SystemSignal` covers countersigning and chain
restore only. **An op that arrives by gossip and integrates produces no
signal.** (This follows from the code paths above; the gossip e2e test does
not exercise signals.) The relay therefore uses `DumpOpTimings` for
op-level observation and treats app signals as an optional, local-only
supplement.

## 3. Authenticated conductor access

* App websockets require an `AppAuthenticationToken` issued over the
  **admin** websocket (`IssueAppAuthenticationToken`; default single-use,
  30 s expiry). `hc client` has no command to issue one, so a sidecar needs
  short-lived admin access. hc-http-gw and h2hc-linker work the same way.
* What the relay does (`crates/holochain-adapter`):
  1. Connect to the admin websocket on **loopback** (`HC_PUSH_HC_ADMIN_ADDR`).
  2. Reuse an app interface whose `allowed_origins` permits
     `hc-push-relay`, or `AttachAppInterface` on port 0, bound to the
     same loopback address, scoped to the configured app id.
  3. Issue a single-use token and **drop the admin connection**.
  4. Use only the app websocket from then on, with an empty
     `ClientAgentSigner`. The relay never signs zome calls and never sees
     agent keys.
* Once connected, the relay can only see the DNAs of the one app it is
  configured for.

## 4. Extend hc-http-gw, or stay separate?

hc-http-gw (`spec.md`, `src/`) is a **stateless, GET-only zome-call
gateway**. It caches connections, transcodes JSON↔msgpack, and has no
streaming, durable state or subscriptions. The relay needs a persistent
journal, per-client subscriptions, SSE, retention and cursor semantics, and
it observes the conductor rather than calling zomes. Merging them would push
statefulness and long-lived connections into a component designed to be
stateless. **Decision: keep them separate**, sharing conventions (env-var
configuration, loopback admin access, origin-scoped app interfaces). They
complement each other. After a notification, a client without its own
conductor *could* read data through a gateway, but then it trusts that
gateway's conductor for validity (see section 6).

h2hc-linker serves zero-arc browser clients over a **persistent WebSocket
session**. That is the opposite trade-off from this project, which avoids
continuous connections. The two can coexist on one EdgeNode.

## 5. Hosting on EdgeNode

Source: `edgenode/docker/{Dockerfile,docker-compose.yml,conductor-config-0.7.0.template.yaml}`,
`docs/superpowers/plans/2026-09-02-holochain-0.7-upgrade.md`.

* EdgeNode runs a Holochain 0.7.0 conductor under s6-overlay in one OCI
  container. The admin websocket listens on port 4444 with
  `danger_bind_addr: null` (loopback). Sidecar tools such as log-harvester
  attach their own app interface (`add-app-ws <port> --allowed-origins
  <origin>`).
* **No EdgeNode changes are needed.** The relay runs as a separate container
  with `network_mode: service:edgenode`. It shares the EdgeNode network
  namespace, so `127.0.0.1:4444` reaches the conductor while nothing
  conductor-related is published. A Caddy container terminates TLS and
  forwards `/v1/*`. See `deploy/compose.yaml`.
* Alternative, not implemented: add the relay as another s6 longrun service
  inside the EdgeNode image, mirroring how log-harvester is added.

## 6. How lightweight clients validate synchronized data

The relay forwards **hints** (op hashes, signal digests) and never op data.
The client's options for turning a hint into trusted data are:

1. **Own conductor on the device** (zero- or small-arc). The app wakes,
   runs its normal zome reads, and its conductor fetches and validates
   through Holochain's own pipeline. This is the only option that keeps
   Holochain's full validation guarantees, and the target design.
2. **No conductor on the device.** The app can fetch records from a
   gateway (hc-http-gw / h2hc-linker) and verify hashes and author
   signatures itself, which proves *authenticity*. It cannot run the DNA's
   validation rules without the wasm, so *validity* comes from the
   gateway's conductor. Treat this as a trusted-gateway mode.

What a relay can and cannot do:

* It **cannot forge actions**: it never supplies signed data, and anything
  fetched later is hash- and signature-checked by Holochain.
* It **can** lie about, delay, reorder or omit hints. Mitigations: several
  independent relays, periodic authoritative reconciliation through the
  conductor (especially after a `GapNotice`), and never treating "no
  notification" as "no change".

## 7. Gaps that require Holochain or Kitsune2 changes

| # | Gap | Effect today | Smallest core change |
|---|---|---|---|
| G1 | No push notification for integrated ops on the app API | Relay polls `DumpOpTimings` (default 2 s), and re-scans from the oldest op still pending validation | **U1, implemented as a patch** (`upstream-changes.md`): opt-in per connection `SystemSignal::OpsIntegrated`, emitted after commit from both integration paths |
| G2 | `OpTimingDump` has no basis hash, op type or action hash | Clients get "something changed in DNA X" plus an op hash they cannot fetch with zome `get`, so they re-run app queries | Add `basis_hash` and `op_type` (and `action_hash` for chain ops) to `OpTimingDump` |
| G3 | `DumpOpTimings` cursor orders by local `when_received` | If the host clock steps backwards, ops can be stamped before the saved cursor and missed until a full rescan | Cursor over a monotonic local sequence (e.g. integration rowid) |
| G4 | No app-API way to fetch raw op data by op hash for forwarding into another conductor's `process_incoming_ops` | Real op-level sync to phones must go through Kitsune2 itself | Either an app-API `FetchOps(op_ids)` returning `DhtOp`s plus an "ingest ops" API on the receiving conductor that runs normal validation, or (preferred) the Kitsune2 transport route in `architecture.md` |
| G5 | Kitsune2 module factories cannot be supplied from outside the conductor | A `ReportFactory` that would observe every fetched op cannot be added without patching Holochain | Conductor config hook for extra K2 report/observer modules |
| G6 | Holochain's websocket `dual_bind` aborts when the kernel has no IPv6 (`EAFNOSUPPORT`); it only falls back on `EADDRNOTAVAIL` | The conductor cannot start on IPv6-less hosts. The e2e scripts work around this with a test-only `LD_PRELOAD` errno shim (`scripts/support/no-ipv6-shim.c`) | Treat `ErrorKind::Unsupported`/`EAFNOSUPPORT` like `AddrNotAvailable` in `holochain_websocket::dual_bind` (unchanged on `main` as of `90198dd`) |
| G7 | No API to start a gossip round with a chosen peer | A restarted lightweight node cannot say "sync with this HoloPort now" | U2 in `upstream-changes.md` |
| G8 | Gossip target selection keeps choosing an offline peer whose record has not expired | Catch-up after restart took 48–84 s with bootstrap (each attempt waits out a transport connect timeout) vs 6.4 s with only the relay-supplied peer | U3 in `upstream-changes.md` |
| G9 | `holochain_client` 0.9.1 panics on any signal variant it cannot decode | New system signals must be opt-in per connection | U4 in `upstream-changes.md` |

The proposed fixes, and the one implemented patch, are collected in
[`upstream-changes.md`](upstream-changes.md), separate from the relay docs.

Other observations from the live runs:

* **Two ways an op becomes integrated.** Ops from other peers go through
  validation and `integrate_dht_ops_workflow`. An agent's *own* ops are
  written by `SourceChain::flush` directly as integrated (`when_received =
  when_integrated = action timestamp`, `locally_validated = true`), without
  that workflow. A notification hook on the workflow alone misses them. The
  three-node event test found this, and the patch hooks the `DhtStore`
  instead.
* **Catching up an offline conductor needs nothing new in Kitsune2.** The
  existing gossip (DHT model diff, new-ops bookmarks) and fetch deliver the
  missing ops, and `process_incoming_ops` runs the device's own validation.
  What a lightweight node lacks is (a) knowing *whether and what* it is
  missing, so it can start networking only when needed and stop when done,
  and (b) a reachable peer that holds the data. The relay supplies both:
  hints (op hashes) and the HoloPort's signed agent info (`GET /v1/peers` →
  `AddAgentInfo`).
* **Transient SQLite busy errors.** The restarted conductor C logged
  `SQLITE_BUSY` (5) / `SQLITE_BUSY_SNAPSHOT` (517) from
  `integrate_dht_ops_consumer`. The workflow retried and every op
  integrated.

* Kitsune2 0.5 refuses `http://` relay URLs unless
  `network.advanced.irohTransport.relayAllowPlainText: true`.
* `hc client call dump-op-timings` prints hashes as raw byte arrays. The
  HoloHash base64 form is `"u" + base64url(39 bytes)`.
* With one conductor and no peers, locally authored ops still appear in
  `DumpOpTimings` as integrated and `Accepted`, so a single HoloPort
  relay reports changes made through its own conductor.

## 8. What is demonstrated, and what is not

| Claim | Status | Evidence |
|---|---|---|
| Real zome call → integrated DHT ops → relay → HTTP client | **Demonstrated** | `scripts/holochain-e2e.sh`. All reported op hashes are cross-checked against the conductor's own `DumpOpTimings` as integrated and `Accepted`. |
| Op authored on conductor A, delivered by Kitsune2 to B, validated on B → relay on B → client | **Demonstrated** | `scripts/holochain-gossip-e2e.sh` (local bootstrap + iroh relay, two conductors) |
| Op-level hints come from Holochain's validation pipeline | Yes, for the relay's own conductor | `when_integrated` + `validation_status` from `DumpOpTimings` |
| Lightweight conductor C, offline while A authored, catches up from B by native Kitsune2 gossip after an HTTP hint, and **validates the ops itself** | **Demonstrated** | `scripts/holochain-three-node-e2e.sh`. Every hinted op is integrated, `Accepted` and `locally_validated: true` in C's own `DumpOpTimings`; A is offline, so the data can only come from B. Control run without the relay's peer hint: no catch-up in 150 s. |
| Event-driven notification (`OpsIntegrated`) | **Demonstrated with a patched conductor** (not released Holochain) | `MODE=event scripts/holochain-three-node-e2e.sh`; see `results.md` |
| Missed notifications / relay restarts do not prevent eventual delivery | **Tested** | `crates/relay/tests/holochain_reconcile.rs` (scripted conductor) and the three-node event run (polling reconciles) |
| Op **data** carried by the relay | **No, by design** | The relay carries hints; data moves by Kitsune2 between conductors |
| Relay as a Kitsune2 module or peer | **Not implemented** | G5; not needed for the demonstrated path |
