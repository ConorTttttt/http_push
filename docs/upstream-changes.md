# Proposed Holochain / Kitsune2 upstream changes

This document is about **Holochain and Kitsune2**, not about the relay. The
relay works without any of these changes: it polls the supported
`DumpOpTimings` app API, and that durable path stays in place even where a
change below is adopted.

Nothing here has been submitted upstream. U1 exists as a patch in this
repository and was built and tested locally against the `holochain-0.7.1`
tag (`fdaa678`). Everything else is a written proposal.

| # | Change | Component | What it unlocks | State |
|---|---|---|---|---|
| U1 | Opt-in `SystemSignal::OpsIntegrated` | holochain (types, conductor_api, state, conductor, client) | Event-driven relay notifications instead of polling | **Patch + tests**: `upstream/holochain-0.7.1-ops-integrated.patch` |
| U2 | Ask the conductor to gossip with a given peer now | kitsune2 + holochain app API | Fast catch-up of a lightweight node from a known always-on peer | Proposal (blocker measured) |
| U3 | Avoid repeatedly picking unreachable peers for gossip | kitsune2 gossip | Catch-up not delayed by stale peer records | Proposal (measured) |
| U4 | `holochain_client`: don't panic on unknown signal variants | holochain_client (Rust) | Forward-compatible signals | Proposal |
| U5 | `OpTimingDump`: basis hash / op type / action hash | holochain conductor API | Actionable, arc-filterable hints from the polling path | Proposal |
| U6 | `dual_bind`: fall back to IPv4 on `EAFNOSUPPORT` | holochain_websocket | Conductor starts on kernels without IPv6 | Proposal (one-line) |
| U7 | Monotonic `DumpOpTimings` cursor | holochain conductor API | Cursor robust to host clock steps | Proposal |

---

## U1: opt-in `SystemSignal::OpsIntegrated` (implemented as a patch)

### What it adds

```rust
// holochain_types::signal
SystemSignal::OpsIntegrated {
    dna_hash: DnaHash,
    when_integrated: Timestamp,
    ops: Vec<IntegratedOp>,          // at most 500 per signal; larger batches are split
}
pub struct IntegratedOp { op_hash: DhtOpHash, basis_hash: AnyLinkableHash, validity: OpValidity }

// holochain_conductor_api
AppRequest::SubscribeOpsIntegrated   // -> AppResponse::Ok, per websocket connection

// holochain_client
AppWebsocket::subscribe_ops_integrated()
```

### Design decisions

**Opt-in per app websocket connection, not per conductor or app.**
`holochain_client` 0.9.1 decodes every signal with
`Signal::try_from_vec(..).expect("Failed to deserialize signal")`
(`app_websocket_inner.rs`). Any unmodified Rust client that registered
`on_signal` would therefore panic on the first unknown variant. The patch
filters the variant in each connection's signal forwarder unless that
connection sent `SubscribeOpsIntegrated`. The three-node event test runs a
stock-crates relay on the same patched conductor and checks it never sees
the signal.

**One choke point covering both ways an op becomes integrated.**
A first version only hooked `integrate_dht_ops_workflow`. The three-node
test then showed ops with no signal. In 0.7.1 an agent's own ops are written
to the DHT store *already integrated* by `SourceChain::flush`
(`holochain_state/src/source_chain.rs`: `when_received = when_integrated =
action timestamp`, `locally_validated = true`). They never pass through the
integration workflow. The final patch puts an `IntegratedOpsObserver` slot
on `DhtStore`. The slot is shared by all clones, and every writable store of
a DNA is a clone of the one created in `space.rs`. It is invoked **after
commit** in both:

* `DhtStore::integrate_ready_ops` (ops from other peers, after validation);
* `SourceChain::flush` (self-authored ops).

This also left the integration workflow's signature untouched.

**Integration never waits for notification delivery.**
* `is_active()` is a single atomic load (number of opted-in connections).
  When it is zero, no vector is built and nothing is spawned.
* When it is active, the store builds `Vec<IntegratedOp>` from data it
  already has and calls the observer *after* the transaction committed.
* The conductor resolves which enabled apps have a cell of the DNA on a
  `tokio::spawn`ed task, then uses the existing per-app `broadcast::Sender`.
  `send` never blocks; lagging connections drop signals and are counted by
  the existing `dropped_signal_metric`.
* The observer holds a `Weak<Conductor>`, avoiding a reference cycle
  between conductor, spaces and store.

**Best effort, by design.** Signals are not persisted or replayed after a
reconnect, and a connection that lags loses them. They are a latency path.
Durable reconciliation remains `DumpOpTimings` (or a future cursor API).
The relay runs both, and the regression test
`crates/relay/tests/holochain_reconcile.rs` covers missed signals across a
relay restart.

**Privacy and scope.** A connection only receives signals for DNAs of the
app it authenticated for, and only op hashes, basis hashes and validity (no
op content).

### Files (diff stat against `holochain-0.7.1`)

```
crates/client/src/app_websocket.rs                         subscribe_ops_integrated()
crates/holochain_types/src/signal.rs                       variant + IntegratedOp + chunk size
crates/holochain_conductor_api/src/app_interface.rs        AppRequest::SubscribeOpsIntegrated
crates/holochain_state/src/dht_store.rs                    observer trait + shared slot + call after commit
crates/holochain_state/src/source_chain.rs                 call after flush commit (self-authored ops)
crates/holochain_state/src/dht_store/tests.rs              observer test (clones, active gating, payload)
crates/holochain/src/conductor/conductor.rs                OpsIntegratedObserver + notify_ops_integrated
crates/holochain/src/conductor/conductor/app_broadcast.rs  opt-in subscriber count + test
crates/holochain/src/conductor/interface/websocket.rs      per-connection opt-in and filter
crates/holochain/src/conductor/api/api_external/app_interface.rs  no-op arm for non-websocket callers
crates/holochain/src/core/queue_consumer.rs                install the observer per space
```

### How it was verified

* Upstream unit tests on the patched tree, including the new observer and
  subscriber-count tests and the existing integration-workflow and
  app-broadcast tests (see `docs/results.md`).
* `scripts/build-patched-holochain.sh` builds the patched conductor and a
  relay compiled against the patched crates (`--features
  ops-integrated-signal`, via `[patch.crates-io]` in a scratch copy of the
  workspace).
* `MODE=event scripts/holochain-three-node-e2e.sh` runs the patched
  conductor as B (A and C stay stock 0.7.1; they interoperate), checks every
  hint was delivered by the event path, and checks a stock client on B is
  unaffected.

### Not done

* Not ported to `main` (0.8.0-dev). The same choke points exist there
  (`integrate_ready_ops`, `SourceChain::flush`), but the patch has not been
  applied or tested on `main`.
* No TypeScript client support (`@holochain/client` passes unknown signal
  objects through rather than panicking, but has no subscribe call).
* Warrant ops are reported with their validity like any other op; their
  semantics for apps are not explored.
* Ordering: each batch is delivered from its own spawned task, so signals
  from two back-to-back batches can arrive out of order. Consumers must not
  rely on signal order (the relay does not; its journal orders by arrival).
* For self-authored ops the signal's `when_integrated` is the flush time,
  while `DumpOpTimings` reports the action timestamp for them. The two
  differ by the time between authoring and commit.

---

## U2: "gossip with this peer now"

**Observed.** In the three-node test C restarts and is handed B's signed
agent info. Kitsune2's initiate loop
(`kitsune2_gossip/src/initiate.rs`) chooses its own target
(`select_next_target`). No app or admin API can direct a round at B, even
though the app knows exactly which always-on peer holds what it is missing.
`K2Gossip::initiate_gossip(url)` and the internal `force_initiate` channel
exist but are not exposed.

**Proposal.** Kitsune2: `Gossip::request_round(url)` (or
`Space::gossip_with(url)`) that queues a round with `url` on the next
initiate iteration, still subject to the usual rate limits on the
responder. Holochain: `AppRequest::GossipWith { dna_hash, peer_url }`,
scoped to the app's DNAs. The relay already supplies the peer record
(`GET /v1/peers`), so the client could request an immediate round with B.

## U3: stale-peer selection

**Observed.** With bootstrap enabled, C's peer store contains the offline
A. Selection prefers the *least recently gossiped* responsive peer, so C
repeatedly picks A. Each attempt waits for the transport connect timeout
(roughly a minute observed) before failing. Measured catch-up: 47–84 s with
bootstrap, versus about 6 s when C knew only B from the relay hint
(`docs/results.md`).

**Proposal.** During initial sync after a restart, prefer peers that are
known reachable (recent successful contact, or freshly added via
`AddAgentInfo`). Mark a peer unresponsive after the first failed connect
rather than after repeated ones. Use a shorter dial timeout for gossip
initiation than for data fetches. Separately, holochain could publish an
agent tombstone on graceful shutdown (SIGTERM did not appear to here).

## U4: forward-compatible signal decoding in `holochain_client`

Replace the `expect` in `AppWebsocketInner::on_signal` with logging and
dropping undecodable signals. Even with U1's per-connection opt-in, a
client that subscribes must be built against matching crates, and any
future system signal would hit the same panic.

## U5: richer `OpTimingDump`

Add `basis_hash`, `op_type` and, for chain ops, `action_hash`. The summaries
already exist where `DumpOpTimings` is answered. This lets the polling path
produce hints as rich as U1's. With the basis location a client can also
drop hints outside its own storage arc, which matters for small-arc mobile
nodes. Today `op_hash` alone cannot be fetched with zome `get`.

## U6: `dual_bind` on IPv6-less kernels

`holochain_websocket::WebsocketListener::dual_bind` falls back to IPv4 only
on `ErrorKind::AddrNotAvailable`. Kernels built without IPv6 return
`EAFNOSUPPORT` (`ErrorKind::Unsupported` / raw os error 97), so the conductor
aborts at startup, even with `danger_bind_addr: 127.0.0.1`. Treat that
error like `AddrNotAvailable`. Unchanged on `main` as of `90198dd`. The test
scripts work around it with `scripts/support/no-ipv6-shim.c` (test-only).

## U7: monotonic `DumpOpTimings` cursor

The cursor orders by local `when_received`. A backwards clock step can
place new ops before a saved cursor. A local monotonic sequence (e.g. the
integration row id) would avoid that.
