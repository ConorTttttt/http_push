# Proposed Holochain / Kitsune2 upstream changes

This document is about **Holochain and Kitsune2**, not about the relay. The
relay works without any of these changes: it polls the supported
`DumpOpTimings` app API, and that durable path stays in place even where a
change below is adopted.

Nothing here has been submitted upstream. The implemented changes are kept
as separate patch series, one per repository and base version, each commit
reviewable on its own:

```
upstream/
  kitsune2-0.5.1/
    0001-feat-gossip-request-a-gossip-round-with-a-verified-p.patch   (U2, Kitsune2 half)
  holochain-0.7.1/
    0001-feat-opt-in-SystemSignal-OpsIntegrated.patch                 (U1)
    0002-feat-AppRequest-RequestGossipRound.patch                     (U2, Holochain half; needs the Kitsune2 patch)
  kitsune2-main/      the same change ported to kitsune2 main (1d26e00, 0.6.0-dev.3)
  holochain-main/     the same series ported to holochain main (90198dd, 0.8.0-dev.15)
```

Apply with `git am` on the named base (`v0.5.1` / `holochain-0.7.1`), or run
`scripts/build-patched-holochain.sh`, which applies the 0.7.1 series and
builds the patched conductor (against the patched Kitsune2) plus the relay,
client and device helper against the patched crates. U1 does not depend on
U2 and can be taken alone (`holochain-0.7.1/0001`).

| # | Change | Component | What it unlocks | State |
|---|---|---|---|---|
| U1 | Opt-in `SystemSignal::OpsIntegrated`, delivered in order | holochain | Event-driven relay notifications instead of polling | **Patch + tests** on 0.7.1; ported to main (build and tests pending) |
| U2 | Request the next gossip round with a given, verified peer | kitsune2 + holochain app API | Fast catch-up from a known always-on peer despite stale peers | **Patches + tests** on 0.7.1; ported to main (build and tests pending) |
| U3 | Stale-peer handling and peer health | kitsune2 / holochain | Catch-up and `get` not delayed by unreachable peers | Measured; proposals |
| U4 | `holochain_client`: don't panic on unknown signal variants | holochain_client (Rust) | Forward-compatible signals | Proposal |
| U5 | `OpTimingDump`: basis hash / op type / action hash | holochain conductor API | Actionable, arc-filterable hints from the polling path | Proposal |
| U6 | `dual_bind`: fall back to IPv4 on `EAFNOSUPPORT` | holochain_websocket | Conductor starts on kernels without IPv6 | Proposal (one-line) |
| U7 | Monotonic `DumpOpTimings` cursor | holochain conductor API | Cursor robust to host clock steps | Proposal |
| U8 | Partial storage arcs and the target arc in the app API | holochain / kitsune2 | Arc-aware catch-up for small-arc mobile nodes | Measured; proposal |

---

## U1: opt-in `SystemSignal::OpsIntegrated` (patch)

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
* The observer hands the batch to a single delivery task through a bounded
  queue (`try_send`, 1024 batches). If the queue is full the batch is
  dropped and counted, and a warning is logged; integration never waits.
  The delivery task resolves which enabled apps have a cell of the DNA and
  uses the existing per-app `broadcast::Sender`, splitting batches into
  signals of at most 500 ops. `send` never blocks; lagging connections drop
  signals and are counted by the existing `dropped_signal_metric`.
* **Ordering.** One delivery task per conductor, fed in commit order, so
  signals arrive in the order the batches were committed, also when a
  batch is split. (The first version spawned one task per batch, so two
  back-to-back batches could be reordered.)
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
crates/holochain/src/conductor/conductor/app_broadcast.rs  opt-in subscriber count, ordered delivery queue, tests
crates/holochain/src/conductor/interface/websocket.rs      per-connection opt-in and filter
crates/holochain/src/conductor/api/api_external/app_interface.rs  no-op arm for non-websocket callers
crates/holochain/src/core/queue_consumer.rs                install the observer per space
```

### How it was verified

* Upstream unit tests on the patched tree (`docs/results.md` has the exact
  runs):
  * `holochain_state`: `integrate_ready_ops_notifies_observer_through_clones`
    (clones share the observer, inactive observers cost nothing, payload).
  * `holochain` `app_broadcast`: subscriber counting; **ordering**
    (`ops_integrated_batches_are_delivered_in_order`, with a resolver that
    is slow for the first batch); **high volume**
    (`large_ops_integrated_batches_are_split_in_order`, 1001 ops ->
    500/500/1 in order); **non-blocking overflow**
    (`full_ops_integrated_queue_drops_without_blocking`, queue of 1 with a
    stuck resolver).
  * The existing integration-workflow and app-broadcast tests are
    unchanged and pass.
* **Missed events, reconnection, restart**: by design signals are best
  effort and are not replayed. The relay pairs them with the durable
  poller; `crates/relay/tests/holochain_reconcile.rs` drops notices while
  the relay is down, restarts it, and checks every op is delivered exactly
  once in a gap-free sequence. The relay's signal source reconnects with
  backoff and re-subscribes on every new connection.
* End to end: `MODE=event scripts/holochain-three-node-e2e.sh` (patched B,
  stock A and C, plus a stock-crates relay on B that must never see the
  signal) and `scripts/mobile-lifecycle-e2e.sh` (two patched relay
  conductors; 104-op bursts; detection p50 around 10 ms).

### Not done

* No TypeScript client support (`@holochain/client` passes unknown signal
  objects through rather than panicking, but has no subscribe call).
* Warrant ops are reported with their validity like any other op; their
  semantics for apps are not explored.
* For self-authored ops the signal's `when_integrated` is the flush time,
  while `DumpOpTimings` reports the action timestamp for them. The two
  differ by the time between authoring and commit.
* Not run: the full Holochain test suite, on either base.

---

## U2: request the next gossip round with a given peer (patches)

**Problem.** After a restart, Kitsune2's initiate loop
(`kitsune2_gossip/src/initiate.rs`) chooses its own target with
`select_next_target`: the least recently gossiped peer that is not marked
unresponsive. With bootstrap enabled, the peer store of a phone that was
asleep contains peers that have since gone offline. Their records stay
valid until they expire. Selection then tends to pick the stale peer first,
since the phone has never gossiped with it. Each attempt waits for the iroh
connect timeout. Measured in this repository: 47-84 s until the first round
with the always-on peer, against about 6 s when the stale peer is absent
(`docs/results.md`). The application, meanwhile, knows exactly which
always-on peer holds what it is missing: the relay told it, with that
peer's signed record.

### Kitsune2: `Gossip::request_round`

```rust
// kitsune2_api::Gossip (default method: returns an error, so other gossip
// implementations keep compiling)
fn request_round(&self, peer: Url) -> BoxFut<'_, K2Result<()>>;
```

`K2Gossip` implements it:

* **Only verified peers.** The URL must belong to a live record in the peer
  store: not a tombstone, not expired. Records enter the peer store only
  after their signature is verified (`AgentInfoSigned::decode`), so this
  can never direct gossip at an unverified address. URLs of the node's own
  agents are refused (`request_round_rejects_unknown_peer`).
* **Queued, not forced.** The peer goes into a small FIFO queue
  (deduplicated) and the initiate loop is woken through its existing
  `force_initiate` channel. The next iteration initiates with the queued
  peer before ordinary selection. The existing checks still apply: one
  initiated round at a time, `initiate_gossip`'s own per-peer checks, and
  the responder's accept limits (`maxConcurrentAcceptedRounds`, and the
  per-peer accept burst limit `initiateBurstFactor` ×
  `initiateBurstWindowCount`). Nothing bypasses the responder.
* **Bounded retries, then ordinary scheduling.** A failed initiation (e.g.
  the "home relay URL not known yet" race in the first seconds after
  start) is retried after `initialInitiateIntervalMs`, at most 10 times.
  Then the request is dropped and ordinary selection carries on unchanged.
  Ordinary scheduling, intervals and peer selection are otherwise
  untouched.
* Two waits in the initiate loop became interruptible by a forced
  initiation: the growing-arc wait and the post-fetch-drain sleep.
  Previously a forced initiation arriving during those sleeps waited for
  them, up to `initialInitiateIntervalMs`.

Tests (`kitsune2_gossip`): `requested_rounds_tests::*` (FIFO, dedup,
bounded retries, re-request resets attempts),
`request_round_rejects_unknown_peer` (unknown URL, own URL), and
`requested_round_syncs_without_waiting_for_the_schedule`. In that test the
initiate intervals are 60 s; a requested round delivers the op within 10 s,
and a control peer that did not request receives nothing. The full
`kitsune2_gossip` (71), `kitsune2_api` (34) and `kitsune2_core` (108) unit
suites pass on 0.5.1.

### Holochain: `AppRequest::RequestGossipRound`

```rust
AppRequest::RequestGossipRound { dna_hash: DnaHash, url: Url }   // 0.7.1
AppRequest::RequestGossipRound { dna_hash: DnaHash, url: String } // main: like PeerMetaInfo
// -> AppResponse::Ok
AppWebsocket::request_gossip_round(dna_hash, url)
```

* App interface, scoped: `dna_hash` must be a DNA of the calling app.
  Only peers already in the conductor's peer store can be requested. On a
  phone, the app adds the relay's records first (`AddAgentInfo`, which
  verifies them; on `main` that request is also available on the app
  interface).
* `HcP2p::request_gossip_round` -> the space's `gossip().request_round()`.
* Test (`conductor::conductor::tests::request_gossip_round`): two
  conductors with publishing disabled and 60 s initiate intervals. After 3
  s Bob has received nothing on his own. Requests for a foreign DNA, an
  unknown URL and Bob's own URL fail. After the request Bob holds and has
  integrated all of Alice's ops: 0.2-1.1 s after the request, in 6 of 6
  runs.

### Effect (lifecycle benchmark)

See `docs/results.md`: with bootstrap enabled, a stale peer A and two
independent always-on peers, the phone validated all 104+ required ops
about 6 s after its conductor started with the request, against tens of
seconds without it. The improvement comes entirely from choosing the peer
the relay named. No gossip goes through the relay.

### Not done

* Not exposed in the admin API or `hc` CLI (the app API is enough for an app
  embedding the conductor).
* A requested peer that turns out to be slow is not abandoned early; the
  round timeout applies as for any round.

---

## U3: stale-peer handling and peer health

What was measured (`docs/results.md`, `docs/holochain-integration.md`):

* **The connect timeout is derived, not configurable.** Holochain sets
  iroh's `connectTimeoutS` to `floor(3/8 * network.request_timeout_s)` (22 s
  for the default 60) and overwrites any value given under
  `advanced.irohTransport` (with a warning). Lowering `request_timeout_s`
  to 8 on the phone (3 s connect timeout) cut catch-up with a stale peer
  from tens of seconds to about 14 s in the three-node test, but it also
  bounds every other network request of that conductor.
* **A failed connect marks the peer unresponsive**, until its record
  expires. The start-up race ("connection attempted before home relay URL
  is known") does not mark the peer unresponsive, which is correct, but
  ordinary selection then moves on to another peer, often the stale one.
* **Zero-arc `get` hits the same problem.** A zero-arc phone does no data
  gossip, but its first `get` after waking can be routed to the stale peer:
  the call failed after about 44 s ("iroh connect timed out"), and the
  following gets succeeded in about 0.2 s, once the peer was marked
  unresponsive. `RequestGossipRound` does not help here. A shorter connect
  timeout does.

Proposals (not implemented):

1. Let applications set the iroh connect timeout independently of
   `request_timeout_s` (stop overwriting `connectTimeoutS`, or add a
   separate network option). Mobile nodes want a short dial timeout without
   shortening long fetches.
2. In `select_next_target`, prefer peers with recent successful contact,
   or freshly added through `AddAgentInfo`, before never-contacted ones,
   during the first rounds after start.
3. Health-aware `get` targeting: race the first two candidates, or skip
   peers with no successful contact since start while others are known.
4. Publish an agent tombstone on graceful shutdown, so peers that stop
   cleanly do not linger as stale records. SIGTERM did not appear to do
   this in 0.7.1.
5. Start the first initiation after a restart when the iroh home relay is
   registered, instead of failing with "Connection attempted before home
   relay URL is known" and retrying on a timer. In every lifecycle run with
   a requested round, the first two attempts failed this way, and the
   first round started about 3 s after the conductor was ready. That is now
   the floor of catch-up time on this host.

---

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


## U8: partial storage arcs and the target arc in the app API

The relay can filter hints by storage arc (`docs/protocol.md`), and the
device helper computes "required" ops from the arc. End to end, Holochain
0.7.1 only supports two arcs:

* `network.target_arc_factor` is applied as
  `apply_arc_factor(DhtArc::FULL, factor)`: 0 gives an empty arc, 1 gives
  FULL, and larger factors are clamped (with an error log). There is no way
  to configure a partial arc.
* Kitsune2 0.5's `update_storage_arcs` without the `sharding` feature only
  grows the current arc towards FULL.
* After a restart the *current* arc in the agent's record starts empty and
  grows to the target once initial sync drains. A phone reading its own
  record at wake-up therefore sees an empty arc, not the arc it will hold.

So zero and full arcs were tested end to end (`scripts/mobile-lifecycle-e2e.sh`,
`C_ARC=zero|full`). Partial and wrapping arcs are tested in Rust only:
`crates/relay/src/journal.rs`, `crates/relay/tests/arc_filter.rs` and
`crates/device/src/lib.rs`, using the same inclusive, wrapping semantics as
`kitsune2_api::DhtArc`.

Proposal: once sharding is enabled, expose the *target* arc next to the
current one (e.g. in the app interface's `AgentInfo` response or a small
`StorageArc` request), so a device can tell a relay its arc before its
conductor has finished growing it.
