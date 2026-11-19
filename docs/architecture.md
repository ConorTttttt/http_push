# Architecture

## Goal and non-goals

Let lightweight mobile Holochain clients learn that **relevant DHT changes
exist** and **catch up after being offline**, using short HTTP exchanges
instead of continuous peer-to-peer connections. Independently operated
HoloPorts or EdgeNodes provide the service.

Non-goals: transferring DHT op *data* through the relay, replacing
Kitsune2 gossip, and a global relay registry. Op data reaches a device only
through its own conductor (Kitsune2 gossip with the HoloPort, validated by
the device itself); the relay tells it *what* is missing and *whom* to ask.

## Components

```
            Holochain peers (Kitsune2 publish / gossip)
                         │
 ┌───────────────────────▼──────────────────────────────── HoloPort / EdgeNode
 │  Holochain conductor ── validation ── integration ── DHT store            │
 │      │ admin ws (loopback only, used briefly to issue an app token)        │
 │      │ app ws (loopback): DumpOpTimings · on_signal · AgentInfo           │
 │  ┌───▼──────────────────────────── hc-push-relay ─────────────────────┐   │
 │  │ ChangeSource(s)  ──▶ ingest ──▶ SQLite journal ──▶ broadcast        │   │
 │  │  · OpTimingsSource     polling, durable  │            │              │   │
 │  │  · OpsIntegratedSource event (patched)   │            │              │   │
 │  │  · AppSignalSource     app signals       ▼            ▼              │   │
 │  │  · MockSource (dev)    GET /v1/sync (durable)   GET /v1/events (SSE) │   │
 │  │  ConductorPeers ─────▶ GET /v1/peers   POST /v1/subscriptions       │   │
 │  └────────────────────────────────┬────────────────────────────────────┘   │
 └───────────────────────────────────┼─────────────────────────────────────────┘
                     Caddy / TLS ────┘
                         │ HTTPS
          ┌──────────────┴───────────────┐
          ▼                              ▼
   relay A (HoloPort)             relay B (EdgeNode)       … any number
          ▲                              ▲
          └──── hc-push-client (phone): per-relay cursor, cross-relay dedup,
                failover, gap notices → app
                       │ hints = missing op hashes; peers = HoloPort's signed record
                       ▼
                phone's own conductor ── AddAgentInfo(peers) ── Kitsune2 gossip
                with the HoloPort ── fetch ── own validation ── integrated
```

| Crate | Role |
|---|---|
| `crates/protocol` | v1 wire types, cursor, change key. No async runtime; mobile-portable. |
| `crates/source-api` | `ChangeSource` trait + `ChangeSink`: the **gossip integration adapter boundary**. |
| `crates/holochain-adapter` | Real conductor sources (Holochain 0.7.x app API). |
| `crates/relay` | HTTP relay library + `hc-push-relay` binary, including the clearly labelled `mock` source. |
| `crates/client` | Portable client library + `hc-push-client` CLI. |

## Relay internals

* **Ingest.** Sources submit `ChangeBatch { changes, checkpoint }` and wait
  for an acknowledgement. The ingest loop validates every change (space
  served, ref ids well-formed, ≤ 64 refs, batch ≤ 5000). It then appends the
  batch **and the source checkpoint in one SQLite transaction** and
  broadcasts the new envelopes. A crash between the two cannot lose
  changes; at worst a source re-submits, and the journal dedups by
  `change_key`.
* **Journal** (`journal.rs`). WAL-mode SQLite with:
  * `events(seq AUTOINCREMENT, change_key UNIQUE, …)`
  * `source_checkpoints`
  * `subscriptions(token_hash UNIQUE, …)`
  * `meta(journal_id, relay_id, pruned_through)`

  Retention is by count *and* age, and always prunes a **prefix**
  (`pruned_through`). A cursor is therefore either fully servable or
  explicitly expired (`410`), never silently incomplete.
* **SSE** subscribes to the broadcast *before* reading the backlog and
  filters by sequence, so nothing that commits in between is lost. Backlogs
  larger than the replay limit, and lagging consumers, get `sync_required`
  instead of an unbounded replay.
* **Maintenance.** Every 60 s: apply retention, expire idle subscriptions.
* **Metrics** (`metrics.rs`, loopback admin listener only:
  `/admin/v1/metrics` Prometheus, `/admin/v1/stats` JSON):
  * per-source detection latency (`observed_at - occurred_at`);
  * SSE dispatch latency;
  * events journaled and duplicates dropped per source;
  * HTTP requests per route and status;
  * response body bytes per route, including streamed SSE;
  * estimated header bytes, request body bytes;
  * journal and subscription gauges.

  The client reports its own side: duration, HTTP bytes, and relay→app and
  origin→app latency per `sync`/`listen`.
* **Shutdown.** On SIGTERM/SIGINT the relay stops accepting, sends
  `sync_required:shutdown` to open streams, stops sources, drains pending
  batches (10 s bound) and exits. State is already durable per batch.

### Holochain sources

* `OpTimingsSource` polls `DumpOpTimings` for each DNA of the configured
  app (default every 2 s, ≤ 10 pages of 500 per poll). It reports ops whose
  `when_integrated` is set: `Accepted` → `op_integrated`, `Rejected` →
  `op_rejected`. Abandoned ops are skipped. The next scan starts just before
  the **oldest op still pending validation** (low-water mark), so ops that
  finish validation later are still reported. Re-reported ops are dropped
  by the journal. If the ops behind a pending op exceed one poll's budget
  (10 pages) or 20 000 ops, the cursor advances past the pending op with a
  warning, so scanning never stalls; that op is then not re-checked.
* `OpsIntegratedSource` (feature `ops-integrated-signal`, conductor with
  the upstream patch U1 in `upstream-changes.md`) sends
  `SubscribeOpsIntegrated` on its own app connection and journals each
  `SystemSignal::OpsIntegrated` immediately, without a checkpoint. It is
  the latency path; the poller keeps running (e.g. every 15–30 s) and
  reconciles anything the signal path missed (relay down, connection lag,
  ops integrated before the subscription). Both paths produce the same
  `change_key`, so the journal keeps whichever arrives first. `discover()`
  probes support and falls back to polling only.
* `ConductorPeers` answers `GET /v1/peers` with the conductor's own signed
  agent infos (`AppRequest::AgentInfo`, filtered to the app's agent). It is
  a peer *hint*, not a directory: the relay never hands out other agents'
  records.
* `AppSignalSource` subscribes with `on_signal` and forwards a
  `sha256:` digest of each `Signal::App` / `Signal::AppDirect` payload
  (local events only, see `holochain-integration.md` §2). It checks
  liveness every 10 s and reconnects with backoff.
* Both reconnect with jittered exponential backoff. `/v1/health` shows
  `degraded` while the conductor is unreachable.

## Decentralization

* Each relay is a standalone service with its own journal, keys and
  subscribers. There is no coordinator, registry or privileged broker.
* The client keeps **independent state per relay** (subscription, token,
  `journal_id`, cursor) and never presents one relay's cursor to another;
  the protocol enforces this (`410 cursor_unknown_journal`).
* Different relays have different coverage: each reports what *its*
  conductor holds (its storage arc) and may serve different DNAs or
  retention windows. `change_key` is content-derived, so overlapping
  coverage is deduplicated and non-overlapping coverage adds up.
  `SyncPolicy::All` maximises coverage; `FirstAvailable` minimises work.
* MVP discovery is static configuration. Possible future mechanisms (none
  implemented):
  * relays publishing a signed capabilities document in a DNA (an entry
    type or anchor);
  * the Kitsune2 bootstrap service's agent-info `url` field;
  * DNS-SD / `.well-known` on HoloPort domains;
  * relays announced through hApp properties.

## Security model

| Requirement | How |
|---|---|
| TLS | Terminated by Caddy (`deploy/Caddyfile`, automatic ACME or internal CA). The relay listens on loopback / internal networks only. |
| Subscription auth | Registration key (operator-issued, ≥ 16 chars, SHA-256 hashed, constant-time compare) or explicit `open` mode. Per-subscription 256-bit bearer token, stored only as SHA-256. Tokens are unrelated to Holochain keys. |
| Agent keys | Never sent to or stored on the relay. The relay's Holochain client uses an empty signer and makes no zome calls. |
| Conductor access | Admin websocket on loopback only, used briefly to issue a single-use, 30 s app token, then dropped. The app interface is origin-scoped (`hc-push-relay`), app-scoped and loopback-bound. Neither websocket is ever published (`deploy/compose.yaml`). |
| Bounded resources | Request body 16 KiB; page ≤ 500; ≤ 16 spaces per subscription; max subscriptions (10 000); global and per-subscription SSE caps; global request concurrency (256) with a 10 s timeout; bounded broadcast buffer; bounded rate-limiter key set; batch and ref limits on ingest; journal retention. |
| Rate limits | Token bucket per subscription (120/min, burst 30) and per client address for registration (10/min, also counts failed keys). Behind a proxy, `HC_PUSH_TRUST_FORWARDED_FOR=true`. |
| Replay / duplicates | Journal dedup by `change_key`; strictly ascending sequences checked by the client; client dedup set across relays; cursors are journal-bound. |
| Input validation | `deny_unknown_fields` on bodies and queries, strict cursor grammar, space/ref/label charsets and lengths. |
| Privacy | Envelopes carry hashes, digests and coarse diagnostics only. Signal payloads are digested. `/v1/health` reveals no errors. Advertising spaces can be disabled. Note: op hashes and timing still reveal *activity* in a DNA to subscribers, so private DNAs should use key mode and give keys only to members (see "Future"). |
| Validation | The relay never supplies data to accept. Clients act on hints by reading through their own conductor (see `holochain-integration.md` §6). |
| Container | Distroless, non-root (uid 65532), read-only root filesystem, all capabilities dropped, `no-new-privileges`. |

## Mobile constraints

* Nothing assumes background execution. `sync()` is a bounded one-shot that
  returns to idle: no background tasks, idle sockets closed after 2 s.
  `listen()` ends after an idle timeout.
* Expected app integration:
  * on launch / foreground: `sync()`, then `listen()` while visible;
  * on an OS-scheduled background slot (Android WorkManager, iOS
    `BGAppRefreshTask`): `sync()`;
  * on a wake-up push (future adapter): `sync()`.
* The client uses rustls (no OpenSSL) and a current-thread runtime, and
  persists state through a pluggable `StateStore` (atomic JSON file by
  default; apps can use their keystore for tokens).

## Web Push and native wake-ups (future, optional)

Web Push (RFC 8030, with RFC 8291 message encryption and RFC 8292 VAPID)
lets a relay POST a small encrypted message to a push service, which wakes
a *browser* service worker. It does **not** bypass mobile background
limits:

* Native Android and iOS apps receive pushes only through FCM / APNs, under
  their quotas and priorities.
* On iOS, Web Push reaches only home-screen web apps.
* Every push path still depends on a platform push service.

The plan is a separate, optional notification adapter: subscriptions gain
an optional `push` endpoint; the relay sends content-free "sync now" nudges
(TTL short, `Urgency: normal`, `Topic` per subscription so nudges
collapse). Correctness never depends on delivery, because `/v1/sync`
remains the source of truth.

## Migration path to genuine Kitsune2 synchronization

1. **Done:** observe the conductor's integrated ops through the app API
   (polling, durable), and let a device's conductor catch up by **native
   Kitsune2 gossip with the HoloPort**: the relay supplies the missing op
   hashes (completion criterion) and the HoloPort's signed peer record.
   Verified with three conductors (`docs/results.md`).
2. **Small additive Holochain changes** (`upstream-changes.md`):
   * U1 `OpsIntegrated` signal: implemented as a patch against 0.7.1 and
     wired into the relay (`OpsIntegratedSource`); polling stays as the
     reconciliation path;
   * U2/U3 "gossip with this peer now" and better stale-peer handling: the
     dominant cost of catch-up today;
   * U5 basis and type in `OpTimingDump` → arc-filterable hints.

   No relay protocol change is needed for any of these.
3. **Only if networking stays too costly on devices.** Step 1 already uses
   Kitsune2's own gossip and the device's own validation; it pays for a
   short-lived Kitsune2 session (iroh transport) per catch-up. If that turns
   out too expensive, a Kitsune2 transport module over HTTP
   (request/response plus SSE for inbound frames) could carry the same
   bounded fetch/gossip rounds with the HoloPort as an always-on K2 peer.
   Ops would still enter the device through `OpStore::process_incoming_ops`
   and Holochain's normal validation. The relay journal would become a
   K2-aware index: per-arc cursors mapped onto `retrieve_op_ids_bounded`
   and slice hashes for region reconciliation. Not started; measure step 1
   on real devices first.
4. **Discovery and incentives**, as listed under Decentralization and in the
   README's future-work section.
