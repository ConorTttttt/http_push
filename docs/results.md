# Measured results

All numbers are from runs in this repository's development container
(4 vCPU, Linux 6.18, no IPv6, loopback networking). Absolute values will
differ on real HoloPorts and phones; the comparisons between runs are the
point. Each row is one run of `scripts/holochain-three-node-e2e.sh`. The raw
`results.json` of each run is in [`results-data/`](results-data/); logs are
written to `.demo/three-node-*` (not committed).

Setup common to all runs:

* Three conductors on one host, local `kitsune2-bootstrap-srv` 0.5.3
  (bootstrap + iroh relay), full storage arc, `GOSSIP=fast`
  (`initiateIntervalMs 2000`, `minInitiateIntervalMs 1000`,
  `initiateJitterMs 500`).
* A and C: stock `holochain 0.7.1` release binary. B: stock 0.7.1 in polling
  mode; in event mode, holochain 0.7.1 **plus the U1 patch**, built locally
  with Holochain's dev profile (dependencies optimized, Holochain crates
  not), so B is somewhat slower than a release build.
* Relay on B: polling every 1000 ms (polling mode) or every 30 000 ms with
  the `OpsIntegrated` source (event mode, so latency comes from the event
  path and polling only reconciles).
* While C is offline, A authors 5 notes and B authors 1. An "observer"
  client stays online on SSE throughout.

Definitions:

* **Detection**: `observed_at_ms - occurred_at_ms` of each hint envelope,
  i.e. from B's integration of the op until the relay journaled it.
* **Notification**: from the relay journal to the online observer client's
  handler (`relay_to_app`); **end-to-end** adds detection (`origin_to_app`).
  All clocks are the same host's.
* **C hint sync**: C's device client catching up over HTTP while C's
  conductor is still stopped.
* **First op received / all integrated**: from C's own `DumpOpTimings`,
  relative to the moment C's conductor process was started.
* **Bytes**: HTTP bytes as seen by the client (request line, headers, body;
  header sizes estimated; TLS/TCP not included).

## Notification latency (relay on B)

| Run | Detection (B integrated → relay journal), ms p50 / max | Notification (journal → online client, SSE), ms p50 / max | End-to-end (B integrated → online client), ms p50 / max | Hint sources |
|---|---|---|---|---|
| polling, C bootstrap on, peer hint on | 199 / 247 | 11 / 19 | 210 / 266 | holochain-app-signals: 1, holochain-op-timings: 18 |
| polling, C bootstrap off, peer hint on | 237 / 365 | 9 / 16 | 246 / 381 | holochain-app-signals: 1, holochain-op-timings: 18 |
| event, C bootstrap off, peer hint on | 7 / 7 | 4 / 8 | 11 / 15 | holochain-ops-integrated: 22, holochain-app-signals: 1 |
| event, C bootstrap on, peer hint on | 4 / 6 | 1 / 7 | 5 / 13 | holochain-ops-integrated: 18, holochain-app-signals: 1 |

"Hint sources" counts the envelopes C received, by the relay source that
journaled them first. In event mode **every op hint came from the
`OpsIntegrated` path**, including B's self-authored ops. The poller
(30 s) contributed none before C woke up. On the same patched B, a second,
stock-crates relay (`holochain_client` 0.9.1, `on_signal` registered)
journaled the normal app signal and all 52 ops by polling, and never received
an `OpsIntegrated` signal (no decode panic in its log).

## Catch-up of the offline conductor C

| Run | Ops authored on A / new on B / hinted to C / validated by C | C hint sync over HTTP (ms, requests, bytes in) | Peer-hint request (bytes in) | C conductor ready, ms | First op received / all integrated, ms after start | Gossip attempts after restart |
|---|---|---|---|---|---|---|
| polling, C bootstrap on, peer hint on | 15 / 18 / 18 / 18 | 5, 1, 7706 | 730 | 2352 | 47243 / 47306 | FAIL other; FAIL other; ok B |
| polling, C bootstrap off, peer hint on | 15 / 18 / 18 / 18 | 5, 1, 7706 | 730 | 2343 | 5166 / 6182 | ok B; ok B |
| polling, C bootstrap off, peer hint off | – / – / 18 / 0 | – | – | – | not caught up within 150 s | – |
| event, C bootstrap off, peer hint on | 17 / 20 / 22 / 22 | 5, 1, 9362 | 730 | 2311 | 5187 / 6202 | ok B; ok B |
| event, C bootstrap on, peer hint on | 15 / 18 / 18 / 18 | 5, 1, 7778 | 730 | 2370 | 68331 / 69341 | FAIL other; FAIL other; ok B |

Gossip attempts are C's own initiations after restart, from Kitsune2's
logs: `FAIL other` is the offline A (connect timeout), `ok B` a round with
B. In every run that caught up, all hinted ops were held, integrated,
`Accepted` and `locally_validated: true` in C's own `DumpOpTimings`. A was
offline, so the data came from B.

## HTTP bandwidth

| Exchange | Requests | Bytes in (client) | Bytes out (client) |
|---|---|---|---|
| C wakes up: `GET /v1/sync` returning 19 hints: 18 op hints + 1 app-signal digest (≈ 406 B per hint, JSON envelopes) | 1 | 7 706 (polling) / 7 778 (event) | 189 |
| C fetches B's peer record: `GET /v1/peers` | 1 | 730 | 210 |
| Online observer: one SSE stream for the whole authoring window, 19 events, plus keep-alives | 1 | 8 544 (polling) / 8 616 (event) | 217 |
| No-op sync (nothing new) | 1 | < 1 000 (`client_flows::sync_report_measures_traffic_and_latency`) | ≈ 190 |

The relay's own counters (`/admin/v1/metrics`,
`hcpush_http_response_body_bytes_total`) agree: e.g. polling run
`/v1/sync` 7 787 B and `/v1/events` 8 379 B of response bodies. These
include the setup syncs, so they are slightly higher than the client's
numbers for the measured exchanges.

The data itself (the 18–22 ops) moved between conductors over Kitsune2
(iroh), not over HTTP, and is not included above.

## Reading the numbers

* **Event-driven detection is roughly 30–50× faster than 1 s polling.** p50 4–7 ms
  versus ~200–240 ms from B's integration to the relay journal; end-to-end
  to an online client p50 5–11 ms versus ~210–250 ms. Polling latency is
  bounded by the poll interval (≈ interval/2 on average), and a shorter
  interval costs a `DumpOpTimings` query per DNA per interval. B ran a
  dev-profile build in event mode, so the event numbers are, if anything,
  pessimistic.
* **SSE fan-out is cheap.** Journal to client handler is ~1–11 ms p50.
* **HTTP is not the bottleneck for catch-up; peer selection is.** C's
  HTTP sync took 5 ms and ~7.7 KB for 18 hints. After restart, C's
  conductor was ready in ~2.3 s.
  * With only the relay's peer hint (bootstrap off), the first missing op
    arrived ~5.2 s after start, and all were validated and integrated by
    ~6.2 s.
  * With bootstrap on, the peer store also held the offline A. Kitsune2
    chose A first (least recently gossiped) and waited out two connect
    timeouts, so catch-up took 47–69 s in these runs (up to 84 s in earlier
    exploratory runs). Once a round with B started, the
    ops arrived in ~25 ms and integrated within ~1 s.

  This is gap U2/U3 in `upstream-changes.md`.
* **The relay's peer hint is what made the no-bootstrap catch-up possible.**
  The control run, identical but without handing B's record to C, caught up
  on 0 of 18 ops in 150 s.
* **Missed notifications only delay delivery.** In the first event-mode attempt
  (patch v1, which only hooked the integration workflow), B's three
  self-authored ops produced no signal. That run failed the hint check
  because the 30 s poller had not reconciled them yet. With polling enabled
  they would have arrived on the next poll, and the regression test covers
  this explicitly. The final patch hooks the store instead.
* **Operation counts.** Six notes produced 15–17 new integrated ops on A
  and 18–20 on B. A's count is lower, most likely because some of B's note
  ops had not yet reached A when A's dump was taken. C received and validated all 18–22 hinted ops.

## Regression and unit tests

`cargo test --workspace`: 63 tests pass, including
`crates/relay/tests/holochain_reconcile.rs`. That test drives the real
`DumpOpTimings` poller against a scripted conductor and an event path that
loses notices while the relay is down. It checks four things:

1. Ops integrated while the relay is up arrive through the event path, with
   `occurred_at_ms`.
2. Ops integrated while the relay is stopped (their notices lost) arrive
   after restart from the persisted poller checkpoint. Earlier ops are not
   re-delivered.
3. An op still pending validation during a scan is reported once it
   integrates, although its notice was dropped.
4. A conductor outage is bridged by reconnecting.

Every op is delivered exactly once with gap-free sequence numbers.

Upstream (`holochain-0.7.1` + `upstream/holochain-0.7.1-ops-integrated.patch`),
run in the patched Holochain tree:

```
cargo test -p holochain_state --lib -- dht_store::tests::integrate_ready_ops
  4 passed  (incl. integrate_ready_ops_notifies_observer_through_clones)
cargo test -p holochain --lib -- conductor::conductor::app_broadcast core::workflow::integrate_dht_ops_workflow
  9 passed  (incl. ops_integrated_subscriptions_are_counted_until_dropped;
             the existing integration-workflow tests are unchanged and pass)
```

The patch applies cleanly to a pristine `holochain-0.7.1` checkout
(`git apply --check`). The full Holochain test suite was **not** run.
