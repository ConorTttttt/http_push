# Measured results

All numbers are from runs in this repository's development container
(4 vCPU, Linux 6.18, no IPv6, loopback networking). Absolute values will
differ on real HoloPorts and phones; the comparisons between runs are the
point. Each row is one run of `scripts/holochain-three-node-e2e.sh`; the raw
`results.json` and logs are written to `.demo/three-node-*`.

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

RESULTS_TABLES

## Reading the numbers

READING

## Regression and unit tests

`cargo test --workspace`: TESTCOUNT tests pass, including
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

UPSTREAMTESTS
