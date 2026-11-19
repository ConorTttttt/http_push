# Proposed upstream patches

Patch series for Holochain and Kitsune2, one directory per repository and
base. Nothing here is part of a Holochain or Kitsune2 release. The design,
tests and status are documented in
[`docs/upstream-changes.md`](../docs/upstream-changes.md).

| Directory | Base | Patches |
|---|---|---|
| `kitsune2-0.5.1/` | `holochain/kitsune2` tag `v0.5.1` (`2f178b0`) | 0001 `Gossip::request_round` |
| `holochain-0.7.1/` | `holochain/holochain` tag `holochain-0.7.1` (`fdaa678`) | 0001 `SystemSignal::OpsIntegrated`; 0002 `AppRequest::RequestGossipRound` (needs the Kitsune2 patch) |
| `kitsune2-main/` | `holochain/kitsune2` `main` (`1d26e00`, 0.6.0-dev.3) | 0001 as above |
| `holochain-main/` | `holochain/holochain` `main` (`90198dd`, 0.8.0-dev.15) | 0001, 0002 as above (`RequestGossipRound.url` is a `String` on main, like `PeerMetaInfo`) |

Each series applies with `git am` on its base. All four were built and
their new and neighbouring unit tests run; see `docs/results.md`
("Upstream test runs"). The Holochain 0001 patch
does not depend on anything else and can be reviewed and taken alone.

To build Holochain 0002 against the patched Kitsune2 without publishing it,
point Cargo at the Kitsune2 checkout, e.g. with a `--config` file containing
a `[patch.crates-io]` table for the `kitsune2*` crates.
`scripts/build-patched-holochain.sh` does this for the 0.7.1 series.
